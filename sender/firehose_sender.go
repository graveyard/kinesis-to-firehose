package sender

import (
	"encoding/json"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	iface "github.com/aws/aws-sdk-go/service/firehose/firehoseiface"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
	"github.com/Clever/amazon-kinesis-client-go/decode"
	"github.com/Clever/kinesis-to-firehose/sender/stats"
	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var log = logger.New("kinesis-to-firehose")

// FirehoseSender is a KCL consumer that writes records to an AWS firehose
type FirehoseSender struct {
	streamName      string
	deployEnv       string
	isElasticsearch bool
	client          iface.FirehoseAPI
}

// FirehoseSenderConfig is the set of config options used in NewFirehoseWriter
type FirehoseSenderConfig struct {
	// DeployEnv is the name of the runtime environment ("development" or "production")
	// It is used in the decoder to inject an environment into logs.
	DeployEnv string
	// FirehoseRegion the region in which the firehose exists
	FirehoseRegion string
	// StreamName is the firehose stream name
	StreamName string
	// IsElasticsearch true if this consumer sends logs to elasticsearch
	IsElasticsearch bool
}

// NewFirehoseSender creates a FirehoseSender
func NewFirehoseSender(config FirehoseSenderConfig) *FirehoseSender {
	f := &FirehoseSender{
		streamName:      config.StreamName,
		deployEnv:       config.DeployEnv,
		isElasticsearch: config.IsElasticsearch,
	}

	awsConfig := aws.NewConfig().WithRegion(config.FirehoseRegion).WithMaxRetries(10)
	sess := session.Must(session.NewSession(awsConfig))
	f.client = firehose.New(sess)

	return f
}

func (f *FirehoseSender) makeKeyESSafe(key string) string {
	// ES doesn't like fields that start with underscores.
	if len(key) > 0 && []rune(key)[0] == '_' {
		key = "kv_" + key
	}

	// ES doesn't handle keys with periods (.)
	if strings.Contains(key, ".") {
		key = strings.Replace(key, ".", "_", -1)
	}

	return key
}

func (f *FirehoseSender) makeESSafe(fields map[string]interface{}) map[string]interface{} {
	for key, val := range fields {
		safekey := f.makeKeyESSafe(key)
		if safekey != key {
			fields[safekey] = fields[key]
			delete(fields, key)
		}

		// ES dynamic mappings get finnicky once you start sending nested objects.
		// E.g., if one app sends a field for the first time as an object, then any log
		// sent by another app containing that field /not/ as an object will fail.
		// One solution is to decode nested objects as strings.
		_, ismap := val.(map[string]interface{})
		_, isarr := val.([]interface{})
		if ismap || isarr {
			bs, _ := json.Marshal(val)
			fields[safekey] = string(bs)
		}
	}

	return fields
}

func (f *FirehoseSender) addKVMetaFields(fields map[string]interface{}) map[string]interface{} {
	if _, ok := fields["_kvmeta"]; !ok {
		return fields
	}

	kvmeta := decode.ExtractKVMeta(fields)
	fields["kv_routes"] = kvmeta.Routes.RuleNames()
	fields["kv_team"] = kvmeta.Team
	fields["kv_language"] = kvmeta.Language
	fields["kv_version"] = kvmeta.Version
	delete(fields, "_kvmeta")

	return fields
}

func (f *FirehoseSender) calcDropLogProbability(fields map[string]interface{}) float64 {
	logTime := fields["timestamp"].(time.Time)
	delay := time.Since(logTime).Seconds() - 120
	if delay <= 0 { // Don't drop logs with less than 2 minute delay
		return 0
	}

	level := ""
	switch l := fields["level"].(type) {
	case string:
		level = l
	}

	if level == "" {
		level = "debug" // Treat unknown levels like "debug"

		// Don't throw away panics and error messages
		raw := strings.ToLower(fields["rawlog"].(string))
		if strings.Contains(raw, "panic") || strings.Contains(raw, "err") {
			level = "critical"
		}
	}

	var half_dropped float64 // the delay in which half the logs will be dropped
	switch level {
	case "critical":
		return 0 // Never drop error or critical levels
	case "trace":
		half_dropped = 600 // 10 minutes
	default:
		fallthrough // Treat unknown levels like "debug"
	case "debug":
		half_dropped = 1800 // 30 minutes
	case "info":
		half_dropped = 3600 // 60 minutes
	case "warning":
		half_dropped = 7200 // 120 minutes
	case "error":
		half_dropped = 14400 // 240 minutes
	}

	return 1 - math.Exp2(-delay/half_dropped)
}

// ProcessMessage processes messages
func (f *FirehoseSender) ProcessMessage(rawlog []byte) ([]byte, []string, error) {
	fields, err := decode.ParseAndEnhance(string(rawlog), f.deployEnv)
	if err != nil {
		return nil, nil, err
	}

	if f.isElasticsearch {
		// Ignore log lines from the elasticserch haproxy.  Otherwise a user's own search query will
		// appear in the results
		if fields["container_app"] == "haproxy-logs" && fields["decoder_msg_type"] != "Kayvee" {
			return nil, nil, kbc.ErrMessageIgnored
		}

		// Ignore log lines from the kinesis consumers starting with SEVERE: Received error...,
		// since they are an unintended result of logging while using KCL
		if fields["container_app"] != nil && fields["rawlog"] != nil &&
			strings.HasPrefix(fields["container_app"].(string), "kinesis-") &&
			strings.HasPrefix(fields["rawlog"].(string), "SEVERE: Received error line from subprocess") {
			return nil, nil, kbc.ErrMessageIgnored
		}

		if rand.Float64() < f.calcDropLogProbability(fields) {
			stats.LogDropped(fields) // Here for alerting
			return nil, nil, kbc.ErrMessageIgnored
		}

		fields = f.addKVMetaFields(fields)
		fields = f.makeESSafe(fields)
	}

	msg, err := json.Marshal(fields)
	if err != nil {
		return nil, nil, err
	}

	// add newline after each record, so that json objects in firehose will apppear one per line
	msg = append(msg, '\n')

	return msg, []string{f.streamName}, nil
}

func (f *FirehoseSender) sendRecords(batch [][]byte, tag string) (
	*firehose.PutRecordBatchOutput, error,
) {
	awsRecords := make([]*firehose.Record, len(batch))
	for idx, log := range batch {
		awsRecords[idx] = &firehose.Record{Data: log}
	}

	return f.client.PutRecordBatch(&firehose.PutRecordBatchInput{
		DeliveryStreamName: &tag,
		Records:            awsRecords,
	})
}

// SendBatch sends batches to a firehose
func (f *FirehoseSender) SendBatch(batch [][]byte, tag string) error {
	res, err := f.sendRecords(batch, tag)
	if err != nil {
		return kbc.CatastrophicSendBatchError{err.Error()}
	}

	retries := 0
	delay := 250
	for *res.FailedPutCount != 0 {
		log.WarnD("retry-failed-records", logger.M{
			"stream": tag, "failed-record-count": *res.FailedPutCount, "retries": retries,
		})

		time.Sleep(time.Duration(delay) * time.Millisecond)

		retryLogs := [][]byte{}
		for idx, entry := range res.RequestResponses {
			if entry != nil && entry.ErrorMessage != nil && *entry.ErrorMessage != "" {
				log.ErrorD("failed-record", logger.M{"stream": tag, "msg": &entry.ErrorMessage})

				retryLogs = append(retryLogs, batch[idx])
			}
		}

		res, err = f.sendRecords(retryLogs, tag)
		if err != nil {
			return kbc.CatastrophicSendBatchError{err.Error()}
		}
		if retries > 4 {
			return kbc.PartialSendBatchError{
				ErrMessage:     "Too many retries failed to put records -- stream: " + tag,
				FailedMessages: retryLogs,
			}
		}
		retries += 1
		delay *= 2
	}

	return nil
}
