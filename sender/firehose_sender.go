package sender

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	iface "github.com/aws/aws-sdk-go/service/firehose/firehoseiface"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"
	"github.com/Clever/amazon-kinesis-client-go/decode"
	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var log = logger.New("kinesis-to-firehose")

// FirehoseSender is a KCL consumer that writes records to an AWS firehose
type FirehoseSender struct {
	streamName             string
	deployEnv              string
	stringifyNested        bool
	renameESReservedFields bool
	minimumTimestamp       time.Time
	client                 iface.FirehoseAPI
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
	// StringifyNested will take any nested JSON objects and send them as strings instead of JSON objects.
	StringifyNested bool
	// RenameESReservedFields will rename any field reserved by ES, e.g. _source, to kv__<field>, e.g. kv__source.
	RenameESReservedFields bool
	// MinimumTimestamp will reject any logs with a timestamp < MinimumTimestamp
	MinimumTimestamp time.Time
}

// NewFirehoseSender creates a FirehoseSender
func NewFirehoseSender(config FirehoseSenderConfig) *FirehoseSender {
	f := &FirehoseSender{
		streamName:             config.StreamName,
		deployEnv:              config.DeployEnv,
		stringifyNested:        config.StringifyNested,
		renameESReservedFields: config.RenameESReservedFields,
		minimumTimestamp:       config.MinimumTimestamp,
	}

	awsConfig := aws.NewConfig().WithRegion(config.FirehoseRegion).WithMaxRetries(10)
	sess := session.Must(session.NewSession(awsConfig))
	f.client = firehose.New(sess)

	return f
}

// ProcessMessage processes messages
func (f *FirehoseSender) ProcessMessage(rawlog []byte) ([]byte, []string, error) {
	fields, err := decode.ParseAndEnhance(
		string(rawlog), f.deployEnv, f.stringifyNested,
		f.renameESReservedFields, f.minimumTimestamp,
	)
	if err != nil {
		return nil, nil, err
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
		log.WarnD("retry-filed-records", logger.M{
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
