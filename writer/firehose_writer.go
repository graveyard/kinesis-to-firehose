package writer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"sync/atomic"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
	"github.com/Clever/kinesis-to-firehose/batcher"
	"github.com/Clever/kinesis-to-firehose/decode"
	"github.com/Clever/kinesis-to-firehose/splitter"
	"github.com/aws/aws-sdk-go/service/firehose"
	iface "github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"golang.org/x/time/rate"
)

// FirehoseWriter is a KCL consumer that writes records to an AWS firehose
type FirehoseWriter struct {
	shardID                string
	checkpointer           *kcl.Checkpointer
	logFile                string
	deployEnv              string
	stringifyNested        bool
	renameESReservedFields bool
	minimumTimestamp       time.Time

	// KCL checkpointing
	sleepDuration        time.Duration
	checkpointRetries    int
	checkpointFreq       time.Duration
	lastCheckpoint       time.Time
	largestSeqFlushed    *big.Int
	largestSubSeqFlushed int

	// Limits the number of records processed per second
	rateLimiter *rate.Limiter

	// Firehose Config
	streamName     string
	messageBatcher batcher.Batcher
	firehoseClient iface.FirehoseAPI

	// Firehose metrics
	recvRecordCount   int64
	sentRecordCount   int64
	failedRecordCount int64
}

// FirehoseWriterConfig is the set of config options used in NewFirehoseWriter
type FirehoseWriterConfig struct {
	// FirehoseClient allows writing to the Firehose API
	FirehoseClient iface.FirehoseAPI
	// StreamName is the firehose stream name
	StreamName string
	// FlushInterval is how often accumulated messages should be bulk put to firehose
	FlushInterval time.Duration
	// FlushCount is the number of messages that triggers a push to firehose. Max batch size is 500, see: http://docs.aws.amazon.com/firehose/latest/dev/limits.html
	FlushCount int
	// FlushSize is the size of a batch in bytes that triggers a push to firehose. Max batch size is 4Mb (4*1024*1024), see: http://docs.aws.amazon.com/firehose/latest/dev/limits.html
	FlushSize int
	// LogFile is the path to a log file (we write logs in a file since stdout/stderr are used by KCL)
	LogFile string
	// DeployEnvironment is the name of the runtime environment ("development" or "production")
	// It is used in the decoder to inject an environment into logs.
	DeployEnvironment string
	// StringifyNested will take any nested JSON objects and send them as strings instead of JSON objects.
	StringifyNested bool
	// RenameESReservedFields will rename any field reserved by ES, e.g. _source, to kv__<field>, e.g. kv__source.
	RenameESReservedFields bool
	// MinimumTimestamp will reject any logs with a timestamp < MinimumTimestamp
	MinimumTimestamp time.Time
}

// NewFirehoseWriter creates a FirehoseWriter
func NewFirehoseWriter(config FirehoseWriterConfig, limiter *rate.Limiter) (*FirehoseWriter, error) {
	if config.FlushCount > 500 || config.FlushCount < 1 {
		return nil, fmt.Errorf("FlushCount must be between 1 and 500 messages")
	}
	if config.FlushSize < 1 || config.FlushSize > 4*1024*1024 {
		return nil, fmt.Errorf("FlushSize must be between 1 and 4*1024*1024 (4 Mb)")
	}

	f := &FirehoseWriter{
		streamName:             config.StreamName,
		firehoseClient:         config.FirehoseClient,
		sleepDuration:          5 * time.Second,
		checkpointRetries:      5,
		checkpointFreq:         60 * time.Second,
		rateLimiter:            limiter,
		logFile:                config.LogFile,
		deployEnv:              config.DeployEnvironment,
		stringifyNested:        config.StringifyNested,
		renameESReservedFields: config.RenameESReservedFields,
		minimumTimestamp:       config.MinimumTimestamp,
	}

	f.messageBatcher = batcher.New(f, config.FlushInterval, config.FlushCount, config.FlushSize)

	return f, nil
}

// Initialize is called when the KCL starts a shard consumer (KCL interface)
func (f *FirehoseWriter) Initialize(shardID string, checkpointer *kcl.Checkpointer) error {
	f.shardID = shardID
	f.checkpointer = checkpointer
	f.lastCheckpoint = time.Now()
	return nil
}

// ProcessRecords is called when the KCL passes records to the KCL consumer (KCL interface)
func (f *FirehoseWriter) ProcessRecords(records []kcl.Record) error {
	for _, record := range records {
		// Wait until rate limiter permits one more record to be processed
		f.rateLimiter.Wait(context.Background())
		atomic.AddInt64(&f.recvRecordCount, 1)
		err := f.processRecord(record)
		if err != nil {
			atomic.AddInt64(&f.failedRecordCount, 1)
			continue
		}
	}

	// Checkpoint Kinesis stream
	if time.Now().Sub(f.lastCheckpoint) > f.checkpointFreq {
		largestSeq := f.largestSeqFlushed.String()
		f.checkpointer.CheckpointWithRetry(&largestSeq, &f.largestSubSeqFlushed, f.checkpointRetries)
		f.lastCheckpoint = time.Now()
		log.Printf(fmt.Sprintf("%s -- Received:%d Sent:%d Failed:%d\n", f.shardID, f.recvRecordCount, f.sentRecordCount, f.failedRecordCount))
	}

	return nil
}

// processRecord handles a single log line, which may be batched (from CWLogs Subscription)
func (f *FirehoseWriter) processRecord(record kcl.Record) error {
	// base64 decode
	decoded, err := base64.StdEncoding.DecodeString(record.Data)
	if err != nil {
		return err
	}
	data := string(decoded)

	// We handle two types of records:
	// - records emitted from CWLogs Subscription
	// - records emiited from KPL
	if splitter.IsGzipped(data) {
		// Process a batch of messages from a CWLogs Subscription
		messages, err := splitter.GetMessagesFromGzippedInput(data, f.deployEnv == "production")
		if err != nil {
			return err
		}
		var lastErr error
		for _, m := range messages {
			// TODO: improve checkpointing. Currently, if any message from the CWLogs batched record is sent,
			// then the whole record will be considered complete after the next checkpoint operation.
			err := f.parseMessageAndPrepareToSend(m, record.SequenceNumber, record.SubSequenceNumber)
			if err != nil {
				lastErr = err
			}
		}
		return lastErr
	}

	// Process a single message, from KPL
	return f.parseMessageAndPrepareToSend(data, record.SequenceNumber, record.SubSequenceNumber)
}

// parseMessageAndPrepareToSend is called within processRecord.
// - it first decodes and enriches the log line.
// - it then adds that output to the messageBatcher, where it will eventually be sent to Firehose.
func (f *FirehoseWriter) parseMessageAndPrepareToSend(message string, sequenceNumber string, subSequenceNumber int) error {
	fields, err := decode.ParseAndEnhance(message, f.deployEnv, f.stringifyNested, f.renameESReservedFields, f.minimumTimestamp)
	if err != nil {
		return err
	}

	msg, err := json.Marshal(fields)
	if err != nil {
		return err
	}

	// add newline after each record, so that json objects in firehose will apppear one per line
	msg = append(msg, '\n')

	err = f.messageBatcher.AddMessage(msg, sequenceNumber, subSequenceNumber)
	if err != nil {
		return err
	}

	return nil
}

// Shutdown is called when the KCL wants to trigger a shutdown of the shard consumer (KCL interface)
func (f *FirehoseWriter) Shutdown(reason string) error {
	if reason == "TERMINATE" {
		fmt.Fprintf(os.Stderr, "Was told to terminate, will attempt to checkpoint.\n")
		f.messageBatcher.Flush()
		f.checkpointer.Shutdown()
	} else {
		fmt.Fprintf(os.Stderr, "Shutting down due to failover. Reason: %s. Will not checkpoint.\n", reason)
	}
	return nil
}

// SendBatch writes a batch of records to AWS Firehose
func (f *FirehoseWriter) SendBatch(batch [][]byte, sequenceNumber *big.Int, subSequenceNumber int) {
	// Construct the array of firehose.Records
	awsRecords := make([]*firehose.Record, len(batch))
	for idx, record := range batch {
		awsRecords[idx] = &firehose.Record{
			Data: record,
		}
	}

	// Write to Firehose
	output, err := f.firehoseClient.PutRecordBatch(&firehose.PutRecordBatchInput{
		DeliveryStreamName: &f.streamName,
		Records:            awsRecords,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing to Firehose: %s\n", err.Error())
	}

	// Track success/failure counts
	sentCount := int64(len(batch))
	if output.FailedPutCount != nil {
		atomic.AddInt64(&f.failedRecordCount, *output.FailedPutCount)
		sentCount -= *output.FailedPutCount
	}
	atomic.AddInt64(&f.sentRecordCount, sentCount)

	// Track largest sequence number flushed, so we can:
	// - checkpoint that sequence number in ProcessRecords
	// - TODO: prevent ProcessRecords from getting too far ahead of last message successfully flushed
	f.largestSeqFlushed = sequenceNumber
	f.largestSubSeqFlushed = subSequenceNumber
}
