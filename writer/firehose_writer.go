package writer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sync/atomic"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
	"github.com/Clever/heka-clever-plugins/batcher"
	"github.com/aws/aws-sdk-go/service/firehose"
	iface "github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"golang.org/x/time/rate"
)

type FirehoseWriter struct {
	shardID string
	logFile string

	// KCL checkpointing
	sleepDuration     time.Duration
	checkpointRetries int
	checkpointFreq    time.Duration
	largestSeq        *big.Int
	largestSubSeq     int
	lastCheckpoint    time.Time

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
}

func NewFirehoseWriter(config FirehoseWriterConfig, limiter *rate.Limiter) (*FirehoseWriter, error) {
	if config.FlushCount > 500 || config.FlushCount < 1 {
		return nil, fmt.Errorf("FlushCount must be between 1 and 500 messages")
	}
	if config.FlushSize < 1 || config.FlushSize > 4*1024*1024 {
		return nil, fmt.Errorf("FlushSize must be between 1 and 4*1024*1024 (4 Mb)")
	}

	f := &FirehoseWriter{
		streamName:        config.StreamName,
		firehoseClient:    config.FirehoseClient,
		sleepDuration:     5 * time.Second,
		checkpointRetries: 5,
		checkpointFreq:    60 * time.Second,
		rateLimiter:       limiter,
		logFile:           config.LogFile,
	}

	f.messageBatcher = batcher.New(f)
	f.messageBatcher.FlushCount(config.FlushCount)
	f.messageBatcher.FlushInterval(config.FlushInterval)
	f.messageBatcher.FlushSize(config.FlushSize)

	return f, nil
}

func (f *FirehoseWriter) Initialize(shardID string) error {
	f.shardID = shardID
	f.lastCheckpoint = time.Now()
	return nil
}

// TODO: refactor KCL checkpointer to include retries / sleep duration
func (f *FirehoseWriter) checkpoint(checkpointer kcl.Checkpointer, sequenceNumber string, subSequenceNumber int) {
	for n := -1; n < f.checkpointRetries; n++ {
		err := checkpointer.Checkpoint(sequenceNumber, subSequenceNumber)
		if err == nil {
			return
		}

		if cperr, ok := err.(kcl.CheckpointError); ok {
			switch cperr.Error() {
			case "ShutdownException":
				fmt.Fprintf(os.Stderr, "Encountered shutdown exception, skipping checkpoint\n")
				return
			case "ThrottlingException":
				fmt.Fprintf(os.Stderr, "Was throttled while checkpointing, will attempt again in %s", f.sleepDuration)
			case "InvalidStateException":
				fmt.Fprintf(os.Stderr, "MultiLangDaemon reported an invalid state while checkpointing\n")
			default:
				fmt.Fprintf(os.Stderr, "Encountered an error while checkpointing: %s", err)
			}
		}

		if n == f.checkpointRetries {
			fmt.Fprintf(os.Stderr, "Failed to checkpoint after %d attempts, giving up.\n", f.checkpointRetries)
			return
		}

		time.Sleep(f.sleepDuration)
	}
}

// shouldUpdateSequence determines whether a new larger sequence number is available
func (f *FirehoseWriter) shouldUpdateSequence(sequenceNumber *big.Int, subSequenceNumber int) bool {
	return f.largestSeq == nil || sequenceNumber.Cmp(f.largestSeq) == 1 ||
		(sequenceNumber.Cmp(f.largestSeq) == 0 && subSequenceNumber > f.largestSubSeq)
}

func (f *FirehoseWriter) ProcessRecords(records []kcl.Record, checkpointer kcl.Checkpointer) error {
	for _, record := range records {
		// Wait until rate limiter permits one record to be processed
		f.rateLimiter.Wait(context.Background())

		// Base64 decode the record
		data, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			return err
		}
		msg := string(data)

		// Write the message to firehose
		err = f.processMessage(msg)
		if err != nil {
			return err
		}

		// Handle checkpointing
		seqNumber := new(big.Int)
		if _, ok := seqNumber.SetString(record.SequenceNumber, 10); !ok {
			fmt.Fprintf(os.Stderr, "could not parse sequence number '%s'\n", record.SequenceNumber)
			continue
		}
		if f.shouldUpdateSequence(seqNumber, record.SubSequenceNumber) {
			f.largestSeq = seqNumber
			f.largestSubSeq = record.SubSequenceNumber
		}
	}
	if time.Now().Sub(f.lastCheckpoint) > f.checkpointFreq {
		f.checkpoint(checkpointer, f.largestSeq.String(), f.largestSubSeq)
		f.lastCheckpoint = time.Now()

		// Write status to file
		err := appendToFile(f.logFile, fmt.Sprintf("%s -- %s\n", f.shardID, f.Status()))
		if err != nil {
			return err
		}
	}
	return nil
}

func appendToFile(filename, text string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.WriteString(text); err != nil {
		return err
	}

	return nil
}

func (f *FirehoseWriter) Shutdown(checkpointer kcl.Checkpointer, reason string) error {
	if reason == "TERMINATE" {
		fmt.Fprintf(os.Stderr, "Was told to terminate, will attempt to checkpoint.\n")
		f.FlushAll()
		f.checkpoint(checkpointer, "", 0)
	} else {
		fmt.Fprintf(os.Stderr, "Shutting down due to failover. Reason: %s. Will not checkpoint.\n", reason)
	}
	return nil
}

// ProcessMessage reads an input string and adds it to the batcher, which will ultimately send it
func (f *FirehoseWriter) processMessage(msg string) error {
	atomic.AddInt64(&f.recvRecordCount, 1)

	// TODO: Fully decode the message
	fields := map[string]interface{}{
		"rawlog": msg,
	}

	record, err := json.Marshal(fields)
	if err != nil {
		atomic.AddInt64(&f.failedRecordCount, 1)
		return err
	}

	return f.messageBatcher.Send(record)
}

// Flush writes a batch of records to AWS Firehose
func (f *FirehoseWriter) Flush(batch [][]byte) {
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
}

// Status returns the number of received, sent, and failed records
func (f *FirehoseWriter) Status() string {
	return fmt.Sprintf("Received:%d Sent:%d Failed:%d", f.recvRecordCount, f.sentRecordCount, f.failedRecordCount)
}

// FlushAll flushes all remaining messages in the batcher.
func (f *FirehoseWriter) FlushAll() {
	f.messageBatcher.Flush()
}
