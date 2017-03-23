package firehose

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Clever/heka-clever-plugins/batcher"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsFirehose "github.com/aws/aws-sdk-go/service/firehose"
)

// FirehoseWriter writes record batches to a firehose stream
type FirehoseWriter struct {
	streamName     string
	messageBatcher batcher.Batcher
	firehoseClient *awsFirehose.Firehose

	recvRecordCount   int64
	sentRecordCount   int64
	failedRecordCount int64
}

// FirehoseWriterConfig is the set of config options used in NewFirehoseWriter
type FirehoseWriterConfig struct {
	// StreamName is the firehose stream name
	StreamName string
	// Region is the AWS region the firehose stream lives in
	Region string
	// FlushInterval is how often accumulated messages should be bulk put to firehose
	FlushInterval time.Duration
	// FlushCount is the number of messages that triggers a push to firehose. Max batch size is 500, see: http://docs.aws.amazon.com/firehose/latest/dev/limits.html
	FlushCount int
	// FlushSize is the size of a batch in bytes that triggers a push to firehose. Max batch size is 4Mb (4*1024*1024), see: http://docs.aws.amazon.com/firehose/latest/dev/limits.html
	FlushSize int
}

// NewFirehoseWriter constructs a FirehoseWriter
func NewFirehoseWriter(config FirehoseWriterConfig) (*FirehoseWriter, error) {
	if config.FlushCount > 500 || config.FlushCount < 1 {
		return nil, fmt.Errorf("FlushCount must be between 1 and 500 messages")
	}
	if config.FlushSize < 1 || config.FlushSize > 4*1024*1024 {
		return nil, fmt.Errorf("FlushSize must be between 1 and 4*1024*1024 (4 Mb)")
	}

	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion(config.Region).WithMaxRetries(4)))

	f := &FirehoseWriter{
		streamName:     config.StreamName,
		firehoseClient: awsFirehose.New(sess),
	}

	f.messageBatcher = batcher.New(f)
	f.messageBatcher.FlushCount(config.FlushCount)
	f.messageBatcher.FlushInterval(config.FlushInterval)
	f.messageBatcher.FlushSize(config.FlushSize)

	return f, nil
}

// ProcessMessage reads an input string and adds it to the batcher, which will ultimately send it
func (f *FirehoseWriter) ProcessMessage(msg string) error {
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
	awsRecords := make([]*awsFirehose.Record, len(batch))
	for idx, record := range batch {
		awsRecords[idx] = &awsFirehose.Record{
			Data: record,
		}
	}

	// Write to Firehose
	output, err := f.firehoseClient.PutRecordBatch(&awsFirehose.PutRecordBatchInput{
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
