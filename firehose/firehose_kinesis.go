package firehose

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/Clever/heka-clever-plugins/aws"
	"github.com/Clever/heka-clever-plugins/batcher"
)

type FirehoseWriter struct {
	conf     *FirehoseWriterConfig
	batchers map[string]batcher.Batcher

	mockEndpoint string

	recvRecordCount    int64
	sentRecordCount    int64
	droppedRecordCount int64
}

type FirehoseWriterConfig struct {
	// The value of this field is used as the firehose stream name
	StreamName string
	// AWS region the stream lives in
	Region string `toml:"region"`
	// Interval at which accumulated messages should be bulk put to
	// firehose, in milliseconds (default 1000, i.e. 1 second).
	FlushInterval uint32 `toml:"flush_interval"`
	// Number of messages that triggers a push to firehose
	// (default to 1, maximum is 500)
	FlushCount int `toml:"flush_count"`
	// Size of batch that triggers a push to firehose
	// (default to 1024 * 1024 (1mb))
	FlushSize int `toml:"flush_size"`
}

func NewFirehoseWriter(config FirehoseWriterConfig, mockEndpoint string) (*FirehoseWriter, error) {
	if config.FlushCount > 500 || config.FlushCount < 1 {
		return nil, fmt.Errorf("FlushCount must be between 1 and 500 messages")
	}
	return &FirehoseWriter{
		conf:         &config,
		mockEndpoint: mockEndpoint,
		batchers:     map[string]batcher.Batcher{},
	}, nil
}

func (f *FirehoseWriter) ProcessMessage(msg string) error {
	atomic.AddInt64(&f.recvRecordCount, 1)

	// TODO: Fully decode the message
	fields := map[string]interface{}{
		"rawlog": msg,
	}

	record, err := json.Marshal(fields)
	if err != nil {
		atomic.AddInt64(&f.droppedRecordCount, 1)
		return err
	}

	batch, ok := f.batchers[f.conf.StreamName]
	if !ok {
		sync := f.createBatcherSync(f.conf.StreamName)
		batch = batcher.New(sync)
		f.batchers[f.conf.StreamName] = batch
	}
	batch.Send(record)

	return nil
}

func (f *FirehoseWriter) createBatcherSync(seriesName string) batcher.Sync {
	var client aws.RecordPutter

	if f.mockEndpoint == "" {
		client = aws.NewFirehose(f.conf.Region, seriesName)
	} else {
		client = aws.NewMockRecordPutter(seriesName, f.mockEndpoint)
	}

	return &syncPutterAdapter{client: client, output: f}
}

type syncPutterAdapter struct {
	client aws.RecordPutter
	output *FirehoseWriter
}

func (s *syncPutterAdapter) Flush(batch [][]byte) {
	count := int64(len(batch))

	err := s.client.PutRecordBatch(batch)
	if err != nil {
		atomic.AddInt64(&s.output.droppedRecordCount, count)
	} else {
		atomic.AddInt64(&s.output.sentRecordCount, count)
	}
}

type FirehoseWriterStatus struct {
	RecvRecordCount    int64
	SentRecordCount    int64
	DroppedRecordCount int64
}

func (f *FirehoseWriter) Status() FirehoseWriterStatus {
	return FirehoseWriterStatus{
		RecvRecordCount:    f.recvRecordCount,
		SentRecordCount:    f.sentRecordCount,
		DroppedRecordCount: f.droppedRecordCount,
	}
}

// FlushAll flushes all batches. It's useful when shutting down.
func (f *FirehoseWriter) FlushAll() {
	for _, batch := range f.batchers {
		batch.Flush()
	}
}
