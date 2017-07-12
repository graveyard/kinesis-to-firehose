package writer

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"testing"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
	"github.com/Clever/kinesis-to-firehose/writer/mock_firehoseiface"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"golang.org/x/time/rate"
)

func setupFirehoseWriter(t *testing.T) *FirehoseWriter {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := mock_firehoseiface.NewMockFirehoseAPI(mockCtrl)
	cfg := FirehoseWriterConfig{
		FlushCount:     100,
		FlushSize:      4 * 1024 * 1024, // 4 Mb
		FlushInterval:  time.Hour,
		FirehoseClient: mockFirehoseAPI,
	}
	limiter := rate.Limiter{}
	writer, err := NewFirehoseWriter(cfg, &limiter)
	assert.NoError(t, err)
	return writer
}

func TestInitFirehoseWriter(t *testing.T) {
	_ = setupFirehoseWriter(t)
}

func TestProcessRecord(t *testing.T) {
	writer := setupFirehoseWriter(t)

	d := `Apr  5 21:45:54 influx-service docker/0000aa112233[1234]: [httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms`
	err := writer.processRecord(kcl.Record{
		Data:              base64.StdEncoding.EncodeToString([]byte(d)),
		SequenceNumber:    "1",
		SubSequenceNumber: 0,
	})
	assert.NoError(t, err)
}

func TestProcessRecordFromCWLogsSubscription(t *testing.T) {
	assert := assert.New(t)

	writer := setupFirehoseWriter(t)

	t.Log("given a log from CW Logs subscription...")
	d := `{"messageType":"DATA_MESSAGE","owner":"123456789012","logGroup":"/aws/batch/job","logStream":"environment--app/11111111-2222-3333-4444-555566667777/88889999-0000-aaa-bbbb-ccccddddeeee","subscriptionFilters":["MySubscriptionFilter"],"logEvents":[{"id":"33418742379011144044923130086453437181614530551221780480","timestamp":1498548236012,"message":"some log line"},{"id":"33418742387663833181953011865369295871402094815542181889","timestamp":1498548236400,"message":"2017/06/27 07:23:56 Another log line"}]}`

	t.Log("gzip it...")
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	_, err := gz.Write([]byte(d))
	assert.NoError(err)
	err = gz.Flush()
	assert.NoError(err)
	err = gz.Close()
	assert.NoError(err)

	t.Log("base64 encode it...")
	encoded := base64.StdEncoding.EncodeToString(b.Bytes())

	t.Log("verify we can process it")
	err = writer.processRecord(kcl.Record{
		Data:              encoded,
		SequenceNumber:    "1",
		SubSequenceNumber: 0,
	})
	assert.NoError(err)
}
