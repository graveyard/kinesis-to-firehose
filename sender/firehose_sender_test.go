package sender

import (
	"testing"

	"github.com/Clever/kinesis-to-firehose/sender/mock_firehoseiface"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setupFirehoseSender(t *testing.T) *FirehoseSender {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := mock_firehoseiface.NewMockFirehoseAPI(mockCtrl)
	return &FirehoseSender{
		streamName: "tester",
		client:     mockFirehoseAPI,
	}
}

func TestInitFirehoseWriter(t *testing.T) {
	_ = setupFirehoseSender(t)
}

func TestProcessRecord(t *testing.T) {
	sender := setupFirehoseSender(t)

	d := `Apr  5 21:45:54 influx-service docker/0000aa112233[1234]: [httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms`
	_, tags, err := sender.ProcessMessage([]byte(d))
	assert.NoError(t, err)
	assert.Contains(t, tags, sender.streamName)
}
