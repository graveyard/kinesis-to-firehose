package sender

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/Clever/kinesis-to-firehose/mocks"
)

func setupFirehoseSender(t *testing.T) *FirehoseSender {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := mocks.NewMockFirehoseAPI(mockCtrl)
	return &FirehoseSender{
		streamName: "tester",
		client:     mockFirehoseAPI,
	}
}

func TestInitFirehoseWriter(t *testing.T) {
	_ = setupFirehoseSender(t)
}

func TestProcessMessageForES(t *testing.T) {
	sender := setupFirehoseSender(t)

	msg := `Apr  5 21:45:54 influx-service docker/0000aa112233[1234]: [httpd] 2017/04/05 ` +
		`21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST ` +
		`/write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http ` +
		`123456-1234-1234-b11b-000000000000 13.688672ms`
	_, tags, err := sender.ProcessMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Contains(t, tags, sender.streamName)

	msg = `2017-08-16T04:37:52.901092+00:00 ip-10-0-102-159 production--haproxy-logs/` +
		`arn%3Aaws%3Aecs%3Aus-west-1%3A589690932525%3Atask%2F124cc8a5-0549-4149-922b-cd411b813d11` +
		`[3252]:  {"timestamp":1502858272,"http_status":200,"request_method":"POST","request":"/` +
		`.kibana-4/__kibanaQueryValidator/_validate/query?explain=true&ignore_unavailable=true",` +
		`"response_time":25,"termination_state":"----","request_body":"{"query":{"query_string":` +
		`{"query":"\"Franklin County School District\"","analyze_wildcard":true,` +
		`"lowercase_expanded_terms":false}}}","backend_name":"elasticsearch"}`
	_, _, err = sender.ProcessMessage([]byte(msg))
	assert.NoError(t, err)
}
