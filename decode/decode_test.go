package decode

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Clever/syslogparser"
	"github.com/stretchr/testify/assert"
)

type Spec struct {
	Input          string
	ExpectedOutput map[string]interface{}
	ExpectedError  error
}

func TestKayveeDecoding(t *testing.T) {
	specs := []Spec{
		Spec{
			Input: `{"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "",
				"postfix": "",
				"a":       "b",
				"Type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Input: `prefix {"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "prefix ",
				"postfix": "",
				"a":       "b",
				"Type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Input: `{"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "",
				"postfix": " postfix",
				"a":       "b",
				"Type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Input: `prefix {"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "prefix ",
				"postfix": " postfix",
				"a":       "b",
				"Type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Input:          `prefix { postfix`, // Just a bracket, but no JSON in body
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  fmt.Errorf(""),
		},
		Spec{
			Input:          `prefix {"a:"b"} postfix`, // JSON missing a quote
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &json.SyntaxError{},
		},
	}

	for _, spec := range specs {
		t.Run(fmt.Sprintf("input:%s", spec.Input), func(t *testing.T) {
			assert := assert.New(t)
			fields, err := FieldsFromKayvee(spec.Input)
			if spec.ExpectedError != nil {
				assert.Error(err)
				assert.IsType(spec.ExpectedError, err)
			} else {
				assert.NoError(err)
			}
			assert.Equal(spec.ExpectedOutput, fields)
		})
	}
}

func TestSyslogDecoding(t *testing.T) {
	// timestamps in Rsyslog_TraditionalFileFormat
	secPrecision := "Jan 2 15:04:05"
	logTime, err := time.Parse(secPrecision, "Oct 25 10:20:37")
	if err != nil {
		t.Fatal(err)
	}
	// parsing assumes log is from the current year
	logTime = logTime.AddDate(time.Now().Year(), 0, 0).UTC()

	logTime2, err := time.Parse(secPrecision, "Apr  5 21:45:54")
	if err != nil {
		t.Fatal(err)
	}
	logTime2 = logTime2.AddDate(time.Now().Year(), 0, 0).UTC()

	// timestamp in Rsyslog_FileFormat
	msPrecision := "2006-01-02T15:04:05.999999-07:00"
	logTime3, err := time.Parse(msPrecision, "2017-04-05T21:57:46.794862+00:00")
	if err != nil {
		t.Fatal(err)
	}
	logTime3 = logTime3.UTC()

	specs := []Spec{
		Spec{
			Input: `Oct 25 10:20:37 localhost anacron[1395]: Some log message`,
			ExpectedOutput: map[string]interface{}{
				"Timestamp":   logTime,
				"Hostname":    "localhost",
				"programname": "anacron",
				"Payload":     "Some log message",
			},
			ExpectedError: nil,
		},
		Spec{
			Input: `Oct 25 10:20:37 some-host docker/fa3a5e338a47[1294]: log body`,
			ExpectedOutput: map[string]interface{}{
				"Timestamp":   logTime,
				"Hostname":    "some-host",
				"programname": "docker/fa3a5e338a47",
				"Payload":     "log body",
			},
			ExpectedError: nil,
		},
		Spec{
			Input: `Apr  5 21:45:54 influx-service docker/0000aa112233[1234]: [httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms`,
			ExpectedOutput: map[string]interface{}{
				"Timestamp":   logTime2,
				"Hostname":    "influx-service",
				"programname": "docker/0000aa112233",
				"Payload":     "[httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms",
			},
			ExpectedError: nil,
		},
		Spec{
			Input: `Apr  5 21:45:54 mongodb-some-machine whackanop: 2017/04/05 21:46:11 found 0 ops`,
			ExpectedOutput: map[string]interface{}{
				"Timestamp":   logTime2,
				"Hostname":    "mongodb-some-machine",
				"programname": "whackanop",
				"Payload":     "2017/04/05 21:46:11 found 0 ops",
			},
			ExpectedError: nil,
		},
		Spec{
			Input: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A58961234%3Atask%2Fa1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			ExpectedOutput: map[string]interface{}{
				"Timestamp":   logTime3,
				"Hostname":    "ip-10-0-0-0",
				"programname": `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A58961234%3Atask%2Fa1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"Payload":     `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			},
			ExpectedError: nil,
		},
		Spec{
			Input:          `not rsyslog`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &syslogparser.ParserError{},
		},
	}
	for _, spec := range specs {
		t.Run(fmt.Sprintf("input:%s", spec.Input), func(t *testing.T) {
			assert := assert.New(t)
			fields, err := FieldsFromSyslog(spec.Input)
			if spec.ExpectedError != nil {
				assert.Error(err)
				assert.IsType(spec.ExpectedError, err)
			} else {
				assert.NoError(err)
			}
			assert.Equal(spec.ExpectedOutput, fields)
		})
	}
}
