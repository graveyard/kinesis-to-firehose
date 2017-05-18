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
	Title          string
	Input          string
	ExpectedOutput map[string]interface{}
	ExpectedError  error
}

func TestKayveeDecoding(t *testing.T) {
	specs := []Spec{
		Spec{
			Title: "handles just JSON",
			Input: `{"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "",
				"postfix": "",
				"a":       "b",
				"type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles prefix + JSON",
			Input: `prefix {"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "prefix ",
				"postfix": "",
				"a":       "b",
				"type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles JSON + postfix",
			Input: `{"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "",
				"postfix": " postfix",
				"a":       "b",
				"type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "handles prefix + JSON + postfix",
			Input: `prefix {"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "prefix ",
				"postfix": " postfix",
				"a":       "b",
				"type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		Spec{
			Title:          "Returns NonKayveeError if not JSON in body",
			Input:          `prefix { postfix`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &NonKayveeError{},
		},
		Spec{
			Title:          "errors on invalid JSON (missing a quote)",
			Input:          `prefix {"a:"b"} postfix`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &json.SyntaxError{},
		},
	}

	for _, spec := range specs {
		t.Run(fmt.Sprintf(spec.Title), func(t *testing.T) {
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
			Title: "Parses Rsyslog_TraditionalFileFormat with simple log body",
			Input: `Oct 25 10:20:37 some-host docker/fa3a5e338a47[1294]: log body`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":   logTime,
				"hostname":    "some-host",
				"programname": "docker/fa3a5e338a47",
				"rawlog":      "log body",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_TraditionalFileFormat with haproxy access log body",
			Input: `Apr  5 21:45:54 influx-service docker/0000aa112233[1234]: [httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":   logTime2,
				"hostname":    "influx-service",
				"programname": "docker/0000aa112233",
				"rawlog":      "[httpd] 2017/04/05 21:45:54 172.17.42.1 - heka [05/Apr/2017:21:45:54 +0000] POST /write?db=foo&precision=ms HTTP/1.1 204 0 - Go 1.1 package http 123456-1234-1234-b11b-000000000000 13.688672ms",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_TraditionalFileFormat",
			Input: `Apr  5 21:45:54 mongodb-some-machine whackanop: 2017/04/05 21:46:11 found 0 ops`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":   logTime2,
				"hostname":    "mongodb-some-machine",
				"programname": "whackanop",
				"rawlog":      "2017/04/05 21:46:11 found 0 ops",
			},
			ExpectedError: nil,
		},
		Spec{
			Title: "Parses Rsyslog_ FileFormat with Kayvee payload",
			Input: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			ExpectedOutput: map[string]interface{}{
				"timestamp":   logTime3,
				"hostname":    "ip-10-0-0-0",
				"programname": `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":      `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
			},
			ExpectedError: nil,
		},
		Spec{
			Title:          "Fails to parse non-RSyslog log line",
			Input:          `not rsyslog`,
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &syslogparser.ParserError{},
		},
	}
	for _, spec := range specs {
		t.Run(fmt.Sprintf(spec.Title), func(t *testing.T) {
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

type ParseAndEnhanceInput struct {
	Line                   string
	StringifyNested        bool
	RenameESReservedFields bool
	MinimumTimestamp       time.Time
}

type ParseAndEnhanceSpec struct {
	Title          string
	Input          ParseAndEnhanceInput
	ExpectedOutput map[string]interface{}
	ExpectedError  error
}

func TestParseAndEnhance(t *testing.T) {
	// timestamp in Rsyslog_FileFormat
	msPrecision := "2006-01-02T15:04:05.999999-07:00"
	logTime3, err := time.Parse(msPrecision, "2017-04-05T21:57:46.794862+00:00")
	if err != nil {
		t.Fatal(err)
	}
	logTime3 = logTime3.UTC()

	specs := []ParseAndEnhanceSpec{
		ParseAndEnhanceSpec{
			Title: "Parses a Kayvee log line from an ECS app",
			Input: ParseAndEnhanceInput{Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Parses a Kayvee log line from an ECS app, with override to container_app",
			Input: ParseAndEnhanceInput{Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished","container_app":"force-app"}`},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished","container_app":"force-app"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "force-app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Parses a non-Kayvee log line",
			Input: ParseAndEnhanceInput{Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: some log`},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `some log`,
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title:          "Fails to parse non-RSyslog log line",
			Input:          ParseAndEnhanceInput{Line: `not rsyslog`},
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &syslogparser.ParserError{},
		},
		ParseAndEnhanceSpec{
			Title: "Parses JSON values",
			Input: ParseAndEnhanceInput{Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"nested":         map[string]interface{}{"a": "b"},
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Has the option to stringify object values",
			Input: ParseAndEnhanceInput{
				Line:            `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`,
				StringifyNested: true,
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": {"a":"b"}}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"nested":         `{"a":"b"}`,
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Has the option to stringify array values",
			Input: ParseAndEnhanceInput{
				Line:            `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": [{"a":"b"}]}`,
				StringifyNested: true,
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "nested": [{"a":"b"}]}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"nested":         `[{"a":"b"}]`,
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Has the option to rename reserved ES fields",
			Input: ParseAndEnhanceInput{
				Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				RenameESReservedFields: true,
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"kv__source":     "a",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Errors if logTime < MinimumTimestamp",
			Input: ParseAndEnhanceInput{
				Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				RenameESReservedFields: true,
				MinimumTimestamp:       time.Now().Add(100 * time.Hour * 24 * 365), // good thru year 2117
			},
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  fmt.Errorf(""),
		},
		ParseAndEnhanceSpec{
			Title: "Accepts logs if logTime > MinimumTimestamp",
			Input: ParseAndEnhanceInput{
				Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				RenameESReservedFields: true,
				MinimumTimestamp:       time.Now().Add(-100 * time.Hour * 24 * 365), // good thru year 2117
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"kv__source":     "a",
			},
			ExpectedError: nil,
		},
		ParseAndEnhanceSpec{
			Title: "Accepts logs if logTime > MinimumTimestamp",
			Input: ParseAndEnhanceInput{
				Line: `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				RenameESReservedFields: true,
				MinimumTimestamp:       time.Now().Add(-100 * time.Hour * 24 * 365), // good thru year 2117
			},
			ExpectedOutput: map[string]interface{}{
				"timestamp":      logTime3,
				"hostname":       "ip-10-0-0-0",
				"programname":    `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`,
				"rawlog":         `2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished", "_source": "a"}`,
				"title":          "request_finished",
				"type":           "Kayvee",
				"prefix":         "2017/04/05 21:57:46 some_file.go:10: ",
				"postfix":        "",
				"env":            "deploy-env",
				"container_env":  "env",
				"container_app":  "app",
				"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
				"kv__source":     "a",
			},
			ExpectedError: nil,
		},
	}
	for _, spec := range specs {
		t.Run(fmt.Sprintf(spec.Title), func(t *testing.T) {
			assert := assert.New(t)
			fields, err := ParseAndEnhance(spec.Input.Line, "deploy-env", spec.Input.StringifyNested, spec.Input.RenameESReservedFields, spec.Input.MinimumTimestamp)
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

func TestGetContainerMeta(t *testing.T) {
	assert := assert.New(t)

	t.Log("Must have a programname to get container meta")
	programname := ""
	_, err := getContainerMeta(programname, "", "", "")
	assert.Error(err)

	t.Log("Can parse a programname")
	programname = `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`
	meta, err := getContainerMeta(programname, "", "", "")
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  "env",
		"container_app":  "app",
		"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
	}, meta)

	t.Log("Can override just 'env'")
	overrideEnv := "force-env"
	meta, err = getContainerMeta(programname, overrideEnv, "", "")
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  overrideEnv,
		"container_app":  "app",
		"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
	}, meta)

	t.Log("Can override just 'app'")
	overrideApp := "force-app"
	meta, err = getContainerMeta(programname, "", overrideApp, "")
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  "env",
		"container_app":  overrideApp,
		"container_task": "abcd1234-1a3b-1a3b-1234-d76552f4b7ef",
	}, meta)

	t.Log("Can override just 'task'")
	overrideTask := "force-task"
	meta, err = getContainerMeta(programname, "", "", overrideTask)
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  "env",
		"container_app":  "app",
		"container_task": overrideTask,
	}, meta)

	t.Log("Can override all fields")
	programname = `env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef`
	meta, err = getContainerMeta(programname, overrideEnv, overrideApp, overrideTask)
	assert.NoError(err)
	assert.Equal(map[string]string{
		"container_env":  overrideEnv,
		"container_app":  overrideApp,
		"container_task": overrideTask,
	}, meta)
}

// Benchmarks
const benchmarkLine = `2017-04-05T21:57:46.794862+00:00 ip-10-0-0-0 env--app/arn%3Aaws%3Aecs%3Aus-west-1%3A999988887777%3Atask%2Fabcd1234-1a3b-1a3b-1234-d76552f4b7ef[3291]: 2017/04/05 21:57:46 some_file.go:10: {"title":"request_finished"}`

func BenchmarkFieldsFromKayvee(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := FieldsFromKayvee(benchmarkLine)
		if err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkFieldsFromSyslog(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := FieldsFromSyslog(benchmarkLine)
		if err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkParseAndEnhance(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := ParseAndEnhance(benchmarkLine, "env", false, false, time.Time{})
		if err != nil {
			b.FailNow()
		}
	}
}
