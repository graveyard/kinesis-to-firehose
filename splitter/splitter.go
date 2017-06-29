package splitter

import (
	"bytes"
	"compress/gzip"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"time"
)

// By setting up a CloudWatchLogs Subscription, we can get logs from one or more CW Logs streams into Kinesis.
// However, these logs are combined in a different format than the other logs we read (effectively, one log per record).

type LogEvent struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

type LogEventBatch struct {
	MessageType         string     `json:"messageType"`
	Owner               string     `json:"owner"`
	LogGroup            string     `json:"logGroup"`
	LogStream           string     `json:"logStream"`
	SubscriptionFilters []string   `json:"subscriptionFilters"`
	LogEvents           []LogEvent `json:"logEvents"`
}

// Unpack expects a base64 encoded + gzipped + json-stringified LogEventBatch
func Unpack(input string) (LogEventBatch, error) {
	decoded, err := b64.StdEncoding.DecodeString(input)
	if err != nil {
		return LogEventBatch{}, err
	}

	gzipReader, err := gzip.NewReader(bytes.NewReader([]byte(decoded)))
	if err != nil {
		return LogEventBatch{}, err
	}

	byt, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		return LogEventBatch{}, err
	}

	var dat LogEventBatch
	if err := json.Unmarshal(byt, &dat); err != nil {
		return LogEventBatch{}, err
	}

	return dat, nil
}

const RFC3339Micro = "2006-01-02T15:04:05.999999-07:00"

const taskMeta = `([a-z0-9-]+)--([a-z0-9-]+)\/` + // env--app
	`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/` + // task-id
	`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})` // container-id

var taskRegex = regexp.MustCompile(taskMeta)

func Split(b LogEventBatch) []string {
	out := []string{}

	env := "unknown"
	app := "unknown"
	task := "00001111-2222-3333-4444-555566667777"
	matches := taskRegex.FindAllStringSubmatch(b.LogStream, 1)
	if len(matches) == 1 {
		env = matches[0][1]
		app = matches[0][2]
		task = matches[0][3]
	}

	rsyslogPrefix := `%s %s %s[%d]: %s`
	// programName is a mocked ARN in the format expected by our log decoders
	programName := env + "--" + app + `/arn%3Aaws%3Aecs%3Aus-east-1%3A999988887777%3Atask%2F` + task
	mockPid := 1
	hostname := "aws-batch"

	for _, event := range b.LogEvents {
		// Adding an extra Microsecond forces `Format` to include all 6 digits within the micorsecond format.
		// Otherwise, time.Format omits trailing zeroes. (https://github.com/golang/go/issues/12472)
		logTime := time.Unix(0, event.Timestamp*int64(time.Millisecond)+int64(time.Microsecond)).UTC().Format(RFC3339Micro)

		// Fake an RSyslog prefix, expected by consumers
		formatted := fmt.Sprintf(rsyslogPrefix, logTime, hostname, programName, mockPid, event.Message)
		out = append(out, formatted)
	}

	return out
}
