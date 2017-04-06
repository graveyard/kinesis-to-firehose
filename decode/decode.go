package decode

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/Clever/syslogparser/rfc3164"
	// "github.com/satyrius/gonx"
)

// reservedFields are automatically set during decoding.
// no field written by a user (e.g. contained in the Kayvee JSON) should overwrite them.
var reservedFields = []string{
	"prefix",
	"postfix",
	"Type",
}

func stringInSlice(s string, slice []string) bool {
	for _, item := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// remapSyslog3164Keys renames fields to match our expecations from heka's syslog parser
// see: https://github.com/mozilla-services/heka/blob/278dd3d5961b9b6e47bb7a912b63ce3faaf8d8bd/sandbox/lua/decoders/rsyslog.lua
var remapSyslog3164Keys = map[string]string{
	"hostname":  "Hostname",
	"timestamp": "Timestamp",
	"tag":       "programname",
	"content":   "Payload",
}

// FieldsFromSyslog takes an RSyslog formatted log line and extracts fields from it
//
// Supports two log lines formats:
// - RSYSLOG_TraditionalFileFormat - the "old style" default log file format with low-precision timestamps (RFC3164)
// - RSYSLOG_FileFormat - a modern-style logfile format similar to TraditionalFileFormat, but with high-precision timestamps and timezone information
//
// For more details on Rsylog formats: https://rsyslog-5-8-6-doc.neocities.org/rsyslog_conf_templates.html
func FieldsFromSyslog(line string) (map[string]interface{}, error) {
	// rfc3164 includes a severity number in front of the Syslog line, but we don't use that
	fakeSeverity := "<12>"
	p3164 := rfc3164.NewParser([]byte(fakeSeverity + line))
	err := p3164.Parse()
	if err != nil {
		return map[string]interface{}{}, err
	}

	out := map[string]interface{}{}
	for k, v := range p3164.Dump() {
		if newKey, ok := remapSyslog3164Keys[k]; ok {
			out[newKey] = v
		}
	}
	return out, nil
}

type NonKayveeError struct{}

func (e NonKayveeError) Error() string {
	return fmt.Sprint("Log line is not Kayvee (doesn't have JSON payload)")
}

// FieldsFromKayvee takes a log line and extracts fields from the Kayvee (JSON) part
func FieldsFromKayvee(line string) (map[string]interface{}, error) {
	m := map[string]interface{}{}

	firstIdx := strings.Index(line, "{")
	lastIdx := strings.LastIndex(line, "}")
	if firstIdx == -1 || lastIdx == -1 || firstIdx > lastIdx {
		return map[string]interface{}{}, &NonKayveeError{}
	}
	m["prefix"] = line[:firstIdx]
	m["postfix"] = line[lastIdx+1:]

	possibleJSON := line[firstIdx : lastIdx+1]
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(possibleJSON), &fields); err != nil {
		return map[string]interface{}{}, err
	}
	for k, v := range fields {
		if !stringInSlice(k, reservedFields) {
			m[k] = v
		}
	}

	m["Type"] = "Kayvee"

	return m, nil
}

func ParseAndEnhance(line string, env string) (map[string]interface{}, error) {
	out := map[string]interface{}{}

	syslogFields, err := FieldsFromSyslog(line)
	if err != nil {
		return map[string]interface{}{}, err
	}
	for k, v := range syslogFields {
		out[k] = v
	}
	rawlog := syslogFields["Payload"].(string)
	programname := syslogFields["programname"].(string)

	// Try pulling Kayvee fields out of message
	kvFields, err := FieldsFromKayvee(rawlog)
	if err != nil {
		switch err.(type) {
		case NonKayveeError:
			// Keep going, not every line is Kayvee...
		default:
			return map[string]interface{}{}, err
		}
	} else {
		for k, v := range kvFields {
			out[k] = v
		}
	}

	// Inject additional business-logic fields
	out["rawlog"] = rawlog
	out["env"] = env
	meta, err := getContainerMeta(programname, "", "", "")
	if err == nil {
		for k, v := range meta {
			out[k] = v
		}
	}

	return out, nil
}

const containerMeta = `([a-z-]+)--([a-z-]+)\/` + // env--app
	`arn%3Aaws%3Aecs%3Aus-(west|east)-[1-2]%3A[0-9]{8}%3Atask%2F` + // ARN cruft
	`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})` // task-id

var containerMetaRegex = regexp.MustCompile(containerMeta)

func getContainerMeta(programname, force_env, force_app, force_task string) (map[string]string, error) {
	if programname == "" {
		return map[string]string{}, fmt.Errorf("no programname")
	}

	env := ""
	app := ""
	task := ""
	matches := containerMetaRegex.FindAllStringSubmatch(programname, 1)
	if len(matches) == 1 {
		env = matches[0][1]
		app = matches[0][2]
		task = matches[0][4]
	}

	if force_env != "" {
		env = force_env
	}
	if force_app != "" {
		app = force_app
	}
	if force_task != "" {
		task = force_task
	}

	if env == "" || app == "" || task == "" {
		return map[string]string{}, fmt.Errorf("unable to get one or more of env/app/task")
	}

	return map[string]string{
		"container_env":  env,
		"container_app":  app,
		"container_task": task,
		"logtag":         fmt.Sprintf("%s--%s/%s", env, app, task),
	}, nil
}

// TODO: Haproxy

// var log_format = `$remote_addr - - [$time_local] "$request_method $request $server_protocol" $status $body_bytes_sent "" "" $client_port $accept_date_ms "$frontend_name" "$backend_name" "$srv_name" $client_time $wait_time $connection_time $tcpinfo_rtt $total_time $termination_state $active_connections $frontend_concurrent_connections $backend_concurrent_connections $server_concurrent_connections $retries $server_queue $backend_queue "$captured_request_cookie" "$captured_response_cookie" "$x_forwarded_for" "$http_user_agent" "$authorization"`
// var haproxyParser = gonx.NewParser(log_format)

// // FieldsFromHaproxy takes a log line and extracts fields from the Haproxy part
// func FieldsFromHaproxy(line string) (map[string]interface{}, error) {
// 	entry, err := haproxyParser.ParseString(line)
// 	if err != nil {
// 		return map[string]interface{}{}, err
// 	}

// 	out := map[string]interface{}{}
// 	for k, v := range entry {
// 		out[k] = v
// 	}
// 	return out, nil
// }

// TODO: KayveeRouting
