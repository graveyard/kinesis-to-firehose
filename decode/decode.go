package decode

import (
	"encoding/json"
	"fmt"
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

// FieldsFromKayvee takes a log line and extracts fields from the Kayvee (JSON) part
func FieldsFromKayvee(line string) (map[string]interface{}, error) {
	m := map[string]interface{}{}

	firstIdx := strings.Index(line, "{")
	lastIdx := strings.LastIndex(line, "}")
	if firstIdx == -1 || lastIdx == -1 || firstIdx > lastIdx {
		return map[string]interface{}{}, fmt.Errorf("non-JSON payload")
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

// Inject fields:
// - Add Raw
// - Add Env
// - Add Clever Container fields (container_{env,app,task})
