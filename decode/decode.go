package decode

import (
	"encoding/json"
	"fmt"
	"strings"
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

// FieldsFromRSyslog takes a log line and extracts fields from it
func FieldsFromRsyslog(line string) map[string]interface{} {
	// TODO
	// template := `%TIMESTAMP:::date-rfc3339% %HOSTNAME% %syslogtag%%msg:::sp-if-no-1st-sp%%msg:::drop-last-lf%\n`
	// tz := "UTC"
	return map[string]interface{}{}
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

func DecodeHaproxy(map[string]interface{}) map[string]interface{} {
	// TODO
	// filename = "lua_decoders/nginx_access.lua"
	// output_limit = 200000
	// # Note that the field $tcpinfo_rtt is actually server response time.  We are using the name $tcpinfo_rtt
	// # so that the nginx decoder will interpret it as an integer.  We should fix this.
	// [HaproxyDecoder.config]
	// type = "haproxy"
	// log_format = '$remote_addr - - [$time_local] "$request_method $request $server_protocol" $status $body_bytes_sent "" "" $client_port $accept_date_ms "$frontend_name" "$backend_name" "$srv_name" $client_time $wait_time $connection_time $tcpinfo_rtt $total_time $termination_state $active_connections $frontend_concurrent_connections $backend_concurrent_connections $server_concurrent_connections $retries $server_queue $backend_queue "$captured_request_cookie" "$captured_response_cookie" "$x_forwarded_for" "$http_user_agent" "$authorization"'
	return map[string]interface{}{}
}
