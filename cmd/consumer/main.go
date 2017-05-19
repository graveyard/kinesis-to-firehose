package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"golang.org/x/time/rate"

	"github.com/Clever/kinesis-to-firehose/writer"
)

var stdErrLogger = log.New(os.Stderr, "", log.Ldate|log.Ltime)

func main() {
	// Create a log file and make the standard logger write to it (across all packages)
	logFile := getEnv("LOG_FILE")

	f, err := os.Create(logFile)
	if err != nil {
		stdErrLogger.Fatalf("Unable to create log file: %s", err.Error())
	}
	defer f.Close()
	log.SetOutput(f)

	stringifyNested := false
	if os.Getenv("STRINGIFY_NESTED") == "1" {
		stringifyNested = true
	}

	renameESReservedFields := false
	if os.Getenv("RENAME_ES_RESERVED_FIELDS") == "1" {
		renameESReservedFields = true
	}

	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion(getEnv("FIREHOSE_AWS_REGION")).WithMaxRetries(4)))
	minimumTimestamp, err := strconv.Atoi(getEnv("MINIMUM_TIMESTAMP"))
	if err != nil {
		stdErrLogger.Fatalf("Invalid MINIMUM_TIMESTAMP: %s", err.Error())
	}
	config := writer.FirehoseWriterConfig{
		FirehoseClient:         firehose.New(sess),
		StreamName:             getEnv("FIREHOSE_STREAM_NAME"),
		FlushInterval:          10 * time.Second,
		FlushCount:             500,
		FlushSize:              4 * 1024 * 1024, // 4Mb
		LogFile:                logFile,
		DeployEnvironment:      getEnv("DEPLOY_ENV"),
		StringifyNested:        stringifyNested,
		RenameESReservedFields: renameESReservedFields,
		MinimumTimestamp:       time.Unix(int64(minimumTimestamp), 0),
	}

	// rateLimit is expressed in records-per-second
	// because a consumer is created for each shard, we can think of this as records-per-second-per-shard
	rl, err := strconv.ParseFloat(getEnv("RATE_LIMIT"), 64)
	if err != nil {
		stdErrLogger.Fatalf("Invalid RATE_LIMIT: %s", err.Error())
	}
	rateLimit := rate.Limit(rl)
	burstLimit := int(rl * 1.2)

	writer, err := writer.NewFirehoseWriter(config, rate.NewLimiter(rateLimit, burstLimit))
	if err != nil {
		stdErrLogger.Fatalf("Failed to create FirehoseWriter: %s", err.Error())
	}
	kclProcess := kcl.New(os.Stdin, os.Stdout, os.Stderr, writer)
	kclProcess.Run()
}

// getEnv looks up an environment variable given and exits if it does not exist.
func getEnv(envVar string) string {
	val := os.Getenv(envVar)
	if val == "" {
		stdErrLogger.Fatalf("Must specify env variable %s", envVar)
	}
	return val
}
