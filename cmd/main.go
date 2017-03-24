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

	"github.com/Clever/kinesis-to-firehose/record_processor"
	"github.com/Clever/kinesis-to-firehose/writer"
)

func main() {
	logFile := getEnv("LOG_FILE")
	f, err := os.Create(logFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion(getEnv("FIREHOSE_AWS_REGION")).WithMaxRetries(4)))
	config := writer.FirehoseWriterConfig{
		FirehoseClient: firehose.New(sess),
		StreamName:     getEnv("FIREHOSE_STREAM_NAME"),
		FlushInterval:  10 * time.Second,
		FlushCount:     500,
		FlushSize:      4 * 1024 * 1024, // 4Mb
	}
	writer, err := writer.NewFirehoseWriter(config)
	if err != nil {
		log.Fatalf("Failed to create FirehoseWriter: %s", err.Error())
	}

	// rateLimit is expressed in records-per-second
	// because a consumer is created for each shard, we can think of this as records-per-second-per-shard
	rl, err := strconv.ParseFloat(getEnv("RATE_LIMIT"), 64)
	if err != nil {
		log.Fatalf("Invalid RATE_LIMIT: %s", err.Error())
	}
	rateLimit := rate.Limit(rl)
	burstLimit := int(rl * 1.2)

	kclProcess := kcl.New(os.Stdin, os.Stdout, os.Stderr, &record_processor.RecordProcessor{
		FirehoseWriter: writer,
		RateLimiter:    rate.NewLimiter(rateLimit, burstLimit),
		LogFile:        logFile,
	})
	kclProcess.Run()
}

// getEnv looks up an environment variable given and exits if it does not exist.
func getEnv(envVar string) string {
	val := os.Getenv(envVar)
	if val == "" {
		log.Fatalf("Must specify env variable %s", envVar)
	}
	return val
}
