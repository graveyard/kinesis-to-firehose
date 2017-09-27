package main

import (
	"log"
	"os"
	"strconv"
	"time"

	kbc "github.com/Clever/amazon-kinesis-client-go/batchconsumer"

	"github.com/Clever/kinesis-to-firehose/sender"
)

// getEnv looks up an environment variable given and exits if it does not exist.
func getEnv(envVar string) string {
	val := os.Getenv(envVar)
	if val == "" {
		log.Fatalf("Must specify env variable %s", envVar)
	}
	return val
}

func getEnvInt(envVar string) int {
	str := getEnv(envVar)
	num, err := strconv.Atoi(str)
	if err != nil {
		log.Fatalf("Env variable %s must be an int instead of '%s'", envVar, str)
	}

	return num
}

func main() {
	suffix := "." + time.Now().Format("2006-01-02T15:04:05") + ".log"
	kbcConfig := kbc.Config{
		BatchInterval: 10 * time.Second,
		BatchCount:    500,
		BatchSize:     4 * 1024 * 1024, // 4Mb
		LogFile:       getEnv("LOG_FILE") + suffix,
		ReadRateLimit: getEnvInt("READ_RATE_LIMIT"),
	}

	firehoseConfig := sender.FirehoseSenderConfig{
		DeployEnv:       getEnv("_DEPLOY_ENV"),
		FirehoseRegion:  getEnv("FIREHOSE_AWS_REGION"),
		StreamName:      getEnv("FIREHOSE_STREAM_NAME"),
		IsElasticsearch: (os.Getenv("IS_ELASTICSEARCH_CONSUMER") == "true"),
	}

	sender := sender.NewFirehoseSender(firehoseConfig)
	consumer := kbc.NewBatchConsumer(kbcConfig, sender)
	consumer.Start()
}
