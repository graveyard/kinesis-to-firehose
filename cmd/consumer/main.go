package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"math/big"
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

type RecordProcessor struct {
	shardID           string
	sleepDuration     time.Duration
	checkpointRetries int
	checkpointFreq    time.Duration
	largestSeq        *big.Int
	largestSubSeq     int
	lastCheckpoint    time.Time
	firehoseWriter    *writer.FirehoseWriter
	rateLimiter       *rate.Limiter // Limits the number of records processed per second
}

func NewRecordProcessor(writer *writer.FirehoseWriter, limiter *rate.Limiter) *RecordProcessor {
	return &RecordProcessor{
		sleepDuration:     5 * time.Second,
		checkpointRetries: 5,
		checkpointFreq:    60 * time.Second,
		firehoseWriter:    writer,
		rateLimiter:       limiter,
	}
}

func (rp *RecordProcessor) Initialize(shardID string) error {
	rp.shardID = shardID
	rp.lastCheckpoint = time.Now()
	return nil
}

func (rp *RecordProcessor) checkpoint(checkpointer kcl.Checkpointer, sequenceNumber string, subSequenceNumber int) {
	for n := -1; n < rp.checkpointRetries; n++ {
		err := checkpointer.Checkpoint(sequenceNumber, subSequenceNumber)
		if err == nil {
			return
		}

		if cperr, ok := err.(kcl.CheckpointError); ok {
			switch cperr.Error() {
			case "ShutdownException":
				fmt.Fprintf(os.Stderr, "Encountered shutdown exception, skipping checkpoint\n")
				return
			case "ThrottlingException":
				fmt.Fprintf(os.Stderr, "Was throttled while checkpointing, will attempt again in %s", rp.sleepDuration)
			case "InvalidStateException":
				fmt.Fprintf(os.Stderr, "MultiLangDaemon reported an invalid state while checkpointing\n")
			default:
				fmt.Fprintf(os.Stderr, "Encountered an error while checkpointing: %s", err)
			}
		}

		if n == rp.checkpointRetries {
			fmt.Fprintf(os.Stderr, "Failed to checkpoint after %d attempts, giving up.\n", rp.checkpointRetries)
			return
		}

		time.Sleep(rp.sleepDuration)
	}
}

// shouldUpdateSequence determines whether a new larger sequence number is available
func (rp *RecordProcessor) shouldUpdateSequence(sequenceNumber *big.Int, subSequenceNumber int) bool {
	return rp.largestSeq == nil || sequenceNumber.Cmp(rp.largestSeq) == 1 ||
		(sequenceNumber.Cmp(rp.largestSeq) == 0 && subSequenceNumber > rp.largestSubSeq)
}

func (rp *RecordProcessor) ProcessRecords(records []kcl.Record, checkpointer kcl.Checkpointer) error {
	for _, record := range records {
		// Wait until rate limiter permits one record to be processed
		rp.rateLimiter.Wait(context.Background())

		// Base64 decode the record
		data, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			return err
		}
		msg := string(data)

		// Write the message to firehose
		err = rp.firehoseWriter.ProcessMessage(msg)
		if err != nil {
			return err
		}

		// Handle checkpointing
		seqNumber := new(big.Int)
		if _, ok := seqNumber.SetString(record.SequenceNumber, 10); !ok {
			fmt.Fprintf(os.Stderr, "could not parse sequence number '%s'\n", record.SequenceNumber)
			continue
		}
		if rp.shouldUpdateSequence(seqNumber, record.SubSequenceNumber) {
			rp.largestSeq = seqNumber
			rp.largestSubSeq = record.SubSequenceNumber
		}
	}
	if time.Now().Sub(rp.lastCheckpoint) > rp.checkpointFreq {
		rp.checkpoint(checkpointer, rp.largestSeq.String(), rp.largestSubSeq)
		rp.lastCheckpoint = time.Now()

		// Write status to file
		err := appendToFile(logFile, fmt.Sprintf("%s -- %s\n", rp.shardID, rp.firehoseWriter.Status()))
		if err != nil {
			return err
		}
	}
	return nil
}

func appendToFile(filename, text string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.WriteString(text); err != nil {
		return err
	}

	return nil
}

func (rp *RecordProcessor) Shutdown(checkpointer kcl.Checkpointer, reason string) error {
	fmt.Fprintf(os.Stderr, "Shutting down. Reason: %s. Attempting to flush messages and checkpoint.\n", reason)
	rp.firehoseWriter.FlushAll()
	time.Sleep(5 * time.Second) // Wait for messages to be flushed
	rp.checkpoint(checkpointer, rp.largestSeq.String(), rp.largestSubSeq)
	return nil
}

var logFile = "/tmp/kcl_stderr"

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

	rp := NewRecordProcessor(writer, rate.NewLimiter(rateLimit, burstLimit))
	kclProcess := kcl.New(os.Stdin, os.Stdout, os.Stderr, rp)
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
