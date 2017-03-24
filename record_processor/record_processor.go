package record_processor

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
	"github.com/Clever/kinesis-to-firehose/writer"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
)

type RecordProcessor struct {
	LogFile        string
	RateLimiter    *rate.Limiter // Limits the number of records processed per second
	FirehoseWriter *writer.FirehoseWriter

	shardID           string
	sleepDuration     time.Duration
	checkpointRetries int
	checkpointFreq    time.Duration
	largestSeq        *big.Int
	largestSubSeq     int
	lastCheckpoint    time.Time
}

func New() *RecordProcessor {
	return &RecordProcessor{
		sleepDuration:     5 * time.Second,
		checkpointRetries: 5,
		checkpointFreq:    60 * time.Second,
	}
}

func (rp *RecordProcessor) Initialize(shardID string) error {
	rp.shardID = shardID
	rp.lastCheckpoint = time.Now()
	return nil
}

func (rp *RecordProcessor) checkpoint(checkpointer kcl.Checkpointer, sequenceNumber string, subSequenceNumber int) {
	for n := 0; n < rp.checkpointRetries; n++ {
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

		if n == rp.checkpointRetries-1 {
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
		rp.RateLimiter.Wait(context.Background())

		// Base64 decode the record
		data, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			return err
		}
		msg := string(data)

		// Write the message to firehose
		err = rp.FirehoseWriter.ProcessMessage(msg)
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
		err := appendToFile(rp.LogFile, fmt.Sprintf("%s -- %s\n", rp.shardID, rp.FirehoseWriter.Status()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (rp *RecordProcessor) Shutdown(checkpointer kcl.Checkpointer, reason string) error {
	if reason == "TERMINATE" {
		fmt.Fprintf(os.Stderr, "Was told to terminate, will attempt to checkpoint.\n")
		rp.FirehoseWriter.FlushAll()
		rp.checkpoint(checkpointer, "", 0)
	} else {
		fmt.Fprintf(os.Stderr, "Shutting down due to failover. Reason: %s. Will not checkpoint.\n", reason)
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
