package batcher

import (
	"fmt"
	"math/big"
	"os"
	"time"
)

type Sync interface {
	SendBatch(batch [][]byte, largestSeq *big.Int, largetsSubSeq int)
}

type Batcher interface {
	// Interval at which accumulated messages should be bulk put to
	// firehose (default 1 second).
	FlushInterval(dur time.Duration)
	// Number of messages that triggers a push to firehose
	// default to 10
	FlushCount(count int)
	// Size of batch that triggers a push to firehose
	// default to 1mb (1024 * 1024)
	FlushSize(size int)

	//AddMesage to the batch
	AddMessage(msg []byte, sequenceNumber string, subSequenceNumber int) error

	//Flush all messages from the batch
	Flush()
}

type batcher struct {
	flushInterval time.Duration
	flushCount    int
	flushSize     int

	largestSeq    *big.Int
	largestSubSeq int

	sync      Sync
	msgChan   chan<- []byte
	flushChan chan<- struct{}
}

func New(sync Sync) *batcher {
	msgChan := make(chan []byte, 100)
	flushChan := make(chan struct{})

	b := &batcher{
		flushCount:    10,
		flushInterval: time.Second,
		flushSize:     1024 * 1024,

		sync:      sync,
		msgChan:   msgChan,
		flushChan: flushChan,
	}

	go b.startBatcher(msgChan, flushChan)

	return b
}

func (b *batcher) FlushInterval(dur time.Duration) {
	b.flushInterval = dur
}

func (b *batcher) FlushCount(count int) {
	b.flushCount = count
}

func (b *batcher) FlushSize(size int) {
	b.flushSize = size
}

func (b *batcher) AddMessage(msg []byte, sequenceNumber string, subSequenceNumber int) error {
	if len(msg) <= 0 {
		return fmt.Errorf("Empty messages can't be sent")
	}

	b.msgChan <- msg
	b.updateSequenceNumber(sequenceNumber, subSequenceNumber)
	return nil
}

// updateSequenceNumber updates sequence number on the batch, if a larger one is available
func (b *batcher) updateSequenceNumber(sequenceNumber string, subSequenceNumber int) {
	seqNumBig := new(big.Int)
	if _, ok := seqNumBig.SetString(sequenceNumber, 10); !ok {
		fmt.Fprintf(os.Stderr, "could not parse sequence number '%s'\n", sequenceNumber)
		return
	}

	// Check if new seqNumBig is larger
	if b.largestSeq == nil || seqNumBig.Cmp(b.largestSeq) == 1 || (seqNumBig.Cmp(b.largestSeq) == 0 && subSequenceNumber > b.largestSubSeq) {
		b.largestSeq = seqNumBig
		b.largestSubSeq = subSequenceNumber
	}
}

func (b *batcher) Flush() {
	b.flushChan <- struct{}{}
}

func (b *batcher) batchSize(batch [][]byte) int {
	total := 0
	for _, msg := range batch {
		total += len(msg)
	}

	return total
}

func (b *batcher) flush(batch [][]byte) [][]byte {
	if len(batch) > 0 {
		b.sync.SendBatch(batch, b.largestSeq, b.largestSubSeq)
	}
	return [][]byte{}
}

func (b *batcher) startBatcher(msgChan <-chan []byte, flushChan <-chan struct{}) {
	batch := [][]byte{}

	for {
		select {
		case <-time.After(b.flushInterval):
			batch = b.flush(batch)
		case <-flushChan:
			batch = b.flush(batch)
		case msg := <-msgChan:
			size := b.batchSize(batch)
			if b.flushSize < size+len(msg) {
				batch = b.flush(batch)
			}

			batch = append(batch, msg)

			if b.flushCount <= len(batch) || b.flushSize <= b.batchSize(batch) {
				batch = b.flush(batch)
			}
		}
	}
}
