package batcher

import (
	"fmt"
	"math/big"
	"sync"
	"time"
)

// Sync is used to allow a writer to syncronize with the batcher.
// The writer declares how to write messages (via its `SendBatch` method), while the batcher
// keeps track of messages written
type Sync interface {
	SendBatch(batch [][]byte, largestSeq *big.Int, largestSubSeq int)
}

// Batcher interface
type Batcher interface {
	// AddMesage to the batch
	AddMessage(msg []byte, sequenceNumber string, subSequenceNumber int) error
	// Flush all messages from the batch
	Flush()
	// SmallestSeqPair returns the smallest SequenceNumber and SubSequence number in
	// the current batch
	SmallestSequencePair() (*big.Int, int)
}

type msgPack struct {
	msg       []byte
	seqNumBig *big.Int
	subSeqNum int
}

type batcher struct {
	mux sync.Mutex

	flushInterval time.Duration
	flushCount    int
	flushSize     int

	// largestSeq and largestSubSeq are used to track the highest sequence number
	// of any record in the batch. This is used for checkpointing.
	largestSeq    *big.Int
	largestSubSeq int

	// smallestSeq and smallestSubSeq are used to track the highest sequence number
	// of any record in the batch. This is used for checkpointing.
	smallestSeq    *big.Int
	smallestSubSeq int

	sync      Sync
	msgChan   chan<- msgPack
	flushChan chan<- struct{}
}

// New creates a new Batcher
// - sync - synchronizes batcher with writer
// - flushInterval - how often accumulated messages should be flushed (default 1 second).
// - flushCount - number of messages that trigger a flush (default 10).
// - flushSize - size of batch that triggers a flush (default 1024 * 1024 = 1 mb)
func New(sync Sync, flushInterval time.Duration, flushCount int, flushSize int) Batcher {
	msgChan := make(chan msgPack, 100)
	flushChan := make(chan struct{})

	b := &batcher{
		flushCount:    flushCount,
		flushInterval: flushInterval,
		flushSize:     flushSize,
		sync:          sync,
		msgChan:       msgChan,
		flushChan:     flushChan,
	}

	go b.startBatcher(msgChan, flushChan)

	return b
}

func (b *batcher) SmallestSequencePair() (*big.Int, int) {
	b.mux.Lock()
	defer b.mux.Unlock()

	return b.smallestSeq, b.smallestSubSeq
}

func (b *batcher) LargestSequencePair() (*big.Int, int) {
	b.mux.Lock()
	defer b.mux.Unlock()

	return b.largestSeq, b.largestSubSeq
}

func (b *batcher) SetFlushInterval(dur time.Duration) {
	b.flushInterval = dur
}

func (b *batcher) SetFlushCount(count int) {
	b.flushCount = count
}

func (b *batcher) SetFlushSize(size int) {
	b.flushSize = size
}

func (b *batcher) AddMessage(msg []byte, sequenceNumber string, subSequenceNumber int) error {
	if len(msg) <= 0 {
		return fmt.Errorf("Empty messages can't be sent")
	}

	seqNumBig := new(big.Int)
	if _, ok := seqNumBig.SetString(sequenceNumber, 10); !ok { // Validating sequenceNumber
		return fmt.Errorf("could not parse sequence number '%s'", sequenceNumber)
	}

	b.msgChan <- msgPack{msg, seqNumBig, subSequenceNumber}
	return nil
}

// updateSequenceNumbers is used to track the highest sequenceNumber of any record in the batch.
// When flush() is called, the batcher sends the sequence number to the writer. When the writer
// checkpoints, it does so up to the latest message that was flushed successfully.
func (b *batcher) updateSequenceNumbers(seqNumBig *big.Int, subSeqNum int) {
	b.mux.Lock()
	defer b.mux.Unlock()

	isLarger := b.largestSeq == nil ||
		seqNumBig.Cmp(b.largestSeq) == 1 ||
		(seqNumBig.Cmp(b.largestSeq) == 0 && subSeqNum > b.largestSubSeq)
	if isLarger {
		b.largestSeq = seqNumBig
		b.largestSubSeq = subSeqNum
	}

	isSmaller := b.smallestSeq == nil ||
		seqNumBig.Cmp(b.smallestSeq) == -1 ||
		(seqNumBig.Cmp(b.smallestSeq) == 0 && subSeqNum < b.smallestSubSeq)
	if isSmaller {
		b.smallestSeq = seqNumBig
		b.smallestSubSeq = subSeqNum
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
		b.smallestSeq = nil
	}
	return [][]byte{}
}

func (b *batcher) startBatcher(msgChan <-chan msgPack, flushChan <-chan struct{}) {
	batch := [][]byte{}

	for {
		select {
		case <-time.After(b.flushInterval):
			batch = b.flush(batch)
		case <-flushChan:
			batch = b.flush(batch)
		case pack := <-msgChan:
			size := b.batchSize(batch)
			if b.flushSize < size+len(pack.msg) {
				batch = b.flush(batch)
			}

			batch = append(batch, pack.msg)
			b.updateSequenceNumbers(pack.seqNumBig, pack.subSeqNum)

			if b.flushCount <= len(batch) || b.flushSize <= b.batchSize(batch) {
				batch = b.flush(batch)
			}
		}
	}
}
