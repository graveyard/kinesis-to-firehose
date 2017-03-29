package batcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type batch [][]byte

type mockSync struct {
	flushChan chan struct{}
	batches   []batch
}

func NewMockSync() *mockSync {
	return &mockSync{
		flushChan: make(chan struct{}, 1),
		batches:   []batch{},
	}
}

func (m *mockSync) Flush(b [][]byte) {
	m.batches = append(m.batches, batch(b))
	m.flushChan <- struct{}{}
}

func (m *mockSync) waitForFlush(timeout time.Duration) error {
	select {
	case <-m.flushChan:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("The flush never came.  I waited %s.", timeout.String())
	}
}

func TestBatchingByCount(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)
	batcher.FlushInterval(time.Hour)
	batcher.FlushCount(2)

	t.Log("Batcher respect count limit")
	assert.NoError(batcher.Send([]byte("hihi")))
	assert.NoError(batcher.Send([]byte("heyhey")))
	assert.NoError(batcher.Send([]byte("hmmhmm"))) // Shouldn't be in first batch

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(2, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
	assert.Equal("heyhey", string(sync.batches[0][1]))

	t.Log("Batcher doesn't send partial batches")
	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestBatchingByTime(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)
	batcher.FlushInterval(time.Millisecond)
	batcher.FlushCount(2000000)

	t.Log("Batcher sends partial batches when time expires")
	assert.NoError(batcher.Send([]byte("hihi")))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))

	t.Log("Batcher sends all messsages in partial batches when time expires")
	assert.NoError(batcher.Send([]byte("heyhey")))
	assert.NoError(batcher.Send([]byte("yoyo")))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(2, len(sync.batches))
	assert.Equal(2, len(sync.batches[1]))
	assert.Equal("heyhey", string(sync.batches[1][0]))
	assert.Equal("yoyo", string(sync.batches[1][1]))

	t.Log("Batcher doesn't send empty batches")
	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestBatchingBySize(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)
	batcher.FlushInterval(time.Hour)
	batcher.FlushCount(2000000)
	batcher.FlushSize(8)

	t.Log("Large messages are sent immediately")
	assert.NoError(batcher.Send([]byte("hellohello")))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hellohello", string(sync.batches[0][0]))

	t.Log("Batcher tries not to exceed size limit")
	assert.NoError(batcher.Send([]byte("heyhey")))
	assert.NoError(batcher.Send([]byte("hihi")))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(2, len(sync.batches))
	assert.Equal(1, len(sync.batches[1]))
	assert.Equal("heyhey", string(sync.batches[1][0]))

	t.Log("Batcher sends messages that didn't fit in previous batch")
	assert.NoError(batcher.Send([]byte("yoyo"))) // At this point "hihi" is in the batch

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(3, len(sync.batches))
	assert.Equal(2, len(sync.batches[2]))
	assert.Equal("hihi", string(sync.batches[2][0]))
	assert.Equal("yoyo", string(sync.batches[2][1]))

	t.Log("Batcher doesn't send partial batches")
	assert.NoError(batcher.Send([]byte("okok")))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestFlushing(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)
	batcher.FlushInterval(time.Hour)
	batcher.FlushCount(2000000)

	t.Log("Calling flush sends pending messages")
	assert.NoError(batcher.Send([]byte("hihi")))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)

	batcher.Flush()

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
}

func TestSendingEmpty(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)

	t.Log("An error is returned when an empty message is sent")
	err = batcher.Send([]byte{})
	assert.Error(err)
}
