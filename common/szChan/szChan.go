// szChan implements a size-capped channel
package szChan

import (
	// "fmt"
	"sync"
	"sync/atomic"
)

// SzChan holds the context for the size-capped channel
type SzChan struct {
	avail    int64        // available capacity in the internal buffer
	capacity int64        // total size/capacity of internal buffer
	writerCh chan Message // channel the writer end puts messages into
	bufferCh chan Message // internal channel that holds the buffered messages
	readerCh chan Message // channel the reader end reads messages from
	cond     *sync.Cond   // conditional variable used to sleep/wake-up
	mu       sync.Mutex   // mutex to use for the conditional-variable
}

const (
	// should be '0' ideally, so the writer blocks as soon as the internal buffer
	// is full (ie, has no capacity); increase to improve performance.
	writerChSize = 32
	readerChSize = 32
	bufferChSize = 10000
)

// Message is the interface that every object passed via the channel should implement
type Message interface {
	Len() int
}

// New creates a new size-capped channel, and returns corresponding
// channels for the reader and writer for the channel.
func New(size int, setOpts ...SetOptFunc) (*SzChan, <-chan Message, chan<- Message) {

	t := &SzChan{
		avail:    int64(size),
		capacity: int64(size),
		readerCh: make(chan Message, readerChSize),
		writerCh: make(chan Message, writerChSize),
		bufferCh: make(chan Message, bufferChSize),
	}

	t.cond = sync.NewCond(&t.mu)

	for _, setOpt := range setOpts {
		setOpt(t)
	}

	go t.readPump()
	go t.writePump()

	return t, t.readerCh, t.writerCh
}

type SetOptFunc func(t *SzChan)

// ReaderChSize can be used to override the default readerChSize
func ReaderChSize(size int) SetOptFunc {

	return func(t *SzChan) {
		close(t.readerCh)
		t.readerCh = make(chan Message, size)
	}
}

// WriterChSize can be used to override the default writerChSize
func WriterChSize(size int) SetOptFunc {

	return func(t *SzChan) {
		close(t.writerCh)
		t.writerCh = make(chan Message, size)
	}
}

// BufferChSize can be used to override the default bufferChSize
func BufferChSize(size int) SetOptFunc {

	return func(t *SzChan) {
		close(t.bufferCh)
		t.bufferCh = make(chan Message, size)
	}
}

// Len returns the size of the messages currently buffered in the SzChan
func (t *SzChan) Len() int {

	return int(atomic.LoadInt64(&t.capacity) - atomic.LoadInt64(&t.avail))
}

// Cap returns the current capacity of the SzChan
func (t *SzChan) Cap() int {

	return int(atomic.LoadInt64(&t.capacity))
}

// SetCap changes the 'capacity' of the channel
func (t *SzChan) SetCap(newCap int) {

	oldCap := atomic.SwapInt64(&t.capacity, int64(newCap)) // set new 'capacity'
	atomic.AddInt64(&t.avail, int64(newCap)-oldCap)        // update 'avail' appropriately
}

// Close closes the sized channel. Just like with go-channels, the caller would
// be able to read and drain out any existing messages buffered in the channel.
func (t *SzChan) Close() {

	close(t.writerCh)
}

// read pump reads from the 'writer' channel and writes out into the internal
// 'buffer' channel if the message size fits.
func (t *SzChan) readPump() {

	// when done, close the buffer channel
	defer close(t.bufferCh)

	for msg := range t.writerCh {

		size := int64(msg.Len()) // query size of message

		// wait until there's enough space in the buffer to hold this message
		for {
			avail := atomic.LoadInt64(&t.avail)

			// if the msg does not readily fit into the available space,
			// then sleep and wait until space is made available (when
			// messages are read out).
			if size > avail {

				t.cond.L.Lock()

				// sleep until there's space available in the buffer
				for size > avail {
					t.cond.Wait() // sleep on cond-var
					avail = atomic.LoadInt64(&t.avail)
				}

				t.cond.L.Unlock()
			}

			// msg-size fits into available-size; atomically update available size
			if atomic.CompareAndSwapInt64(&t.avail, avail, avail-size) {
				break
			}
		}

		// we should ideally never block below! if we do block, that
		// indicates that there are lots of tiny msgs being sent and
		// we should increase the size of 'bufferCh'.
		t.bufferCh <- msg
	}
}

// write pump reads from the internal 'buffer' channel and writes out into the
// 'reader' channel. every time a message is drained out, we updated the 'avail'
// appropriately and send a signal over the conditional-variable to wake up the
// publisher, in case it is stuck.
func (t *SzChan) writePump() {

	// when done, close the 'reader' channel; for the reader this would
	// make it seem to behave like a go-channel would, where it can drain
	// out until the end.
	defer close(t.readerCh)

	for msg := range t.bufferCh {

		t.readerCh <- msg
		atomic.AddInt64(&t.avail, int64(msg.Len()))
		t.cond.Signal()
	}
}
