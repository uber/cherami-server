// sizedCh implements a size-capped channel
package sizedCh

import (
	"sync"
	"sync/atomic"
)

// SizedCh holds the context for the size-capped channel
type SizedCh struct {
	availSize int64        // available size in the channel; updated atomically
	writerCh  chan Message // channel the writer end puts messages into
	bufferCh  chan Message // internal channel that holds the buffered messages
	readerCh  chan Message // channel the reader end reads messages from
	cond      *sync.Cond   // conditional variable used to sleep/wake-up
	mu        sync.Mutex   // mutex to use for the conditional-variable
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

type SetOpt func(t *SizedCh)

func ReaderChSize(size int) SetOpt {

	return func(t *SizedCh) {
		close(t.readerCh)
		t.readerCh = make(chan Message, size)
	}
}

func WriterChSize(size int) SetOpt {

	return func(t *SizedCh) {
		close(t.writerCh)
		t.writerCh = make(chan Message, size)
	}
}

func BufferChSize(size int) SetOpt {

	return func(t *SizedCh) {
		close(t.bufferCh)
		t.bufferCh = make(chan Message, size)
	}
}

// New creates a new size-capped channel, and returns corresponding
// channels for the reader and writer for the channel.
func New(size int, setOpts ...SetOpt) (*SizedCh, <-chan Message, chan<- Message) {

	t := &SizedCh{
		availSize: int64(size),
		readerCh:  make(chan Message, readerChSize),
		writerCh:  make(chan Message, writerChSize),
		bufferCh:  make(chan Message, bufferChSize),
	}

	t.cond = sync.NewCond(&t.mu)

	for _, setOpt := range setOpts {
		setOpt(t)
	}

	go t.readPump()
	go t.writePump()

	return t, t.readerCh, t.writerCh
}

// Increase increases the 'size' of the channel; the size can be
// negative to reduce the size. Returns the new size of the channel.
func (t *SizedCh) Increase(size int) (newSize int) {

	return int(atomic.AddInt64(&t.availSize, int64(size)))
}

// Close closes the sized channel. Just like with go-channels, the caller would
// be able to read and drain out any existing messages buffered in the channel.
func (t *SizedCh) Close() {

	close(t.writerCh)
}

// read pump reads from the 'writer' channel and writes out into the internal
// 'buffer' channel if the message size fits.
func (t *SizedCh) readPump() {

	// when done, close the buffer channel
	defer close(t.bufferCh)

	for msg := range t.writerCh {

		// find message size
		msgSize := int64(msg.Len())

		// check if there's space in the buffer; wait if not
		for {
			availSize := atomic.LoadInt64(&t.availSize)

			// if the msg does not readily fit into the available space,
			// then sleep and wait until space is made available (when
			// messages are read out).
			if msgSize > availSize {

				t.cond.L.Lock()

				// sleep until there's space available in the buffer
				for msgSize > availSize {

					t.cond.Wait() // sleep on conditional-variable
					availSize = atomic.LoadInt64(&t.availSize)
				}

				t.cond.L.Unlock()
			}

			// msg-size fits into available-size; atomically update available size
			if atomic.CompareAndSwapInt64(&t.availSize, availSize, availSize-msgSize) {
				break
			}
		}

		// we should ideally never block below! if we do block, that
		// indicates that there are lots of tiny msgs being sent and
		// we should probably increase the size of 'bufferCh'.
		t.bufferCh <- msg
	}
}

// write pump reads from the internal 'buffer' channel and writes out into the
// 'reader' channel. every time a message is drained out, we updated the
// 'availSize' appropriately and send a signal over the conditional-variable
// to wake up the publisher, in case it is stuck.
func (t *SizedCh) writePump() {

	// when done, close the 'reader' channel; for the reader this would
	// make it seem to behave like a go-channel would, where it can drain
	// out until the end.
	defer close(t.readerCh)

	for msg := range t.bufferCh {

		t.readerCh <- msg
		atomic.AddInt64(&t.availSize, int64(msg.Len()))
		t.cond.Signal()
	}
}
