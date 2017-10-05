package sizedCh

import (
	"sync"
	"sync/atomic"
)

type SizedCh struct {
	writerCh  chan Message
	bufferCh  chan Message
	readerCh  chan Message
	totalSize int64
	availSize int64
	mu        sync.Mutex
	cond      *sync.Cond
}

const (
	readerChSize = 16
	writerChSize = 16
	bufferChSize = 10000
)

type Message interface {
	Size() int
}

func New(size int) (*SizedCh, <-chan Message, chan<- Message) {

	t := &SizedCh{
		readerCh: make(chan Message, readerChSize),
		writerCh: make(chan Message, writerChSize),
		bufferCh: make(chan Message, bufferChSize),
	}

	t.cond = sync.NewCond(&t.mu)

	go t.readPump()
	go t.writePump()

	return t, t.readerCh, t.writerCh
}

func (t *SizedCh) Increase(size int) (newSize int) {

	return int(atomic.AddInt64(&t.totalSize, int64(size)))
}

func (t *SizedCh) Close() {

	close(t.writerCh)
}

func (t *SizedCh) readPump() {

	defer close(t.bufferCh)

	for msg := range t.writerCh {

		msgSize := int64(msg.Size())

		for {
			availSize := atomic.LoadInt64(&t.availSize)

			if msgSize > availSize {

				// if msg-size larger than available-size, then sleep

				t.cond.L.Lock()

				for msgSize > availSize {
					t.cond.Wait()
					availSize = atomic.LoadInt64(&t.availSize)
				}

				t.cond.L.Unlock()
			}

			// msg-size fits into available-size; atomically c-a-s availSize
			if atomic.CompareAndSwapInt64(&t.availSize, availSize, availSize-msgSize) {
				break
			}
		}

		t.bufferCh <- msg // we should never block here! blocking here indicates bufferCh needs to be larger
	}
}

func (t *SizedCh) writePump() {

	defer close(t.readerCh)

	for msg := range t.bufferCh {

		t.readerCh <- msg
		atomic.AddInt64(&t.availSize, int64(msg.Size()))
		t.cond.Signal()
	}
}
