package util

import (
	"fmt"
)

type BufferConsumer interface {
	OnBuffer(buffer []byte) error
	OnMeta(meta interface{}) error
}

type BufferProducer interface {
	OnBuffer(buffer []byte, metaSender MetaSender) error
}

type BufferChan struct {
	chanProducer chan []byte
	chanConsumer chan interface{}
	producer     BufferProducer
	consumer     BufferConsumer
	buffer       []byte
}

func NewBufferChan(producer BufferProducer, consumer BufferConsumer, bufferSize int, chanSize int) BufferChan {
	var ret BufferChan
	ret.chanProducer = make(chan []byte, chanSize)
	ret.chanConsumer = make(chan interface{}, chanSize)
	ret.buffer = make([]byte, bufferSize)
	ret.producer = producer
	ret.consumer = consumer
	sliceSize := bufferSize / chanSize

	for i := 0; i < chanSize; i++ {
		ret.chanProducer <- ret.buffer[i*sliceSize : (i+1)*sliceSize]
	}
	return ret
}

type MetaSender interface {
	SendMeta(meta interface{})
}

func (self *BufferChan) SendMeta(meta interface{}) {
	switch meta.(type) {
	case []byte:
		panic("meta must not be []byte")
	case nil:
		panic("meta must not be nil")
	}
	self.chanConsumer <- meta

}

func (self *BufferChan) Close() {
	close(self.chanProducer)
	close(self.chanConsumer)
}

func (self *BufferChan) runProducer() error {
	defer close(self.chanProducer)
	for {
		err := self.ProducerDo(func(buf []byte) error {
			return self.producer.OnBuffer(buf, self)
		})
		if err == ErrClosed {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

var ErrClosed = fmt.Errorf("chan closed")

func (self *BufferChan) ProducerDo(f func([]byte) error) error {
	buf, ok := <-self.chanProducer
	if !ok {
		return ErrClosed
	}
	err := f(buf)
	if err != nil {
		return err
	}
	self.chanConsumer <- buf
	return nil
}

func (self *BufferChan) runConsumer() error {
	defer close(self.chanConsumer)
	for e := range self.chanConsumer {
		switch e.(type) {
		case []byte:
			err := self.consumer.OnBuffer(e.([]byte))
			if err != nil {
				return err
			}
			self.chanProducer <- e.([]byte)
		default:
			err := self.consumer.OnMeta(e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
