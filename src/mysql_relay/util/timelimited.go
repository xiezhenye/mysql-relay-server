package util

import (
	"errors"
	"io"
	"time"
)

var ERR_TIMEOUT = errors.New("TIMEOUT")

type TimeLimitedReadWriter struct {
	readWriter io.ReadWriter
	timeout    time.Duration
}

type ioResult struct {
	n   int
	err error
}

type ioFunc func([]byte) (int, error)

func (self *TimeLimitedReadWriter) io(fn ioFunc, buf []byte) (n int, err error) {
	ch := make(chan ioResult, 1)
	go func() {
		n, err := fn(buf)
		ch <- ioResult{n, err}
	}()
	select {
	case res := <-ch:
		return res.n, res.err
	case <-time.After(self.timeout):
		return 0, ERR_TIMEOUT
	}
}

func (self *TimeLimitedReadWriter) Write(buf []byte) (n int, err error) {
	return self.io(self.readWriter.Write, buf)
}

func (self *TimeLimitedReadWriter) Read(buf []byte) (n int, err error) {
	return self.io(self.readWriter.Read, buf)
}

func NewTimeLimitedReadWriter(readWriter io.ReadWriter, timeout uint32) TimeLimitedReadWriter {
	ret := TimeLimitedReadWriter{readWriter: readWriter, timeout: time.Second * time.Duration(timeout)}
	return ret
}
