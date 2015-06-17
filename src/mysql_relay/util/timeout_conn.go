package util

import (
	"net"
	"sync"
	"time"
)

type TimeoutConn struct {
	net.Conn
	timeout time.Duration
	lock    sync.Mutex
}

func (self *TimeoutConn) io(fn ioFunc, buf []byte) (n int, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	err = self.Conn.SetDeadline(time.Now().Add(self.timeout))
	if err != nil {
		return
	}
	return fn(buf)
}

func (self *TimeoutConn) Write(buf []byte) (n int, err error) {
	return self.io(self.Conn.Write, buf)
}

func (self *TimeoutConn) Read(buf []byte) (n int, err error) {
	return self.io(self.Conn.Read, buf)
}

func NewTimeoutConn(conn net.Conn, timeout uint32) *TimeoutConn {
	ret := &TimeoutConn{
		Conn:    conn,
		timeout: time.Second * time.Duration(timeout),
	}
	return ret
}
