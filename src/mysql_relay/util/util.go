package util

import (
	//"bufio"
	"fmt"
	//"io"
	"os"
	"time"
	//"unsafe"
)

type AutoDelayer struct {
	t time.Duration
}

var autoDelayMax = 1 * time.Second
var autoDelayMin = 5 * time.Millisecond

func (self *AutoDelayer) Delay() {
	if self.t == 0 {
		self.t = autoDelayMin
	} else {
		self.t *= 2
	}
	if self.t > autoDelayMax {
		self.t = autoDelayMax
	}
	time.Sleep(self.t)
}

func (self *AutoDelayer) Reset() {
	self.t = 0
}

type Joinable func() error
type Barrier []Joinable
type JoinError []error

func (self JoinError) Error() string {
	s := ""
	for _, e := range self {
		if e != nil {
			s += e.Error()
		}
	}
	return s
}

func (self Barrier) Run() error {
	errChan := make(chan struct {
		int
		error
	}, len(self))
	ret := make([]error, len(self))
	for i, f := range self {
		go func(f Joinable) {
			errChan <- struct {
				int
				error
			}{i, f()}
		}(f)
	}
	hasError := false
	for i := 0; i < len(self); i++ {
		e := <-errChan
		if e.error != nil {
			ret[e.int] = e.error
			hasError = true
		}
	}
	close(errChan)
	if hasError {
		return JoinError(ret)
	}
	return nil
}

type NullAbleString struct {
	str    string
	isNull bool
}

type Logger struct {
	file     *os.File
	toStderr bool
	prefix   string
}

func (self *Logger) ToFile(file string) (err error) {
	self.file, err = os.OpenFile(file, os.O_APPEND|os.O_RDWR, 0644)
	return
}

func (self *Logger) SetToStderr(b bool) {
	self.toStderr = b
}

func (self *Logger) SetPrefix(prefix string) {
	self.prefix = prefix
}

func (self *Logger) Log(level string, messageFormat string, messageArgs ...interface{}) {
	now := time.Now().Format(time.RFC3339)
	formatted := []byte(fmt.Sprintf("%s\t%s\t%s%s\n", now, level, self.prefix,
		fmt.Sprintf(messageFormat, messageArgs...)))
	if self.toStderr {
		os.Stderr.Write(formatted)
	}
	if self.file == nil {
		return
	}
	self.file.Write(formatted)
	self.file.Sync()
}

func (self *Logger) Info(messageFormat string, messageArgs ...interface{}) {
	self.Log("INFO", messageFormat, messageArgs...)
}

func (self *Logger) Warn(messageFormat string, messageArgs ...interface{}) {
	self.Log("WARN", messageFormat, messageArgs...)
}

func (self *Logger) Fatal(messageFormat string, messageArgs ...interface{}) {
	self.Log("FATAL", messageFormat, messageArgs...)
}

func (self *Logger) Error(messageFormat string, messageArgs ...interface{}) {
	self.Log("ERR", messageFormat, messageArgs...)
}

// func BufReader(reader io.Reader, buf []byte) bufio.Reader {
// 	var ret bufio.Reader
// 	p := (*[]byte)(unsafe.Pointer(&ret))
// 	*p = buf
// 	ret.Reset(reader)
// 	return ret
// }
//
// func BufWriter(writer io.Writer, buf []byte) bufio.Writer {
// 	var ret bufio.Writer
// 	p := (*[]byte)(unsafe.Pointer(&ret))
// 	*p = buf
// 	ret.Reset(writer)
// 	return ret
// }
