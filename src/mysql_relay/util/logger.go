package util

import (
	"fmt"
	"os"
	"time"
)

type Logger struct {
	file     *os.File
	toStderr bool
	prefix   string
}

func (self *Logger) ToFile(file string) (err error) {
	self.file, err = os.OpenFile(file, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
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
