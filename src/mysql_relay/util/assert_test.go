package util

import (
	"fmt"
	"testing"
	"time"
)

func ta01() (err error) {
	defer RecoverToError(&err)
	Assert0(func() error {
		return fmt.Errorf("xxx1")
	}())
	return nil
}

func ta02() (err error) {
	var _err error
	go func() {
		defer func() {}()
		defer RecoverToError(&_err)
		Assert0(func() error {
			return fmt.Errorf("xxx2")
		}())
	}()
	time.Sleep(1)
	return _err
}

func TestAssert0(t *testing.T) {
	err := ta01()
	if err.Error() != "xxx1" {
		t.Fail()
	}
	err = ta02()
	if err.Error() != "xxx2" {
		t.Fail()
	}
}
