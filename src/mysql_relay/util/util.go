package util

import (
    "time"
//    "fmt"
)

type AutoDelayer struct {
    t   time.Duration
    
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

type Joinable func()error
type Barrier []Joinable
type JoinError []error

func (self JoinError) Error() string {
    s := ""
    for _, e := range self {
        if e != nil {
            s+= e.Error()
        }
    }
    return s
}

func (self Barrier) Run() error {
    errChan := make(chan struct{int;error},len(self))
    ret := make([]error, len(self))
    for i, f := range self {
        go func(f Joinable) {
            errChan<-struct{int; error}{i, f()}
        }(f)
    }
    hasError := false
    for i := 0; i < len(self); i++ {
        e := <- errChan
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

