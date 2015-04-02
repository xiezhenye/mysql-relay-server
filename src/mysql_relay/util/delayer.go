package util

import (
	"time"
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
