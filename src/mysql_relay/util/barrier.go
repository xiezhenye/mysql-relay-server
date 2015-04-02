package util

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
