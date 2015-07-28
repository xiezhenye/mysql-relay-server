package util

type NullAbleString struct {
	str    string
	isNull bool
}

func RecoverToError(err *error) {
	r := recover()
	if r != nil {
		*err = r.(error)
	}
}

func Assert0(err error) {
	if err != nil {
		panic(err)
	}
}

func Assert1(a interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return a
}

func Assert2(a1, a2 interface{}, err error) (interface{}, interface{}) {
	if err != nil {
		panic(err)
	}
	return a1, a2
}

func Assert3(a1, a2, a3 interface{}, err error) (interface{}, interface{}, interface{}) {
	if err != nil {
		panic(err)
	}
	return a1, a2, a3
}

func Assert4(a1, a2, a3, a4 interface{}, err error) (interface{}, interface{}, interface{}, interface{}) {
	if err != nil {
		panic(err)
	}
	return a1, a2, a3, a4
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
