package util

import ()

type NullAbleString struct {
	str    string
	isNull bool
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
