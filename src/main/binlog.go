package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"mysql_relay/mysql"
	"os"
)

func main() {
	var err error
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	stat, err := f.Stat()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	err = dumpBinlog(f, 4, uint32(stat.Size()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
}

func dumpBinlog(file *os.File, from uint32, to uint32) (err error) {
	var buffer [8192]byte
	if from >= to {
		return
	}
	file.Seek(int64(from), 0)
	var n int64
	pos := from

	for {
		buffer[0] = '\x00'
		_, err = file.Read(buffer[1 : mysql.BinlogEventHeaderSize+1])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		var event mysql.BinlogEventPacket
		event.FromBuffer(buffer[:])
		event.PacketLength = event.EventSize + 1 //
		fmt.Printf("@%d %s\n", pos, event.String())
		//if event.LogPos != 0 && event.LogPos-event.EventSize != pos {
		//err = fmt.Errorf("bad pos: pos: %d, LogPos: %d, EventSize: %d", pos, event.LogPos, event.EventSize) //mysql.BuildErrPacket(mysql.ER_BINLOG_LOGGING_IMPOSSIBLE, "")
		//return
		//}
		err = event.Reset(false)
		if err != nil {
			return
		}

		reader := event.GetReader(file, buffer[:mysql.BinlogEventHeaderSize+1])
		reader.Read(buffer[0:1])

		dumper := hex.Dumper(os.Stdout)
		n, err = io.Copy(dumper, &reader)
		dumper.Close()

		fmt.Println()
		// TODO: validate checksum
		pos += uint32(n - 1)
		if err != nil {
			fmt.Println(err.Error())
		}
		if n == 0 {
			return
		}
		if pos >= to {
			return
		}

	}
	return
}
