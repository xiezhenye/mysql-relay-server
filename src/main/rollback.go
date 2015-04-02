package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"mysql_relay/mysql"
	"os"
)

func main() {
	var err error
	var binlogPath string
	var rollbackPos int64
	var outputPath string
	flag.StringVar(&binlogPath, "f", "", "binlog file path")
	flag.Int64Var(&rollbackPos, "p", 0, "rollback position")
	flag.StringVar(&outputPath, "o", "", "output file path")

	flag.Parse()

	f, err := os.Open(binlogPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	stat, err := f.Stat()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	var txs RowTransactionSet
	txs, err = scanTrans(f, uint32(rollbackPos), uint32(stat.Size()))
	if err != nil {
		if err != io.EOF {
			fmt.Fprintln(os.Stderr, err.Error())
			return
		}
	}
	fmt.Printf("txs:%v\n", txs)
}

func readEvent(file *os.File, buffer []byte) (event mysql.BinlogEventPacket, err error) {
	buffer[0] = '\x00'
	_, err = file.Read(buffer[1 : mysql.BinlogEventHeaderSize+1])
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	event.FromBuffer(buffer[:])
	event.PacketLength = event.EventSize + 1 //
	event.BodyLength = int(event.PacketLength - 1 - mysql.BinlogEventHeaderSize)
	err = event.Reset(true)
	return
}

func readFDE(file *os.File) (fde mysql.FormatDescriptionEvent, err error) {
	file.Seek(mysql.LOG_POS_START, 0)
	var buffer [128]byte
	buffer[0] = '\x00'
	_, err = file.Read(buffer[1 : mysql.BinlogEventHeaderSize+1])
	if err != nil {
		fmt.Printf("read fde failed: %s\n", err.Error())
		return
	}
	var event mysql.BinlogEventPacket
	event.FromBuffer(buffer[:])

	if event.EventType != mysql.FORMAT_DESCRIPTION_EVENT {
		err = fmt.Errorf("Not a FORMAT_DESCRIPTION_EVENT")
		return
	}

	fmt.Println("fde read: " + event.String())
	_, err = file.Read(buffer[mysql.BinlogEventHeaderSize+1 : event.EventSize+1])
	if err != nil {
		return
	}
	event.PacketHeader = mysql.PacketHeader{PacketLength: event.EventSize + 1}
	event.BodyLength = int(event.EventSize + 1 - mysql.BinlogEventHeaderSize)
	err = fde.Parse(&event, buffer[1:])
	if err != nil {
		fmt.Println("parse fde failed! %s\n", err.Error())
		return
	}
	fmt.Printf("FDE: %v\n", fde)
	return
}

type EventEntry struct {
	pos  uint32
	size uint32
}
type RowTransaction struct {
	Begin    EventEntry
	TableMap EventEntry
	Rows     []EventEntry
	Xid      EventEntry
}
type RowTransactionSet struct {
	transactions []RowTransaction
	maxSize      uint32
}

func scanTrans(file *os.File, from uint32, to uint32) (rtxs RowTransactionSet, err error) {
	var buffer [8192]byte
	if from >= to {
		return
	}

	fde, err := readFDE(file)
	if err != nil {
		return
	}
	hasChecksum := fde.ChecksumAlgorism == 1
	file.Seek(int64(from), 0)
	//var n int64
	pos := from
	rtxs.transactions = make([]RowTransaction, 0, 1000)

	for {
		var event mysql.BinlogEventPacket
		var tx RowTransaction
		tx.Rows = make([]EventEntry, 0, 2)

		// BEGIN
		event, err = readEvent(file, buffer[:])
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return
			}
		}
		event.HasChecksum = hasChecksum
		fmt.Printf("event:%s\n", event.String())
		if event.EventType != mysql.QUERY_EVENT && event.EventSize > 100 { // 100 is much bigger than COMMIT query size
			err = fmt.Errorf("not trans begin")
			return
		}
		reader := event.GetReader(file, buffer[:mysql.BinlogEventHeaderSize+1])
		n, _ := reader.Read(buffer[mysql.BinlogEventHeaderSize+1 : event.PacketLength])
		if n <= 0 {
			err = fmt.Errorf("read failed")
			return
		}

		var q mysql.QueryEvent
		q.Parse(&event, buffer[:])
		if q.Query != "BEGIN" {
			err = fmt.Errorf("not trans begin")
			return
		}
		_, err = io.Copy(ioutil.Discard, &reader)
		if err != nil {
			fmt.Println(err.Error())
		}
		tx.Begin = EventEntry{pos: pos, size: event.EventSize}
		pos += event.EventSize
		if event.EventSize > rtxs.maxSize {
			rtxs.maxSize = event.EventSize
		}
		// TABLE_MAP
		event, err = readEvent(file, buffer[:])
		if err != nil {
			return
		}
		event.HasChecksum = hasChecksum
		fmt.Printf("event:%s\n", event.String())
		if event.EventType != mysql.TABLE_MAP_EVENT {
			err = fmt.Errorf("not table map")
			return
		}
		reader = event.GetReader(file, buffer[:mysql.BinlogEventHeaderSize+1])
		_, err = io.Copy(ioutil.Discard, &reader)
		if err != nil {
			fmt.Println(err.Error())
		}
		tx.TableMap = EventEntry{pos: pos, size: event.EventSize}
		pos += event.EventSize
		if event.EventSize > rtxs.maxSize {
			rtxs.maxSize = event.EventSize
		}

		// ROW EVENTS
		for {
			event, err = readEvent(file, buffer[:])
			if err != nil {
				return
			}
			event.HasChecksum = hasChecksum
			fmt.Printf("event:%s\n", event.String())
			if event.EventType != mysql.UPDATE_ROWS_EVENTv2 &&
				event.EventType != mysql.DELETE_ROWS_EVENTv2 &&
				event.EventType != mysql.WRITE_ROWS_EVENTv2 {
				break
			}
			reader = event.GetReader(file, buffer[:mysql.BinlogEventHeaderSize+1])
			_, err = io.Copy(ioutil.Discard, &reader)
			if err != nil {
				fmt.Println(err.Error())
			}
			tx.Rows = append(tx.Rows, EventEntry{pos: pos, size: event.EventSize})
			pos += event.EventSize
			if event.EventSize > rtxs.maxSize {
				rtxs.maxSize = event.EventSize
			}
		}

		// XID EVENT
		if event.EventType != mysql.XID_EVENT {
			err = fmt.Errorf("not xid")
			return
		}
		reader = event.GetReader(file, buffer[:mysql.BinlogEventHeaderSize+1])
		_, err = io.Copy(ioutil.Discard, &reader)
		if err != nil {
			fmt.Println(err.Error())
		}
		tx.Xid = EventEntry{pos: pos, size: event.EventSize}
		pos += event.EventSize
		if event.EventSize > rtxs.maxSize {
			rtxs.maxSize = event.EventSize
		}
		rtxs.transactions = append(rtxs.transactions, tx)
		fmt.Println()
	}
	return
}
