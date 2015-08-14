package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"mysql_relay/mysql"
	"os"
)

const (
	MAX_EVENT_SIZE = 1048576
)

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

type BinlogFile struct {
	file   *os.File
	buffer [8192]byte
	fde    mysql.FormatDescriptionEvent
}

func (self *BinlogFile) Init(file *os.File) {
	self.file = file
}

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
	var txs []RowTransaction
	binlog := new(BinlogFile)
	binlog.Init(f)
	txs, err = binlog.scanTrans(uint32(rollbackPos), uint32(stat.Size()))
	if err != nil && err != io.EOF {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	fmt.Printf("txs:%v\n", txs)
}

func (self *BinlogFile) readEvent() (event mysql.BinlogEventPacket, err error) {
	self.buffer[0] = '\x00'
	_, err = self.file.Read(self.buffer[1 : mysql.BinlogEventHeaderSize+1])
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	event.FromBuffer(self.buffer[:])
	event.PacketLength = event.EventSize + 1 //
	event.BodyLength = int(event.PacketLength - 1 - mysql.BinlogEventHeaderSize)
	err = event.Reset(true)
	event.HasChecksum = self.fde.ChecksumAlgorism == 1
	return
}

func (self *BinlogFile) readFDE() (err error) {
	self.file.Seek(mysql.LOG_POS_START, 0)
	self.buffer[0] = '\x00'
	_, err = self.file.Read(self.buffer[1 : mysql.BinlogEventHeaderSize+1])
	if err != nil {
		fmt.Printf("read fde failed: %s\n", err.Error())
		return
	}
	var event mysql.BinlogEventPacket
	event.FromBuffer(self.buffer[:])
	if event.EventType != mysql.FORMAT_DESCRIPTION_EVENT {
		err = fmt.Errorf("Not a FORMAT_DESCRIPTION_EVENT")
		return
	}
	fmt.Println("fde read: " + event.String())
	_, err = self.file.Read(self.buffer[mysql.BinlogEventHeaderSize+1 : event.EventSize+1])
	if err != nil {
		return
	}
	event.PacketHeader = mysql.PacketHeader{PacketLength: event.EventSize + 1}
	event.BodyLength = int(event.EventSize + 1 - mysql.BinlogEventHeaderSize)
	err = self.fde.Parse(&event, self.buffer[1:])
	if err != nil {
		fmt.Println("parse fde failed! %s\n", err.Error())
		return
	}
	fmt.Printf("FDE: %v\n", self.fde)
	return
}

func (self *BinlogFile) eventIsBegin(event *mysql.BinlogEventPacket) (bool, error) {
	if event.EventType != mysql.QUERY_EVENT && event.EventSize > 100 { // 100 is much bigger than COMMIT query size
		return false, nil
	}
	reader := event.GetReader(self.file, self.buffer[:mysql.BinlogEventHeaderSize+1])
	n, _ := reader.Read(self.buffer[mysql.BinlogEventHeaderSize+1 : event.PacketLength])
	if n <= 0 {
		return false, fmt.Errorf("read failed")
	}
	var q mysql.QueryEvent
	q.Parse(event, self.buffer[:])
	if q.Query != "BEGIN" {
		return false, nil
	}
	return true, nil
}

func (self *BinlogFile) discardEvent(event *mysql.BinlogEventPacket) (err error) {
	reader := event.GetReader(self.file, self.buffer[:mysql.BinlogEventHeaderSize+1])
	_, err = io.Copy(ioutil.Discard, &reader)
	if err != nil {
		fmt.Println(err.Error())
	}
	return nil
}

func (self *BinlogFile) scanTrans(from uint32, to uint32) (rtxs []RowTransaction, err error) {
	if from >= to {
		return
	}
	err = self.readFDE()
	if err != nil {
		return
	}
	self.file.Seek(int64(from), 0)
	//var n int64
	pos := from
	rtxs = make([]RowTransaction, 0, 64)

	for {
		var event mysql.BinlogEventPacket
		var tx RowTransaction
		tx.Rows = make([]EventEntry, 0, 4)

		// BEGIN
		event, err = self.readEvent()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return
			}
		}
		fmt.Printf("event:%s\n", event.String())
		var isBegin bool
		isBegin, err = self.eventIsBegin(&event)
		if err != nil {
			return
		}
		if !isBegin {
			err = fmt.Errorf("not trans begin")
			return
		}
		//discardEvent(&event, file, buffer[:])
		tx.Begin = EventEntry{pos: pos, size: event.EventSize}
		pos += event.EventSize

		// TABLE_MAP
		event, err = self.readEvent()
		if err != nil {
			return
		}
		fmt.Printf("event:%s\n", event.String())
		if event.EventType != mysql.TABLE_MAP_EVENT {
			err = fmt.Errorf("not table map")
			return
		}
		self.discardEvent(&event)
		tx.TableMap = EventEntry{pos: pos, size: event.EventSize}
		pos += event.EventSize

		// ROW EVENTS
		for {
			event, err = self.readEvent()
			if err != nil {
				return
			}
			fmt.Printf("event:%s\n", event.String())
			if event.EventType != mysql.UPDATE_ROWS_EVENTv2 &&
				event.EventType != mysql.DELETE_ROWS_EVENTv2 &&
				event.EventType != mysql.WRITE_ROWS_EVENTv2 {
				break
			}
			self.discardEvent(&event)
			tx.Rows = append(tx.Rows, EventEntry{pos: pos, size: event.EventSize})
			pos += event.EventSize
		}
		// XID EVENT
		if event.EventType != mysql.XID_EVENT {
			err = fmt.Errorf("not xid")
			return
		}
		self.discardEvent(&event)
		tx.Xid = EventEntry{pos: pos, size: event.EventSize}
		pos += event.EventSize

		rtxs = append(rtxs, tx)
		fmt.Println()
	}
	return
}
