package mysql

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
)

const (
	LOG_EVENT_BINLOG_IN_USE_F            = 0x0001
	LOG_EVENT_FORCED_ROTATE_F            = 0x0002
	LOG_EVENT_THREAD_SPECIFIC_F          = 0x0004
	LOG_EVENT_SUPPRESS_USE_F             = 0x0008
	LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F = 0x0010
	LOG_EVENT_ARTIFICIAL_F               = 0x0020
	LOG_EVENT_RELAY_LOG_F                = 0x0040
	LOG_EVENT_IGNORABLE_F                = 0x0080
	LOG_EVENT_NO_FILTER_F                = 0x0100
	LOG_EVENT_MTS_ISOLATE_F              = 0x0200
)

const LOG_POS_START = 4

const (
	UNKNOWN_EVENT            byte = 0x00
	START_EVENT_V3                = 0x01
	QUERY_EVENT                   = 0x02
	STOP_EVENT                    = 0x03
	ROTATE_EVENT                  = 0x04
	INTVAR_EVENT                  = 0x05
	LOAD_EVENT                    = 0x06
	SLAVE_EVENT                   = 0x07
	CREATE_FILE_EVENT             = 0x08
	APPEND_BLOCK_EVENT            = 0x09
	EXEC_LOAD_EVENT               = 0x0a
	DELETE_FILE_EVENT             = 0x0b
	NEW_LOAD_EVENT                = 0x0c
	RAND_EVENT                    = 0x0d
	USER_VAR_EVENT                = 0x0e
	FORMAT_DESCRIPTION_EVENT      = 0x0f
	XID_EVENT                     = 0x10
	BEGIN_LOAD_QUERY_EVENT        = 0x11
	EXECUTE_LOAD_QUERY_EVENT      = 0x12
	TABLE_MAP_EVENT               = 0x13
	WRITE_ROWS_EVENTv0            = 0x14
	UPDATE_ROWS_EVENTv0           = 0x15
	DELETE_ROWS_EVENTv0           = 0x16
	WRITE_ROWS_EVENTv1            = 0x17
	UPDATE_ROWS_EVENTv1           = 0x18
	DELETE_ROWS_EVENTv1           = 0x19
	INCIDENT_EVENT                = 0x1a
	HEARTBEAT_EVENT               = 0x1b
	IGNORABLE_EVENT               = 0x1c
	ROWS_QUERY_EVENT              = 0x1d
	WRITE_ROWS_EVENTv2            = 0x1e
	UPDATE_ROWS_EVENTv2           = 0x1f
	DELETE_ROWS_EVENTv2           = 0x20
	GTID_EVENT                    = 0x21
	ANONYMOUS_GTID_EVENT          = 0x22
	PREVIOUS_GTIDS_EVENT          = 0x23
	BINLOG_EVENT_END              = 0x24
)

const BinlogEventHeaderSize = 19

var EventNames = [...]string{"UNKNOWN_EVENT", "START_EVENT_V3", "QUERY_EVENT",
	"STOP_EVENT", "ROTATE_EVENT", "INTVAR_EVENT", "LOAD_EVENT", "SLAVE_EVENT",
	"CREATE_FILE_EVENT", "APPEND_BLOCK_EVENT", "EXEC_LOAD_EVENT",
	"DELETE_FILE_EVENT", "NEW_LOAD_EVENT", "RAND_EVENT", "USER_VAR_EVENT",
	"FORMAT_DESCRIPTION_EVENT", "XID_EVENT", "BEGIN_LOAD_QUERY_EVENT",
	"EXECUTE_LOAD_QUERY_EVENT", "TABLE_MAP_EVENT", "WRITE_ROWS_EVENTv0",
	"UPDATE_ROWS_EVENTv0", "DELETE_ROWS_EVENTv0", "WRITE_ROWS_EVENTv1",
	"UPDATE_ROWS_EVENTv1", "DELETE_ROWS_EVENTv1", "INCIDENT_EVENT",
	"HEARTBEAT_EVENT", "IGNORABLE_EVENT", "ROWS_QUERY_EVENT", "WRITE_ROWS_EVENTv2",
	"UPDATE_ROWS_EVENTv2", "DELETE_ROWS_EVENTv2", "GTID_EVENT",
	"ANONYMOUS_GTID_EVENT", "PREVIOUS_GTIDS_EVENT", "BINLOG_EVENT_END"}

type BinlogEventPacket struct {
	PayloadPacket
	Timestamp uint32
	EventType byte
	ServerId  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
	//
	HasChecksum bool
}

func (self *BinlogEventPacket) String() string {
	return fmt.Sprintf("type: %s, pos: %d, size: %d, timestamp: %d",
		EventNames[self.EventType], self.LogPos, self.EventSize, self.Timestamp)
}

func ParseBinlogName(name string) (prefix string, n int64, err error) {
	//"log-bin.000005"
	p := strings.LastIndex(name, ".")
	if p < 0 {
		err = errors.New("not valid binlog name")
		return
	}
	prefix = name[0:p]
	n, err = strconv.ParseInt(name[p+1:], 10, 0)
	return
}

func ToBinlogName(prefix string, n int64) string {
	return fmt.Sprintf("%s.%06d", prefix, n)
}

func NextBinlogName(name string) (next string, err error) {
	var prefix string
	var n int64
	prefix, n, err = ParseBinlogName(name)
	if err != nil {
		return
	}
	next = ToBinlogName(prefix, n+1)
	return
}

func (self *BinlogEventPacket) FromBuffer(buffer []byte) (read int, err error) {
	self.Timestamp = ENDIAN.Uint32(buffer[1:])
	self.EventType = buffer[5]
	self.ServerId = ENDIAN.Uint32(buffer[6:])
	self.EventSize = ENDIAN.Uint32(buffer[10:])
	self.LogPos = ENDIAN.Uint32(buffer[14:])
	self.Flags = ENDIAN.Uint16(buffer[18:])
	self.BodyLength = int(self.PacketLength) - BinlogEventHeaderSize - 1
	read = BinlogEventHeaderSize + 1
	return
}

func (self *BinlogEventPacket) ToBuffer(buffer []byte) (writen int, err error) {
	buffer[0] = '\x00'
	ENDIAN.PutUint32(buffer[1:], self.Timestamp)
	buffer[5] = self.EventType
	ENDIAN.PutUint32(buffer[6:], self.ServerId)
	ENDIAN.PutUint32(buffer[10:], self.EventSize)
	ENDIAN.PutUint32(buffer[14:], self.LogPos)
	ENDIAN.PutUint16(buffer[18:], self.Flags)
	writen = BinlogEventHeaderSize + 1
	return
}

func (self *BinlogEventPacket) IsFake() bool {
	return self.LogPos == 0
}

type FormatDescriptionEvent struct {
	BinlogVersion         uint16
	MysqlServerVersion    string
	CreateTimestamp       uint32
	EventHeaderLength     byte
	EventTypeHeaderLength [40]byte
	ChecksumAlgorism      byte
}

type RotateEvent struct {
	Name     string
	Position uint64
}

type QueryEvent struct {
	SlaveProxyId  uint32
	ExecutionTime uint32
	Schema        string
	ErrorCode     uint16
	StatusVars    string
	Query         string
}

type TableMapColumnEntry struct {
	Type byte
	// Meta uint32
	// Size uint32
	// Null bool
}
type TableMapEvent struct {
	TableId    uint32
	Flags      uint16
	SchemaName string
	TableName  string
	Columns    []TableMapColumnEntry
}

type RotateEventPacket struct {
	BinlogEventPacket
	RotateEvent
}

func (self *RotateEvent) Parse(packet *BinlogEventPacket, buffer []byte) (err error) {
	if packet.EventType != ROTATE_EVENT {
		err = NOT_SUCH_EVENT
		return
	}
	p := int(packet.PacketLength) - packet.BodyLength
	self.Position = ENDIAN.Uint64(buffer[p:])
	end := packet.PacketLength
	if packet.HasChecksum {
		end -= 4
	}
	self.Name = string((buffer[p+8 : end]))
	return
}

func (self *RotateEvent) BuildFakePacket(serverId uint32) RotateEventPacket {
	var ret RotateEventPacket
	ret.LogPos = 0
	ret.ServerId = serverId
	ret.EventSize = uint32(len(self.Name)+8) + BinlogEventHeaderSize
	ret.EventType = ROTATE_EVENT
	ret.HasChecksum = true
	ret.Flags = LOG_EVENT_ARTIFICIAL_F
	if ret.HasChecksum {
		ret.EventSize += 4
	}
	//ret.PacketHeader.PacketLength = ret.EventSize + 1
	//ret.PacketHeader.PacketSeq = 1
	ret.RotateEvent = *self
	return ret
}

func (self *RotateEventPacket) ToBuffer(buffer []byte) (writen int, err error) {
	n := 0
	n, err = self.BinlogEventPacket.ToBuffer(buffer)
	if err != nil {
		return
	}
	writen, err = self.RotateEvent.ToBuffer(buffer[n:])
	writen += n
	if self.HasChecksum {
		checksum := crc32.ChecksumIEEE(buffer[1:writen])
		fmt.Printf("crc32 of %v == %08x\n", buffer[1:writen], checksum)
		ENDIAN.PutUint32(buffer[writen:], checksum)
		writen += 4
	}
	fmt.Printf("rotate event packet: %dbytes\n", writen)
	return
}

func (self *RotateEvent) ToBuffer(buffer []byte) (writen int, err error) {
	ENDIAN.PutUint64(buffer, self.Position)
	writen = copy(buffer[8:], []byte(self.Name)) + 8
	return
}

func (self *FormatDescriptionEvent) Parse(packet *BinlogEventPacket, buffer []byte) (err error) {
	/*
	   http://dev.mysql.com/doc/internals/en/format-description-event.html
	    size of FormatDescriptionEvent is 92 now
	*/
	if packet.EventType != FORMAT_DESCRIPTION_EVENT {
		err = NOT_SUCH_EVENT
		return
	}
	p := int(packet.PacketLength) - packet.BodyLength
	self.BinlogVersion = ENDIAN.Uint16(buffer[p:])
	p += 2
	self.MysqlServerVersion = strings.TrimRight(string(buffer[p:p+50]), "\x00")
	p += 50
	self.CreateTimestamp = ENDIAN.Uint32(buffer[p:])
	p += 4
	self.EventHeaderLength = buffer[p]
	p += 1
	copy(self.EventTypeHeaderLength[:], buffer[p:p+int(BINLOG_EVENT_END)])

	formatDescriptionEventSize := int(self.EventTypeHeaderLength[FORMAT_DESCRIPTION_EVENT-1])

	tailSize := int(packet.EventSize) - int(self.EventHeaderLength) - formatDescriptionEventSize
	if tailSize == (1 + 4) {
		p = int(packet.PacketLength) - packet.BodyLength + formatDescriptionEventSize
		self.ChecksumAlgorism = buffer[p]
	}
	return
}

func (self *QueryEvent) Parse(packet *BinlogEventPacket, buffer []byte) (err error) {
	if packet.EventType != QUERY_EVENT {
		err = NOT_SUCH_EVENT
		return
	}
	p := int(packet.PacketLength) - packet.BodyLength
	self.SlaveProxyId = ENDIAN.Uint32(buffer[p:])
	p += 4
	self.ExecutionTime = ENDIAN.Uint32(buffer[p:])
	p += 4
	schemaLength := buffer[p]
	p += 1
	self.ErrorCode = ENDIAN.Uint16(buffer[p:])
	p += 2
	statusVarLength := ENDIAN.Uint16(buffer[p:])
	p += 2
	self.StatusVars = string(buffer[p : p+int(statusVarLength)])
	p += int(statusVarLength)
	self.Schema = string(buffer[p : p+int(schemaLength)])
	p += int(schemaLength)
	p += 1 // 00

	end := packet.PacketLength
	if packet.HasChecksum {
		end -= 4
	}
	self.Query = string((buffer[p:end]))
	return
}

func DumpBinlogTo(cmdBinlogDump ComBinglogDump, readWriter io.ReadWriter, ret chan<- BinlogEventPacket, buffer []byte) (err error) {
	defer close(ret)
	cmdPacket := CommandPacket{Command: &cmdBinlogDump}
	err = WritePacketTo(&cmdPacket, readWriter, buffer)
	if err != nil {
		return
	}
	seq := byte(0)
	for {
		seq++
		var header PacketHeader
		header, err = ReadPacketHeader(readWriter)
		if err != nil {
			return
		}
		if header.PacketSeq != seq {
			err = PACKET_SEQ_NOT_CORRECT
			return
		}
		err = ReadPacket(header, readWriter, buffer)
		if err != nil {
			return
		}
		if buffer[0] != GRP_OK {
			var errPacket ErrPacket
			errPacket.PacketHeader = header
			_, err = errPacket.FromBuffer(buffer)
			if err != nil {
				return
			}
			err = errPacket.ToError()
			return
		}
		var event BinlogEventPacket
		event.PacketHeader = header
		_, err = event.FromBuffer(buffer)
		if err != nil {
			return
		}
		ret <- event
		reader := event.GetReader(readWriter, buffer)
		io.Copy(ioutil.Discard, &reader)
	}
	return
}

type BinlogEventStream struct {
	ret     chan BinlogEventPacket
	errs    chan error
	canRead chan struct{}
}

func (self *BinlogEventStream) GetChan() <-chan BinlogEventPacket {
	return self.ret
}

func (self *BinlogEventStream) GetError() error {
	err, _ := <-self.errs
	return err
}

func (self *Client) DumpBinlog(cmdBinlogDump ComBinglogDump) (ret BinlogEventStream) {
	var err error
	_, err = self.Command(&QueryCommand{Query: "SET @master_binlog_checksum='NONE';"})
	if err != nil {
		ret.errs <- err
		close(ret.errs)
		close(ret.canRead)
		return
	}
	ret.ret = make(chan BinlogEventPacket)
	ret.errs = make(chan error)
	//ret.canRead = make(chan struct{})
	go func() {
		err := DumpBinlogTo(cmdBinlogDump, self.Conn, ret.ret, self.Buffer[:])
		if err != nil {
			ret.errs <- err
		}
		close(ret.errs)
		close(ret.canRead)
	}()
	return
}
