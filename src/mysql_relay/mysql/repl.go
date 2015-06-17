package mysql

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"mysql_relay/util"
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
	UNKNOWN_EVENT             byte = 0x00
	START_EVENT_V3                 = 0x01
	QUERY_EVENT                    = 0x02
	STOP_EVENT                     = 0x03
	ROTATE_EVENT                   = 0x04
	INTVAR_EVENT                   = 0x05
	LOAD_EVENT                     = 0x06
	SLAVE_EVENT                    = 0x07
	CREATE_FILE_EVENT              = 0x08
	APPEND_BLOCK_EVENT             = 0x09
	EXEC_LOAD_EVENT                = 0x0a
	DELETE_FILE_EVENT              = 0x0b
	NEW_LOAD_EVENT                 = 0x0c
	RAND_EVENT                     = 0x0d
	USER_VAR_EVENT                 = 0x0e
	FORMAT_DESCRIPTION_EVENT       = 0x0f
	XID_EVENT                      = 0x10
	BEGIN_LOAD_QUERY_EVENT         = 0x11
	EXECUTE_LOAD_QUERY_EVENT       = 0x12
	TABLE_MAP_EVENT                = 0x13
	WRITE_ROWS_EVENTv0             = 0x14
	UPDATE_ROWS_EVENTv0            = 0x15
	DELETE_ROWS_EVENTv0            = 0x16
	WRITE_ROWS_EVENTv1             = 0x17
	UPDATE_ROWS_EVENTv1            = 0x18
	DELETE_ROWS_EVENTv1            = 0x19
	INCIDENT_EVENT                 = 0x1a
	HEARTBEAT_EVENT                = 0x1b
	IGNORABLE_EVENT                = 0x1c
	ROWS_QUERY_EVENT               = 0x1d
	WRITE_ROWS_EVENTv2             = 0x1e
	UPDATE_ROWS_EVENTv2            = 0x1f
	DELETE_ROWS_EVENTv2            = 0x20
	GTID_EVENT                     = 0x21
	ANONYMOUS_GTID_EVENT           = 0x22
	PREVIOUS_GTIDS_EVENT           = 0x23
	TRANSACTION_CONTEXT_EVENT      = 0x24
	VIEW_CHANGE_EVENT              = 0x25
	XA_PREPARE_LOG_EVENT           = 0x26
	BINLOG_EVENT_END               = 0x27
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
	"ANONYMOUS_GTID_EVENT", "PREVIOUS_GTIDS_EVENT", "TRANSACTION_CONTEXT_EVENT",
	"VIEW_CHANGE_EVENT", "XA_PREPARE_LOG_EVENT", "BINLOG_EVENT_END"}

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
	Semisync    byte
}

const (
	SEMISYNC_NO   byte = 0
	SEMISYNC_ACK       = 1
	SEMISYNC_SKIP      = 2
)

//https://dev.mysql.com/doc/internals/en/semi-sync-ack-packet.html
//   payload:
//     1                  [ef]
//     8                  log position
//     string             log filename
type SemisyncAckPacket struct {
	PacketHeader
	Position uint64
	Name     string
}

func (self *SemisyncAckPacket) ToBuffer(buffer []byte) (writen int, err error) {
	buffer[0] = '\xef'
	ENDIAN.PutUint64(buffer[1:], self.Position)
	writen = 9
	writen += copy(buffer[9:], []byte(self.Name))
	return
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
	p := 1
	if self.Semisync != SEMISYNC_NO {
		p = 3
	}
	self.Timestamp = ENDIAN.Uint32(buffer[p:])
	self.EventType = buffer[p+4]
	self.ServerId = ENDIAN.Uint32(buffer[p+5:])
	self.EventSize = ENDIAN.Uint32(buffer[p+9:])
	self.LogPos = ENDIAN.Uint32(buffer[p+13:])
	self.Flags = ENDIAN.Uint16(buffer[p+17:])
	self.BodyLength = int(self.PacketLength) - BinlogEventHeaderSize - p
	read = BinlogEventHeaderSize + p
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
		//fmt.Printf("crc32 of %v == %08x\n", buffer[1:writen], checksum)
		ENDIAN.PutUint32(buffer[writen:], checksum)
		writen += 4
	}
	//fmt.Printf("rotate event packet: %dbytes\n", writen)
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

type BinlogEventStream struct {
	curEvent BinlogEventPacket
	ret      chan *BinlogEventPacket
	canRead  chan struct{}
	semisync bool
	errs     error
}

func (self *BinlogEventStream) GetError() error {
	return self.errs
}

func (self *BinlogEventStream) IsSemisync() bool {
	return self.semisync
}

func (self *BinlogEventStream) Next() *BinlogEventPacket {
	self.canRead <- struct{}{}
	return <-self.ret
}

func readEventPacketHeader(readWriter io.ReadWriter, buffer []byte, expectedSeq byte) (header PacketHeader, err error) {
	defer util.RecoverToError(&err)
	header = util.Assert1(ReadPacketHeader(readWriter)).(PacketHeader)
	if header.PacketSeq != expectedSeq {
		err = PACKET_SEQ_NOT_CORRECT
		return
	}

	util.Assert0(ReadPacket(header, readWriter, buffer))
	if buffer[0] != GRP_OK {
		var errPacket ErrPacket
		errPacket.PacketHeader = header
		_ = util.Assert1(errPacket.FromBuffer(buffer))
		err = errPacket.ToError()
	}

	return
}

func (self *Client) DumpBinlog(cmdBinlogDump ComBinglogDump, semisync bool, heartbeatPeriod uint32) (ret *BinlogEventStream, err error) {
	//fmt.Printf("DumpBinlog %v!!! ...", cmdBinlogDump)
	ret = new(BinlogEventStream)
	ret.ret = make(chan *BinlogEventPacket)
	ret.canRead = make(chan struct{})
	defer func() {
		util.RecoverToError(&err)
		if err != nil {
			close(ret.ret)
		}
	}()
	_ = util.Assert1(self.Command(&QueryCommand{Query: "SET @master_binlog_checksum='NONE';"}))
	_ = util.Assert1(self.Command(&QueryCommand{Query: fmt.Sprintf("SET @master_heartbeat_period=%d;", heartbeatPeriod)}))
	if semisync {
		_, semi_err := self.Command(&QueryCommand{Query: "SET @rpl_semi_sync_slave = 1;"})
		ret.semisync = (semi_err == nil)
	}

	cmdPacket := CommandPacket{Command: &cmdBinlogDump}
	util.Assert0(WritePacketTo(&cmdPacket, self.Conn, self.Buffer[:]))

	go func() {
		defer func() {
			close(ret.ret)
			util.RecoverToError(&ret.errs)
		}()
		seq := byte(0)
		<-ret.canRead
		for {
			var event BinlogEventPacket
			seq++
			header := util.Assert1(readEventPacketHeader(self.Conn, self.Buffer[:], seq)).(PacketHeader)
			event.PacketHeader = header
			event.Semisync = SEMISYNC_NO
			if ret.semisync {
				// see https://dev.mysql.com/doc/internals/en/semi-sync-binlog-event.html
				if self.Buffer[1] != '\xef' {
					// TODO: error!
				}
				if self.Buffer[2]&0x01 != 0 {
					event.Semisync = SEMISYNC_ACK
				} else {
					event.Semisync = SEMISYNC_SKIP
				}
			}

			_ = util.Assert1(event.FromBuffer(self.Buffer[:]))
			ret.ret <- &event //
			<-ret.canRead
			packetReader := event.GetReader(self.Conn, self.Buffer[:])
			io.Copy(ioutil.Discard, &packetReader)
		}
	}()
	return
}

func (self *Client) SendSemisyncAck(name string, position uint64) error {
	var buffer [64]byte // TODO: test if buffer size is enough to contain name
	return WritePacketTo(&SemisyncAckPacket{Name: name, Position: position}, self.Conn, buffer[:])
}
