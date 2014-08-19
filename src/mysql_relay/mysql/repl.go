package mysql

import (
    //"io"
    //"encoding/binary"
    //"fmt"
)

const (
    LOG_EVENT_BINLOG_IN_USE_F = 0x0001
    LOG_EVENT_FORCED_ROTATE_F = 0x0002
    LOG_EVENT_THREAD_SPECIFIC_F = 0x0004
    LOG_EVENT_SUPPRESS_USE_F = 0x0008
    LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F = 0x0010
    LOG_EVENT_ARTIFICIAL_F = 0x0020
    LOG_EVENT_RELAY_LOG_F = 0x0040
    LOG_EVENT_IGNORABLE_F = 0x0080
    LOG_EVENT_NO_FILTER_F = 0x0100
    LOG_EVENT_MTS_ISOLATE_F = 0x0200
)
type BinlogEventPacket struct {
    PacketHeader
    Timestamp  uint32
    EventType  byte
    ServerId   uint32
    EventSize  uint32
    LogPos     uint32
    Flags      uint16
}

func (self *BinlogEventPacket) FromBuffer(buffer []byte) (err error) {
    self.Timestamp = ENDIAN.Uint32(buffer[1:])
    self.EventType = buffer[5]
    self.ServerId  = ENDIAN.Uint32(buffer[6:])
    self.EventSize = ENDIAN.Uint32(buffer[10:])
    self.LogPos    = ENDIAN.Uint32(buffer[14:])
    self.Flags     = ENDIAN.Uint16(buffer[18:])
    self.BodyLength = int(self.PacketLength) - 20
    return
}

