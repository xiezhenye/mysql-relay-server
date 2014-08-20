package mysql

import (
    "io"
    "fmt"
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
    PayloadPacket
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

func DumpBinlogTo(cmdBinlogDump ComBinglogDump, readWriter io.ReadWriter, canRead <-chan struct{}, ret chan<-BinlogEventPacket, buffer []byte) (err error){
    defer close(ret)
    cmdPacket := CommandPacket{Command:&cmdBinlogDump}
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
            fmt.Println(header)
            return 
        }
        err = ReadPacket(header, readWriter, buffer)
        if err != nil {
            return
        }
        if buffer[0] != GRP_OK {
            var errPacket ErrPacket
            errPacket.PacketHeader = header
            err = errPacket.FromBuffer(buffer)
            if err != nil {
                return
            }
            err = errPacket.ToError()
            return
        }
        var event BinlogEventPacket
        event.PacketHeader = header
        err = event.FromBuffer(buffer)
        if err != nil {
            return
        }
        fmt.Println(event)
        ret<-event
        <-canRead
    }
    return
}
