package mysql

import (
    "encoding/binary"
    "io"
    //"fmt"
)


type Command interface {
    CommandType() byte
    Outputable
}

type CommandPacket struct {
    PacketHeader
    Command
}

type ComRegisterSlave struct {
/*
http://dev.mysql.com/doc/internals/en/com-register-slave.html
1              [15] COM_REGISTER_SLAVE
4              server-id
1              slaves hostname length
string[$len]   slaves hostname
1              slaves user len
string[$len]   slaves user
1              slaves password len
string[$len]   slaves password
2              slaves mysql-port
4              replication rank
4              master-id
*/
    ServerId  uint32
}

func (self *ComRegisterSlave) ToBuffer(buffer []byte) (writen int, err error) {
    buffer[0] = byte(COM_REGISTER_SLAVE)
    binary.LittleEndian.PutUint32(buffer[1:], self.ServerId)
    for i := range buffer[5:17] {
        buffer[i] = 0
    }
    return 17, nil
}

func (self *ComRegisterSlave) CommandType() byte {
    return COM_REGISTER_SLAVE
}

type ComBinglogDump struct {
/*
http://dev.mysql.com/doc/internals/en/com-binlog-dump.html
1              [12] COM_BINLOG_DUMP
4              binlog-pos
2              flags
4              server-id
string[EOF]    binlog-filename
*/
    BinlogPos       uint32
    Flags           uint16
    ServerId        uint32
    BinlogFilename  string
}

func (self *ComBinglogDump) ToBuffer(buffer []byte) (writen int, err error) {
    buffer[0] = byte(COM_BINLOG_DUMP)
    binary.LittleEndian.PutUint32(buffer[1:], self.BinlogPos)
    binary.LittleEndian.PutUint16(buffer[5:], self.Flags)
    binary.LittleEndian.PutUint32(buffer[7:], self.ServerId)
    copy(buffer[11:], []byte(self.BinlogFilename))
    writen = 11+len(self.BinlogFilename)
    return
}

func (self *ComBinglogDump) CommandType() byte {
    return COM_BINLOG_DUMP
}

func ExecCommand(command Command, readWriter io.ReadWriter, buffer []byte) (ret OkPacket, err error) {
    cmdPacket := CommandPacket{Command:command}
    err = WritePacketTo(&cmdPacket, readWriter, buffer)
    if err != nil {
        return
    }
    packet, err := ReadGenericResponsePacket(readWriter, buffer)
    if err != nil {
        return
    }
    ret, err = packet.ToOk()
    return
}

