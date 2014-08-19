package mysql

import (
    "io"
)

/*
http://dev.mysql.com/doc/internals/en/com-query-response.html
http://dev.mysql.com/doc/internals/en/protocoltext-resultset.html
*/


type ColumnCountPacket struct {
    PacketHeader
    ColumnCount  uint64
}

func (self *ColumnCountPacket) FromBuffer(buffer []byte) (err error) {
    self.ColumnCount, _ = getLenencInt(buffer)
    return
}

type ColumnDefinitionPacket struct {
/*
http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
as
http://dev.mysql.com/doc/refman/5.0/en/identifiers.html
max length of schema, table, name is 64, so total size will be less than the buffer size
*/
    PacketHeader
    Catalog      string
    Schema       string
    Table        string
    OrgTable     string
    Name         string
    OrgName      string
    FixedLength  uint64
    CharacterSet uint16
    ColumnLength uint32
    Type         byte
    Flags        uint16
    Decimals     byte
    Filler       uint16
}

func (self *ColumnDefinitionPacket) FromBuffer(buffer []byte) (err error) {
    var strLen int
    var p      int
    var n      int
    p = 0
    // n should always be 1
    strLen, n = getLenencInt(buffer[p:])
    p+= n
    self.Catalog = string(buffer[p:p+strLen])
    p+= strLen
    
    strLen, n = getLenencInt(buffer[p:])
    p+= n
    self.Schema = string(buffer[p:p+strLen])
    p+= strLen
    
    strLen, n = getLenencInt(buffer[p:])
    p+= n
    self.Table = string(buffer[p:p+strLen])
    p+= strLen
    
    strLen, n = getLenencInt(buffer[p:])
    p+= n
    self.OrgTable = string(buffer[p:p+strLen])
    p+= strLen
    
    strLen, n = getLenencInt(buffer[p:])
    p+= n
    self.Name = string(buffer[p:p+strLen])
    p+= strLen
    
    strLen, n = getLenencInt(buffer[p:])
    p+= n
    self.OrgName = string(buffer[p:p+strLen])
    p+= strLen
    
    self.FixedLength, n = getLenencInt(buffer[p:])
    p+= n
    
    self.CharacterSet = ENDIAN.Uint16(buffer[p:])
    p+= 2
    
    self.ColumnLength = ENDIAN.Uint32(buffer[p:])
    p+= 4
    
    self.Type = buffer[p]
    p+= 1
    
    self.Flags = ENDIAN.Uint16(buffer[p:])
    p+= 2
    
    self.Decimals = buffer[p]
    p+= 1
    
    self.Filler = ENDIAN.Uint16(self.buffer[p:])
    p+= 2
    
    self.BodyLength = int(self.PacketLength) -  p
    return
}


type ResultRowPacket struct {
    PacketHeader
    Columns [][]byte
    Buffer  []byte
}

func (self *ResultRowPacket) Init(columnCount int) {
    self.Columns = make([]string, columnCount)
}

func (self *ResultRowPacket) FromBuffer(buffer []byte) (err error) {
    if self.PacketLength > len(buffer) {
        err = BUFFER_NOT_SUFFICIENT
        return
    }
    var strLen int
    var p      int
    var n      int
    p = 0
    for i := range self.Columns {
        // n should always be 1
        strLen, n = getLenencInt(buffer[p:])
        if n == 0 { // nil
            p++
            self.Columns[i] = nil
            continue
        }
        p+= n
        self.Columns[i] = buffer[p:p+strLen]
        p+= strLen
    }
}

func (self *ResultRowPacket) FromReader(reader io.Reader) (err error) {
    if self.Buffer == nil || len(self.Buffer) < self.PacketLength {
        self.Buffer = make([]byte, self.PacketLength)
    }
    _, err = reader.Read(self.Buffer)
    if err != nil {
        return
    }
    err = self.FromBuffer(self.Buffer)
}
