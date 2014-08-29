package mysql

import (
    //"io"
)

/*
http://dev.mysql.com/doc/internals/en/com-query-response.html
http://dev.mysql.com/doc/internals/en/protocoltext-resultset.html
*/

type QueryCommand struct {
    Query string
}

func (self *QueryCommand) CommandType() byte {
    return COM_QUERY
}

func (self *QueryCommand) ToBuffer(buffer []byte) (writen int, err error) {
    buffer[0] = COM_QUERY
    n := copy(buffer[1:], []byte(self.Query))
    writen = n + 1
    return
}

type ColumnCountPacket struct {
    PacketHeader
    ColumnCount  uint64
}

func (self *ColumnCountPacket) FromBuffer(buffer []byte) (read int, err error) {
    var leInt LenencInt
    read, _ = leInt.FromBuffer(buffer)
    self.ColumnCount = uint64(leInt)
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

func (self *ColumnDefinitionPacket) FromBuffer(buffer []byte) (read int, err error) {
    var p      int
    var n      int
    p = 0
    // n should always be 1
    
    var leStr LenencString
    n, _ = leStr.FromBuffer(buffer[p:])
    self.Catalog = string(leStr)
    p+= n
    
    n, _ = leStr.FromBuffer(buffer[p:])
    self.Schema = string(leStr)
    p+= n
    
    n, _ = leStr.FromBuffer(buffer[p:])
    self.Table = string(leStr)
    p+= n
    
    n, _ = leStr.FromBuffer(buffer[p:])
    self.OrgTable = string(leStr)
    p+= n
    
    n, _ = leStr.FromBuffer(buffer[p:])
    self.Name = string(leStr)
    p+= n
    
    n, _ = leStr.FromBuffer(buffer[p:])
    self.OrgName = string(leStr)
    p+= n
    
    var leInt LenencInt
    n, _ = leInt.FromBuffer(buffer[p:])
    self.FixedLength = uint64(n)
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
    
    self.Filler = ENDIAN.Uint16(buffer[p:])
    p+= 2
    read = p
    return
}


type ResultRowPacket struct {
    PacketHeader
    Columns []string
}

func (self *ResultRowPacket) Init(columnCount int) {
    self.Columns = make([]string, columnCount)
}

func (self *ResultRowPacket) FromBuffer(buffer []byte) (read int, err error) {
    if int(self.PacketLength) > len(buffer) {
        err = BUFFER_NOT_SUFFICIENT
        return
    }
    p := 0
    for i := range self.Columns {
        // n should always be 1
        var leStr LenencString
        n, _err := leStr.FromBuffer(buffer[p:])
        p+= n
        if _err != nil {
            self.Columns[i] = ""
        } else {
            self.Columns[i] = string(leStr)
        }
    }
    read = p
    return
}
/*
func (self *ResultRowPacket) FromReader(reader io.Reader) (err error) {
    if self.Buffer == nil || len(self.Buffer) < int(self.PacketLength) {
        self.Buffer = make([]byte, self.PacketLength)
    }
    _, err = reader.Read(self.Buffer)
    if err != nil {
        return
    }
    _, err = self.FromBuffer(self.Buffer)
    return
}
*/