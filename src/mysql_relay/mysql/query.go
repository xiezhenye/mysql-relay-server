package mysql

import (
    "io"
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
    read, err = leInt.FromBuffer(buffer)
    self.ColumnCount = uint64(leInt)
    return
}

func (self *ColumnCountPacket) ToBuffer(buffer []byte) (writen int, err error) {
    leInt := LenencInt(self.ColumnCount)
    writen, err = leInt.ToBuffer(buffer)
    return
}

type ColumnDefinition struct {
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

type ColumnDefinitionPacket struct {
/*
http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
as
http://dev.mysql.com/doc/refman/5.0/en/identifiers.html
max length of schema, table, name is 64, so total size will be less than the buffer size
*/
    PacketHeader
    ColumnDefinition
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

func (self *ColumnDefinitionPacket) ToBuffer(buffer []byte) (writen int, err error) {
    var p      int
    var n      int
    p = 0
    // n should always be 1
    var leStr LenencString
    var leInt LenencInt
    leStr = LenencString(self.Catalog)
    n, _ = leStr.ToBuffer(buffer[p:])
    p+= n
    
    leStr = LenencString(self.Schema)
    n, _ = leStr.ToBuffer(buffer[p:])
    p+= n
    
    leStr = LenencString(self.Table)
    n, _ = leStr.ToBuffer(buffer[p:])
    p+= n
    
    leStr = LenencString(self.OrgTable)
    n, _ = leStr.ToBuffer(buffer[p:])
    p+= n
    
    leStr = LenencString(self.Name)
    n, _ = leStr.ToBuffer(buffer[p:])
    p+= n
    
    leStr = LenencString(self.OrgName)
    n, _ = leStr.ToBuffer(buffer[p:])
    p+= n
    
    leInt = LenencInt(self.FixedLength)
    n, _ = leInt.ToBuffer(buffer[p:])
    p+= n
    
    ENDIAN.PutUint16(buffer[p:], self.CharacterSet)
    p+= 2
    
    ENDIAN.PutUint32(buffer[p:], self.ColumnLength)
    p+= 4
    
    buffer[p] = self.Type
    p+= 1
    
    ENDIAN.PutUint16(buffer[p:], self.Flags)
    p+= 2
    
    buffer[p] = self.Decimals
    p+= 1
    
    ENDIAN.PutUint16(buffer[p:], self.Filler)
    p+= 2
    writen = p
    return
}

/*
decimals (1) -- max shown decimal digits
0x00 for integers and static strings
0x1f for dynamic strings, double, float
0x00 to 0x51 for decimals
*/


type ResultRowPacket struct {
    PacketHeader
    ResultRow
}

type ResultRow struct {
    Values     []Value
}

type Value struct {
    Value       string
    IsNull      bool
}

func (self *ResultRowPacket) Init(columnCount int) {
    self.Values = make([]Value, columnCount)
}

func (self *ResultRowPacket) FromBuffer(buffer []byte) (read int, err error) {
    if int(self.PacketLength) > len(buffer) {
        err = BUFFER_NOT_SUFFICIENT
        return
    }
    p := 0
    for i := range self.Values {
        // n should always be 1
        var leStr LenencString
        n, _err := leStr.FromBuffer(buffer[p:])
        p+= n
        if _err != nil {
            self.Values[i] = Value{ Value: "", IsNull: true }
        } else {
            self.Values[i]= Value{ Value: string(leStr), IsNull: false } 
        }
    }
    read = p
    return
}

type ResultSet struct {
    Columns      []ColumnDefinition
    Rows         [][]string
}

type Cursor struct {
    Reader       io.Reader
    Columns      []ColumnDefinition
    Rows         chan []string
}