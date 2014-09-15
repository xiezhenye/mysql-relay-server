package mysql

import (
    "encoding/binary"
    "io"
    "fmt"
)

type PacketHeader struct {
    PacketSeq    byte
    PacketLength uint32
}

type PayloadPacket struct {
    PacketHeader
    BodyLength   int
    Pos          int
}

func (self *PacketHeader) FromUint32(n uint32) {
    self.PacketLength = n & 0x00ffffff
    self.PacketSeq    = uint8(n >> 24)    
}

func (self *PacketHeader) ToUint32() uint32 {
    return (self.PacketLength & 0x00ffffff) | (uint32(self.PacketSeq) << 24)    
}

func (self *PacketHeader) GetHeader() *PacketHeader {
    return self
}

type Packet interface {
    GetHeader() *PacketHeader
}

type OutputPacket interface {
    Outputable
    Packet
}

// packet header has bean already sent. ToBuffer should write the packet body from buffer
// buffer should begin after packet header
type Outputable interface {
    ToBuffer(buffer []byte) (int, error)
}

type InputPacket interface {
    Inputable
    Packet
}

// packet header has bean already read. FromBuffer should read the packet body from buffer
// buffer should begin after packet header
type Inputable interface {
    FromBuffer(buffer []byte) (int, error)
}

func ReadPacketFrom(packet InputPacket, reader io.Reader, buffer []byte) (err error) {
    header := packet.GetHeader()
    *header, err = ReadPacketHeader(reader)
    if err != nil {
        return err
    }
    err = ReadPacket(*header, reader, buffer)
    if err != nil {
        return err
    }
    _, err = packet.FromBuffer(buffer)
    return
}

func WritePacketTo(packet OutputPacket, writer io.Writer, buffer []byte) (err error) {
    writen, err := packet.ToBuffer(buffer[4:])
    if err != nil {
        return err
    }
    packet.GetHeader().PacketLength = uint32(writen)
    uint32Header := packet.GetHeader().ToUint32()
    ENDIAN.PutUint32(buffer, uint32Header)
    //err = binary.Write(writer, ENDIAN, uint32Header)
    if err != nil {
        return
    }
    _, err = writer.Write(buffer[:writen+4])
    return
}

func ReadPacket(header PacketHeader, reader io.Reader, buffer []byte) (err error) {
    var bytesRead int
    if int(header.PacketLength) <= len(buffer) {
        bytesRead, err = reader.Read(buffer[0:int(header.PacketLength)])
        if err != nil {
           return 
        }
        if int(header.PacketLength) != bytesRead {
            err = BYTES_READ_NOT_CORRECT
            return
        }
    } else {
        bytesRead, err = reader.Read(buffer)
        if err != nil {
           return 
        }
        if len(buffer) != bytesRead {
            err = BYTES_READ_NOT_CORRECT
            return    
        }
    }
    return
}

func ReadPacketHeader(reader io.Reader) (header PacketHeader, err error) {
    var t uint32
    err = binary.Read(reader, ENDIAN, &t)
    if err != nil {
        return
    }
    header.FromUint32(t)
    return
}

type NullString string

func (self *NullString) FromBuffer(buffer []byte) (int, error) {
    var i int
    var b byte
    for i, b = range(buffer) {
        if b == '\x00' {
            *self = NullString(buffer[:i])
            return i+1, nil
        }
    }
    return 0, BUFFER_NOT_SUFFICIENT
}

func (self *NullString) ToBuffer(buffer []byte) (int, error) {
    copy(buffer, []byte(*self))
    buffer[len(*self)] = '\x00'
    return len(*self) + 1, nil
}

type LenencInt uint64
func (self *LenencInt) Size() int {
    n := uint64(*self)
    if n < 251 {
        return 1
    }
    if n <= 0xffff {
        return 3
    }
    if n <= 0xffffff {
        return 4
    }
    return 9
}
func (self *LenencInt) FromBuffer(buffer []byte) (read int, err error) {
/*
http://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
To convert a length-encoded integer into its numeric value, check the first byte:
If it is < 0xfb, treat it as a 1-byte integer.
If it is 0xfc, it is followed by a 2-byte integer.
If it is 0xfd, it is followed by a 3-byte integer.
If it is 0xfe, it is followed by a 8-byte integer.
*/
    switch buffer[0] {
    case '\xfb':
        *self = LenencInt(0)
        return 1, LENENCINT_IS_NULL        
    case '\xfc':
        *self = LenencInt(uint64(ENDIAN.Uint16(buffer[1:])))
        return 3, nil
    case '\xfd':
        *self = LenencInt(ENDIAN.Uint32(buffer[0:]) & 0x00ffffff)
        return 4, nil
    case '\xfe':
        *self = LenencInt(ENDIAN.Uint64(buffer[1:]))
        return 9, nil
    case '\xff':
        *self = LenencInt(0)
        return 1, NOT_VALID_LENENCINT
    default: // < '\xfb'
        *self = LenencInt(buffer[0])
        return 1, nil
    }
}

func (self *LenencInt) ToBuffer(buffer []byte) (int, error) {
/*
http://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
To convert a number value into a length-encoded integer:
If the value is < 251, it is stored as a 1-byte integer.
If the value is ≥ 251 and < (2^16), it is stored as fc + 2-byte integer.
If the value is ≥ (2^16) and < (2^24), it is stored as fd + 3-byte integer.
If the value is ≥ (2^24) and < (2^64) it is stored as fe + 8-byte integer.
*/
    n := uint64(*self)
    if n < 251 {
        buffer[0] = byte(n)
        return 1, nil
    }
    if n <= 0xffff {
        buffer[0] = '\xfc'
        ENDIAN.PutUint16(buffer[1:], uint16(n))
        return 3, nil
    }
    if n <= 0xffffff {
        ENDIAN.PutUint32(buffer, uint32(n) | 0xfd000000)
        return 4, nil
    }
    buffer[0] = '\xfe'
    ENDIAN.PutUint64(buffer[1:], uint64(n))
    return 9, nil
}

type LenencString string

func (self *LenencString) FromBuffer(buffer []byte) (read int, err error) {
    var n LenencInt
    read, err = n.FromBuffer(buffer)
    if err != nil {
        return
    }
    *self = LenencString(buffer[read:read + int(n)])
    read+= len(*self)
    return
}

func (self *LenencString) ToBuffer(buffer []byte) (writen int, err error) {
    n := LenencInt(len(*self))
    writen, err = n.ToBuffer(buffer)
    if err != nil {
        return
    }
    copy(buffer[writen:], []byte(*self))
    writen+= len(*self)
    return
}

type GenericResponsePacket struct {
    PacketHeader
    PacketType  byte
    Buffer      []byte
}

func (self GenericResponsePacket) ToOk() (ret OkPacket, err error) {
    switch self.PacketType {
    case GRP_OK:
        ret.PacketHeader = self.PacketHeader
        _, err = ret.FromBuffer(self.Buffer)
    case GRP_ERR:
        var errPacket ErrPacket
        errPacket, err = self.ToErr()
        if err != nil {
            return
        }
        err = errPacket.ToError()
    case GRP_EOF:
        var eofPacket EofPacket
        eofPacket, err = self.ToEof()
        if err != nil {
            return
        }
        err = eofPacket.ToError()
    default:
        err = NOT_GENERIC_RESPONSE_PACKET
    }
    return
}

func (self GenericResponsePacket) ToErr() (ret ErrPacket, err error) {
    ret.PacketHeader = self.PacketHeader
    if self.PacketType != GRP_ERR {
        err = NOT_SUCH_PACET_TYPE
        return
    }
    _, err = ret.FromBuffer(self.Buffer)
    return
}

func (self GenericResponsePacket) ToEof() (ret EofPacket, err error) {
    ret.PacketHeader = self.PacketHeader
    if self.PacketType != GRP_EOF {
        err = NOT_SUCH_PACET_TYPE
        return
    }
    _, err = ret.FromBuffer(self.Buffer)
    return
}

func ReadGenericResponsePacket(reader io.Reader, buffer []byte) (ret GenericResponsePacket, err error) {
    ret.PacketHeader, err = ReadPacketHeader(reader)
    if err != nil {
        return
    }
    err = ReadPacket(ret.PacketHeader, reader, buffer)
    if err != nil {
        return
    }
    ret.PacketType = buffer[0]
    ret.Buffer = buffer
    return
}

type OkPacket struct {
    PayloadPacket
// http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    AffectedRows uint64
    LastInsertId uint64
    StatusFlags  uint16
    Warnings     uint16
}

func (self *OkPacket) FromBuffer(buffer []byte) (read int, err error) {
    if buffer[0] != GRP_OK {
        err = NOT_OK_PACKET
        return
    }
    var n int
    var leInt LenencInt
    p := 1
    n, _ = leInt.FromBuffer(buffer[p:])
    self.AffectedRows = uint64(leInt)
    p+= n
    n, _ = leInt.FromBuffer(buffer[p:])
    self.LastInsertId = uint64(leInt)
    p+= n
    self.StatusFlags = ENDIAN.Uint16(buffer[p:])
    p+= 2
    self.Warnings = ENDIAN.Uint16(buffer[p:])
    p+= 2
    self.BodyLength = int(self.PacketLength) -  p
    read = p
    return   
}

func (self *OkPacket) ToBuffer(buffer []byte) (writen int, err error) {
    buffer[0] = GRP_OK
    p := 1
    n := 0
    leInt := LenencInt(self.AffectedRows)
    n, _ = leInt.ToBuffer(buffer[p:])
    p+= n
    leInt = LenencInt(self.LastInsertId)
    n, _ = leInt.ToBuffer(buffer[p:])
    p+= n
    ENDIAN.PutUint16(buffer[p:], self.StatusFlags)
    p+= 2
    ENDIAN.PutUint16(buffer[p:], self.Warnings)
    p+= 2
    writen = p
    return
}

type PayloadReader struct {
    reader  io.Reader
    header  *PayloadPacket
    firstBuffer  []byte
}

func (self *PayloadPacket) Reset(skipHeader bool) error {
    if self.Pos > int(self.PacketLength - 1) {
        return SEEK_AFTER_READ
    }
    if skipHeader {
        self.Pos = int(self.PacketLength) - self.BodyLength
    } else {
        self.Pos = 0
    }
    return nil
}

func (self *PayloadPacket) GetReader(reader io.Reader, buffer []byte) PayloadReader {
    return PayloadReader{
        reader: reader,
        header: self,
        firstBuffer: buffer,
    }
}

func (self *PayloadReader) Read(buffer []byte) (n int, err error) {
    if self.header.Pos >= int(self.header.PacketLength) {
        return 0, io.EOF
    }
    var copied int
    var srcEnd int
    var destEnd int
    for destEnd < len(buffer) {
        if self.header.Pos < len(self.firstBuffer) {
            // has remained src buffer
            srcEnd = int(self.header.PacketLength)
            if srcEnd > len(self.firstBuffer) {
                srcEnd = len(self.firstBuffer)
            }
            copied = copy(buffer[destEnd:], self.firstBuffer[self.header.Pos:srcEnd])
            self.header.Pos+= copied
            destEnd+= copied
        } else {
            // read data
            srcRem := int(self.header.PacketLength) - self.header.Pos
            destRem := len(buffer) - destEnd
            rem := srcRem
            if srcRem > destRem {
                rem = destRem
            }
            // directly read data to dest buffer
            copied, err = self.reader.Read(buffer[destEnd:destEnd + rem])
            if err != nil {
                return destEnd, err
            }
            destEnd+= copied
            self.header.Pos+= copied
        }
        if self.header.Pos >= int(self.header.PacketLength) {
            // reach the end
            return destEnd, io.EOF
        }
    }
    return destEnd, nil
}

type ErrPacket struct {
    PacketHeader
//http://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
    ErrorCode     uint16
    SqlState      string
    ErrorMessage  string
}

func BuildErrPacket(code uint16, params... interface{}) (ret ErrPacket) {
    ret = ErrPacket {
        ErrorCode: code,
        SqlState: SERVER_SQL_STATES[code],
        ErrorMessage: fmt.Sprintf(SERVER_ERR_MESSAGES[code], params...),
    }
    return
}

func (self *ErrPacket) FromBuffer(buffer []byte) (read int, err error) {
    if buffer[0] != GRP_ERR {
        err = NOT_ERR_PACKET
        return
    }
    self.ErrorCode = ENDIAN.Uint16(buffer[1:])
    self.SqlState = string(buffer[4:8])
    self.ErrorMessage = string(buffer[9:self.PacketLength])
    read = int(self.PacketLength)
    return
}

func (self *ErrPacket) ToBuffer(buffer[]byte) (writen int, err error) {
    buffer[0] = GRP_ERR
    p := 1
    fmt.Println(*self)
    ENDIAN.PutUint16(buffer[1:], self.ErrorCode)
    p+= 2
    if self.SqlState != "" {
        if len(self.SqlState) != 5 {
            //
        }
        buffer[p] = '#'
        p+= 1
        copy(buffer[p:], []byte(self.SqlState))
        p+= len(self.SqlState)
    }
    copy(buffer[p:], []byte(self.ErrorMessage))
    p+= len(self.ErrorMessage)
    writen = p
    return
}

func (self ErrPacket) ToError() (err error) {
    err = Error{int(self.ErrorCode)+100000, "("+self.SqlState+") "+self.ErrorMessage}
    return
}

func (self ErrPacket) Error() string {
    return self.ToError().Error()
}
type EofPacket struct {
    PacketHeader
//http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
    WarningCount uint16
    StatusFlags  uint16
}

func (self *EofPacket) FromBuffer(buffer []byte) (read int, err error) {
    if buffer[0] != GRP_EOF {
        err = NOT_EOF_PACKET
        return
    }
    self.WarningCount = ENDIAN.Uint16(buffer[1:])
    self.StatusFlags = ENDIAN.Uint16(buffer[3:])
    read = 5
    return
}

func (self *EofPacket) ToBuffer(buffer []byte) (writen int, err error) {
    buffer[0] = GRP_EOF
    ENDIAN.PutUint16(buffer[1:], self.WarningCount)
    ENDIAN.PutUint16(buffer[3:], self.StatusFlags)
    writen = 5
    return
}

func (self EofPacket) ToError() (err error) {
    err = Error{200000, fmt.Sprintf("%d, %d", self.WarningCount, self.StatusFlags)}
    return
}
