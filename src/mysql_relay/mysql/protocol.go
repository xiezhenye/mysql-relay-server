package mysql

import (
    "errors"
    "encoding/binary"
    "io"
    "crypto/sha1"
    "fmt"
)

const (
    CLIENT_LONG_PASSWORD                  = 0x00000001
    CLIENT_FOUND_ROWS                     = 0x00000002
    CLIENT_LONG_FLAG                      = 0x00000004
    CLIENT_CONNECT_WITH_DB                = 0x00000008
    CLIENT_NO_SCHEMA                      = 0x00000010
    CLIENT_COMPRESS                       = 0x00000020
    CLIENT_ODBC                           = 0x00000040
    CLIENT_LOCAL_FILES                    = 0x00000080
    CLIENT_IGNORE_SPACE                   = 0x00000100
    CLIENT_PROTOCOL_41                    = 0x00000200
    CLIENT_INTERACTIVE                    = 0x00000400
    CLIENT_SSL                            = 0x00000800
    CLIENT_IGNORE_SIGPIPE                 = 0x00001000
    CLIENT_TRANSACTIONS                   = 0x00002000
    CLIENT_RESERVED                       = 0x00004000
    CLIENT_SECURE_CONNECTION              = 0x00008000
    CLIENT_MULTI_STATEMENTS               = 0x00010000
    CLIENT_MULTI_RESULTS                  = 0x00020000
    CLIENT_PS_MULTI_RESULTS               = 0x00040000
    CLIENT_PLUGIN_AUTH                    = 0x00080000
    CLIENT_CONNECT_ATTRS                  = 0x00100000
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
)
var (
    ENDIAN = binary.LittleEndian
)
const (
    GRP_OK  byte = '\x00'
    GRP_ERR byte = '\xff'
    GRP_EOF byte = '\xfe'
)
const (
    SERVER_STATUS_IN_TRANS uint16 = 0x0001	
    SERVER_STATUS_AUTOCOMMIT = 0x0002	
    SERVER_MORE_RESULTS_EXISTS = 0x0008
    SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010
    SERVER_STATUS_NO_INDEX_USED = 0x0020
    SERVER_STATUS_CURSOR_EXISTS = 0x0040
    SERVER_STATUS_LAST_ROW_SENT = 0x0080
    SERVER_STATUS_DB_DROPPED = 0x0100
    SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200
    SERVER_STATUS_METADATA_CHANGED = 0x0400
    SERVER_QUERY_WAS_SLOW = 0x0800
    SERVER_PS_OUT_PARAMS = 0x1000
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

type Outputable interface {
    ToBuffer(buffer []byte) (ret []byte, err error)
}

type InputPacket interface {
    Inputable
    Packet
}

type Inputable interface {
    FromBuffer(buffer []byte) error
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
    err = packet.FromBuffer(buffer)
    return
}

func WritePacketTo(packet OutputPacket, writer io.Writer, buffer []byte) (err error) {
    bufRet, err := packet.ToBuffer(buffer)
    if err != nil {
        return err
    }
    packet.GetHeader().PacketLength = uint32(len(bufRet))
    uint32Header := packet.GetHeader().ToUint32()
    err = binary.Write(writer, ENDIAN, uint32Header)
    if err != nil {
        return
    }
    _, err = writer.Write(bufRet)
    return
}

type HandShakePacket struct {
    PacketHeader
    ProtoVer        byte
    ServerVer       string
    ConnId          uint32
    CharacterSet    byte
    StatusFlags     uint16
    CapabilityFlags uint32
    AuthString      string
    AuthPluginName  string
}

type AuthPacket struct {
     PacketHeader
/*
http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
4              capability flags, CLIENT_PROTOCOL_41 always set
4              max-packet size
1              character set
string[23]     reserved (all [0])
string[NUL]    username
  if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
lenenc-int     length of auth-response
string[n]      auth-response
  } else if capabilities & CLIENT_SECURE_CONNECTION {
1              length of auth-response
string[n]      auth-response
  } else {
string[NUL]    auth-response
  }
  if capabilities & CLIENT_CONNECT_WITH_DB {
string[NUL]    database
  }
  if capabilities & CLIENT_PLUGIN_AUTH {
string[NUL]    auth plugin name
  }
*/  
    //CLIENT_SECURE_CONNECTION only
    CapabilityFlags     uint32
    MaxPacketSize       uint32
    CharacterSet        byte
    //[23]byte
    Username            string
    AuthResponse        string
    Database            string
    AuthPluginName      string
}

func (self *HandShakePacket) FromBuffer(buffer []byte) error {
    var p int
    self.ProtoVer = uint8(buffer[0])
    p = 1
    self.ServerVer = nullString(buffer[p:self.PacketLength])
    if int(self.PacketLength) - len(self.ServerVer) < 24 {
        return errors.New("bad handshake packet")
    }
    p += len(self.ServerVer)
    if self.ProtoVer == 10 {
        return handShakeV10(buffer[p:self.PacketLength], self)
    } else if self.ProtoVer == 9 {
        return handShakeV9(buffer[p:self.PacketLength], self)
    } else {
        return errors.New("protocol not supported")
    }
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

func ReadHandShake(reader io.Reader, buffer []byte) (handshake HandShakePacket, err error) {
    err = ReadPacketFrom(&handshake, reader, buffer)
    if err != nil {
       return 
    }
    if int(handshake.PacketLength) > len(buffer) {
        err = errors.New("handshake packet too big")
        return
    }
    if len(handshake.AuthString) != 20 {
        err = BAD_HANDSHAKE_PACKET
    }
    if handshake.PacketSeq != 0 {
        err = BAD_HANDSHAKE_PACKET
        return
    }
    return
}

func nullString(buffer []byte) string {
    var i int
    var b byte
    for i, b = range(buffer) {
        if b == '\x00' {
            return string(buffer[:i+1])
        }
    }
    return ""
}

func handShakeV10(buffer []byte, handshake *HandShakePacket) (err error) {
/*
http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
1              [0a] protocol version
string[NUL]    server version
4              connection id
string[8]      auth-plugin-data-part-1
1              [00] filler
2              capability flags (lower 2 bytes)
  if more data in the packet:
1              character set
2              status flags
2              capability flags (upper 2 bytes)
  if capabilities & CLIENT_PLUGIN_AUTH {
1              length of auth-plugin-data
  } else {
1              [00]
  }
string[10]     reserved (all [00])
  if capabilities & CLIENT_SECURE_CONNECTION {
string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
  if capabilities & CLIENT_PLUGIN_AUTH {
string[NUL]    auth-plugin name
  }
*/
    handshake.ConnId = ENDIAN.Uint32(buffer)
    if buffer[12] != '\x00' {
        return BAD_HANDSHAKE_PACKET
    }
    handshake.CapabilityFlags = uint32(ENDIAN.Uint16(buffer[13:]))
    if len(buffer) > 15 {
        handshake.CharacterSet = buffer[15]
        handshake.StatusFlags = ENDIAN.Uint16(buffer[16:])
        handshake.CapabilityFlags += (uint32(ENDIAN.Uint16(buffer[18:])) << 16)
    }
    authPluginDataLength := int(buffer[20])
    if (handshake.CapabilityFlags & CLIENT_PLUGIN_AUTH) == 0 {
        if authPluginDataLength != 0 {
            return BAD_HANDSHAKE_PACKET    
        }
    }
    if (handshake.CapabilityFlags & CLIENT_SECURE_CONNECTION) != 0 {
        copy(buffer[23:31], buffer[4:12])
        handshake.AuthString = string(buffer[23:22+authPluginDataLength])
    }
    if (handshake.CapabilityFlags & CLIENT_PLUGIN_AUTH) != 0 {
        handshake.AuthPluginName = nullString(buffer[23+authPluginDataLength:])
    }
    return
}

func handShakeV9(buffer []byte, handshake *HandShakePacket) (err error) {
/*
http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV9
1              [09] protocol_version
string[NUL]    server_version
4              connection_id
string[NUL]    scramble
*/
    return
}

func (self *AuthPacket) ToBuffer(buffer []byte) (ret []byte, err error) {
    var p int
    if (self.CapabilityFlags & RELAY_CLIENT_CAP) == 0 {
        err = errors.New("server capability not sufficient")
        return
    }
    ENDIAN.PutUint32(buffer[0:], self.CapabilityFlags)
    ENDIAN.PutUint32(buffer[4:], self.MaxPacketSize)
    buffer[5] = self.CharacterSet
    copy(buffer[32:], []byte(self.Username))
    p = 32 + len(self.Username)
    buffer[p] = '\x00'
    p+= 1
    authResponseLength := len(self.AuthResponse)
    buffer[p] = byte(authResponseLength)
    p+= 1
    copy(buffer[p:p+authResponseLength], []byte(self.AuthResponse))
    p+= authResponseLength
    if self.CapabilityFlags & CLIENT_CONNECT_WITH_DB != 0 {
        copy(buffer[p:], []byte(self.Database))
        p+= len(self.Database)
        buffer[p] = '\x00'
        p+= 1
    }
    if self.CapabilityFlags & CLIENT_PLUGIN_AUTH != 0 {
        copy(buffer[p:], []byte(self.AuthPluginName))
        p+= len(self.AuthPluginName)
        buffer[p] = '\x00'
    }
    ret = buffer[0:p]
    return
}

func buildAuthPacket(username string, password string, handshake HandShakePacket) (authPacket AuthPacket, err error) {
    if (handshake.CapabilityFlags & RELAY_CLIENT_CAP) != RELAY_CLIENT_CAP {
        err = errors.New("server capability not sufficient")
        return
    }
    // TODO: check CapabilityFlags
    authPacket.CapabilityFlags = RELAY_CLIENT_CAP //handshake.CapabilityFlags
    authPacket.MaxPacketSize   = 0
    authPacket.CharacterSet    = handshake.CharacterSet
    authPacket.Username        = username
    authPacket.AuthResponse    = authResponse(handshake.AuthString, password)
    authPacket.AuthPluginName  = handshake.AuthPluginName
    
    authPacket.PacketSeq = handshake.PacketSeq + 1
    return
}

func authResponse(authString string, password string) string {
    t1 := sha1.Sum([]byte(password))
    t2 := sha1.Sum(t1[:])
    t3 := sha1.Sum([]byte(authString + string(t2[:])))
    for i := range(t1) {
        t3[i] = t1[i] ^ t3[i]
    }
    return string(t3[:])
}

func getLenencInt(buffer []byte) (uint64, int) {
/*
http://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
To convert a length-encoded integer into its numeric value, check the first byte:
If it is < 0xfb, treat it as a 1-byte integer.
If it is 0xfc, it is followed by a 2-byte integer.
If it is 0xfd, it is followed by a 3-byte integer.
If it is 0xfe, it is followed by a 8-byte integer.
*/
    if buffer[0] < '\xfb' {
        return uint64(buffer[0]), 1
    }
    if buffer[0] == '\xfc' {
        return uint64(ENDIAN.Uint16(buffer[1:])), 3
    }
    if buffer[0] == '\xfd' {
        return uint64(ENDIAN.Uint32(buffer[0:]) & 0x00ffffff), 4
    }
    if buffer[0] == '\xfe' {
        return uint64(ENDIAN.Uint64(buffer[1:])), 9
    }
    // TODO: deal with \xfb ( null ) or \xff
    return 0, 0
}

func putLenencInt(n uint64, buffer []byte) int {
/*
http://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
To convert a number value into a length-encoded integer:
If the value is < 251, it is stored as a 1-byte integer.
If the value is ≥ 251 and < (2^16), it is stored as fc + 2-byte integer.
If the value is ≥ (2^16) and < (2^24), it is stored as fd + 3-byte integer.
If the value is ≥ (2^24) and < (2^64) it is stored as fe + 8-byte integer.
*/
    if n < 251 {
        buffer[0] = byte(n)
        return 1
    }
    if n <= 0xffff {
        buffer[0] = '\xfc'
        ENDIAN.PutUint16(buffer[1:], uint16(n))
        return 3
    }
    if n <= 0xffffff {
        ENDIAN.PutUint32(buffer, uint32(n) | 0xfd000000)
        return 4
    }
    buffer[0] = '\xfe'
    ENDIAN.PutUint64(buffer[1:], n)
    return 9
}

func readLenencInt(reader io.Reader) (ret uint64, n int, err error) {
    var buf [8]byte
    _, err = reader.Read(buf[0:1])
    if err != nil {
        return
    }
    if buf[0] < '\xfb' {
        ret, n = uint64(buf[0]), 1
        return
    }
    if buf[0] == '\xfc' {
        _, err = reader.Read(buf[0:2])
        if err != nil {
            return
        }
        ret, n = uint64(ENDIAN.Uint16(buf[:])), 3
    }
    if buf[0] == '\xfd' {
        _, err = reader.Read(buf[1:4])
        if err != nil {
            return
        }
        ret, n = uint64(ENDIAN.Uint32(buf[:]) & 0x00ffffff), 4
    }
    if buf[0] == '\xfe' {
        _, err = reader.Read(buf[0:8])
        if err != nil {
            return
        }
        ret, n = uint64(ENDIAN.Uint64(buf[:])), 9
    }
    ret, n = 0, 0
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
        err = ret.FromBuffer(self.Buffer)
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
    err = ret.FromBuffer(self.Buffer)
    return
}

func (self GenericResponsePacket) ToEof() (ret EofPacket, err error) {
    ret.PacketHeader = self.PacketHeader
    if self.PacketType != GRP_EOF {
        err = NOT_SUCH_PACET_TYPE
        return
    }
    err = ret.FromBuffer(self.Buffer)
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

func Auth(authPacket AuthPacket, readWriter io.ReadWriter, buffer []byte) (ret OkPacket, err error) {
    err = WritePacketTo(&authPacket, readWriter, buffer)
    packet, err := ReadGenericResponsePacket(readWriter, buffer)
    if err != nil {
        return
    }
    if packet.PacketSeq != authPacket.PacketSeq + 1 {
        err = PACKET_SEQ_NOT_CORRECT
        return
    }
    ret, err = packet.ToOk()
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

func (self *OkPacket) FromBuffer(buffer []byte) (err error) {
    if buffer[0] != GRP_OK {
        err = NOT_OK_PACKET
        return
    }
    var n int
    p := 1
    self.AffectedRows, n = getLenencInt(buffer[p:])
    p+= n
    self.LastInsertId, n = getLenencInt(buffer[p:])
    p+= n
    self.StatusFlags = ENDIAN.Uint16(buffer[p:])
    p+= 2
    self.Warnings = ENDIAN.Uint16(buffer[p:])
    p+= 2
    self.BodyLength = int(self.PacketLength) -  p
    return   
}

type PayloadReader struct {
    reader  io.Reader
    header  *PayloadPacket
    firstBuffer  []byte
}

func (self *PayloadPacket) GetReader(reader io.Reader, buffer []byte, withHeader bool) PayloadReader {
    if withHeader {
        self.Pos = 0
    } else {
        self.Pos = int(self.PacketLength) - self.BodyLength
    }
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

func (self *ErrPacket) FromBuffer(buffer []byte) (err error) {
    if buffer[0] != GRP_ERR {
        err = NOT_ERR_PACKET
        return
    }
    self.ErrorCode = ENDIAN.Uint16(buffer[1:])
    self.SqlState = string(buffer[4:8])
    self.ErrorMessage = string(buffer[9:self.PacketLength])
    return
}

func (self ErrPacket) ToError() (err error) {
    err = Error{int(self.ErrorCode)+100000, "("+self.SqlState+") "+self.ErrorMessage}
    return
}

type EofPacket struct {
    PacketHeader
//http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
    WarningCount uint16
    StatusFlags  uint16
}

func (self *EofPacket) FromBuffer(buffer []byte) (err error) {
    if buffer[0] != GRP_EOF {
        err = NOT_EOF_PACKET
        return
    }
    self.WarningCount = ENDIAN.Uint16(buffer[1:])
    self.StatusFlags = ENDIAN.Uint16(buffer[2:])
    return
}

func (self EofPacket) ToError() (err error) {
    err = Error{200000, fmt.Sprintf("%d, %d", self.WarningCount, self.StatusFlags)}
    return
}
