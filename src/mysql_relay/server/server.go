package server

import (
    "net"
    "sync/atomic"
    "regexp"
    "strings"
    "mysql_relay/util"
    "mysql_relay/mysql"
    "fmt"
    "time"
    "io"
    "io/ioutil"
    "strconv"
)

type Server struct {
    Addr        string
    Peers       map[uint32]Peer
    NextConnId  uint32
    Closed      chan uint32

    Config
}

const PEER_BUFFER_SIZE = 1024
type Peer struct {
    ConnId  uint32
    Server  *Server
    Conn    net.Conn
    Buffer  [PEER_BUFFER_SIZE]byte
}

func (self *Peer) Close() {
    self.Conn.Close()
}

func (self *Peer) RemoteAddr() *net.TCPAddr {
    return self.Conn.RemoteAddr().(*net.TCPAddr)
}

func (self *Peer) RemoteIP() string {
    return self.RemoteAddr().IP.String()
}

func (self *Peer) Auth() (err error) {
    if !self.Server.CheckHost(self.RemoteIP()) {
        errPacket := mysql.ErrPacket {
            ErrorCode: mysql.ER_HOST_NOT_PRIVILEGED,
            SqlState : "",
            ErrorMessage: fmt.Sprintf(mysql.SERVER_ERR_MESSAGES[mysql.ER_HOST_NOT_PRIVILEGED], self.RemoteIP()),
        }
        err = mysql.WritePacketTo(&errPacket, self.Conn, self.Buffer[:])
        return
    }

    fmt.Println(self.RemoteIP())
    handshake := mysql.BuildHandShakePacket(self.Server.Config.Server.Version, self.ConnId)
    err = mysql.WritePacketTo(&handshake, self.Conn, self.Buffer[:])
    fmt.Println(handshake)
    if err != nil {
        return
    }
    var auth mysql.AuthPacket
    err = mysql.ReadPacketFrom(&auth, self.Conn, self.Buffer[:])
    if err != nil {
        return
    }
    user, ok := self.Server.Config.Users[auth.Username]
    authed := false
    if ok {
        if hostContains(user.Host, self.RemoteIP()) {
            hash2 := mysql.Hash2(user.Password)
            authed = mysql.CheckAuth(handshake.AuthString, hash2[:], []byte(auth.AuthResponse))
        }
    }
    fmt.Println(authed)
    if authed {
        okPacket := mysql.OkPacket{}
        okPacket.PacketSeq = auth.PacketSeq + 1
        err = mysql.WritePacketTo(&okPacket, self.Conn, self.Buffer[:])
    } else {
        errPacket := mysql.BuildErrPacket(mysql.ER_ACCESS_DENIED_ERROR, auth.Username, self.RemoteIP(), "yes");
        errPacket.PacketSeq = auth.PacketSeq + 1
        err = mysql.WritePacketTo(&errPacket, self.Conn, self.Buffer[:])
        if err == nil {
            err = errPacket.ToError()
        }
    }
    return
}

func (self *Server) CheckHost(host string) bool {
    for _, user := range self.Config.Users {
        if hostContains(user.Host, host) {
            return true
        }
    }
    return false
}

func hostContains(sNet string, sHost string) bool {
    _, ipNet, err := net.ParseCIDR(sNet)
    if err != nil {
        return false
    }
    return ipNet.Contains(net.ParseIP(sHost))
}

func (self *Server) Run() (err error) {
    var listen net.Listener
    var conn   net.Conn
    listen, err = net.Listen("tcp", self.Config.Server.Addr)
    if err != nil {
        return
    }
    defer listen.Close()
    var delayer util.AutoDelayer
    self.Closed = make(chan uint32)
    self.Peers = make(map[uint32]Peer)
    go func() {
        for closed := range self.Closed {
            delete(self.Peers, closed)
        }
    }()
    for {
        conn, err = listen.Accept()
        if err != nil {
            if isTemporaryNetError(err) {
                delayer.Delay()
                continue
            } else {
                return
            }
        } else {
            delayer.Reset()
        }
        connId := self.GetNextConnId()
        self.Peers[connId] = Peer{ConnId:connId, Conn:conn, Server:self}
        go self.handle(self.Peers[connId])
    }
}

func (self *Server) handle(peer Peer) {
    defer func() {
        peer.Close()
        self.Closed<-peer.ConnId
    }()
    err := peer.Auth()
    if err != nil {
        return
    }
    cmdPacket := mysql.BaseCommandPacket{}
    for {
        err = mysql.ReadPacketFrom(&cmdPacket, peer.Conn, peer.Buffer[:])
        if err != nil {
            return
        }
        switch cmdPacket.Type {
        case mysql.COM_QUERY:
            peer.onCmdQuery(&cmdPacket)
        case mysql.COM_BINLOG_DUMP:
            peer.onCmdBinlogDump(&cmdPacket)
        case mysql.COM_PING:
            peer.onCmdPing(&cmdPacket)
        default:
            peer.onCmdUnknown(&cmdPacket)
        }
        reader := cmdPacket.GetReader(peer.Conn, peer.Buffer[:])
        io.Copy(ioutil.Discard, &reader)
    }
}

func (peer *Peer) onCmdQuery(cmdPacket *mysql.BaseCommandPacket) (err error) {
    query := string(peer.Buffer[1:cmdPacket.PacketLength])
    fmt.Println(query)
    query = NormalizeSpecialQuery(query)
    switch query {
    case "select @@version_comment limit 1":
        return onSqlVersionComment(peer)
    case "show variables like 'server_id'":
        return onSqlServerId(peer)
    case "show variables like 'server_uuid'":
        return onSqlServerUuid(peer)
    case "select unix_timestamp()":
        return onSqlUnixTimestamp(peer)
    case "select version()":
        return onSqlVersion(peer)
    case "set @master_binlog_checksum='none'":
        return peer.SendOk(cmdPacket.PacketSeq + 1)
    }
    errPacket := mysql.BuildErrPacket(mysql.ER_NOT_SUPPORTED_YET, "this")
    errPacket.PacketSeq = cmdPacket.PacketSeq + 1
    err = mysql.WritePacketTo(&errPacket, peer.Conn, peer.Buffer[:])
    return
}

func (peer *Peer) SendOk(seq byte) (err error) {
    okPacket := mysql.OkPacket{}
    okPacket.PacketSeq = seq
    err = mysql.WritePacketTo(&okPacket, peer.Conn, peer.Buffer[:])
    return
}

func onSqlVersionComment(peer *Peer) (err error) {
    //select @@version_comment limit 1
    cols := [1]mysql.ColumnDefinition{
        {
            Catalog: "def",
            Name: "@@version_comment",
            Decimals: 127,
            CharacterSet: mysql.LATIN1_SWEDISH_CI,
            Type: mysql.MYSQL_TYPE_VAR_STRING,
            ColumnLength: 28,
        },
    }
    cursor := mysql.Cursor {
        Columns: cols[:],
        ReadWriter: peer.Conn,
        Buffer: peer.Buffer[:],
    }
    err = cursor.BeginWrite()
    if err != nil {
        return
    }
    cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
        {Value: mysql.VERSION_COMMENT, IsNull: false} ,
    }}
    close(cursor.Rows)
    return
}

func onSqlVersion(peer *Peer) (err error) {
    //select version()
    cols := [1]mysql.ColumnDefinition{
        {
            Catalog: "def",
            Name: "version()",
            Decimals: 127,
            CharacterSet: mysql.LATIN1_SWEDISH_CI,
            Type: mysql.MYSQL_TYPE_VAR_STRING,
            ColumnLength: 28,
        },
    }
    cursor := mysql.Cursor {
        Columns: cols[:],
        ReadWriter: peer.Conn,
        Buffer: peer.Buffer[:],
    }
    err = cursor.BeginWrite()
    if err != nil {
        return
    }
    cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
        {Value: "5.6.19-log", IsNull: false} ,
    }}
    close(cursor.Rows)
    return
}

func onSqlServerId(peer *Peer) (err error) {
    //show variables like 'server_id'
    cols := [2]mysql.ColumnDefinition{
        {
            Catalog: "def",
            Schema: "information_schema",
            Table: "VARIABLES",
            OrgTable: "VARIABLES",
            Name: "Variable_name",
            OrgName: "VARIABLE_NAME",
            Decimals: 0,
            CharacterSet: mysql.LATIN1_SWEDISH_CI,
            Type: mysql.MYSQL_TYPE_VAR_STRING,
            Flags: mysql.SERVER_STATUS_IN_TRANS,
            ColumnLength: 192,
        },
        {
            Catalog: "def",
            Schema: "information_schema",
            Table: "VARIABLES",
            OrgTable: "VARIABLES",
            Name: "Variable_value",
            OrgName: "VARIABLE_VALUE",
            Decimals: 0,
            CharacterSet: mysql.LATIN1_SWEDISH_CI,
            Type: mysql.MYSQL_TYPE_VAR_STRING,
            Flags: 0,
            ColumnLength: 3072,
        },
    }
    cursor := mysql.Cursor {
        Columns: cols[:],
        ReadWriter: peer.Conn,
        Buffer: peer.Buffer[:],
    }
    err = cursor.BeginWrite()
    if err != nil {
        return
    }
    cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
        {Value: "server_id", IsNull: false} ,
        {Value: "2",         IsNull: false} ,
    }}
    close(cursor.Rows)
    return
}

func onSqlServerUuid(peer *Peer) (err error) {
    //show variables like 'server_uuid'
    cols := [2]mysql.ColumnDefinition{
        {
            Catalog: "def",
            Schema: "information_schema",
            Table: "VARIABLES",
            OrgTable: "VARIABLES",
            Name: "Variable_name",
            OrgName: "VARIABLE_NAME",
            Decimals: 0,
            CharacterSet: mysql.LATIN1_SWEDISH_CI,
            Type: mysql.MYSQL_TYPE_VAR_STRING,
            Flags: mysql.SERVER_STATUS_IN_TRANS,
            ColumnLength: 192,
        },
        {
            Catalog: "def",
            Schema: "information_schema",
            Table: "VARIABLES",
            OrgTable: "VARIABLES",
            Name: "Variable_value",
            OrgName: "VARIABLE_VALUE",
            Decimals: 0,
            CharacterSet: mysql.LATIN1_SWEDISH_CI,
            Type: mysql.MYSQL_TYPE_VAR_STRING,
            Flags: 0,
            ColumnLength: 3072,
        },
    }
    cursor := mysql.Cursor {
        Columns: cols[:],
        ReadWriter: peer.Conn,
        Buffer: peer.Buffer[:],
    }
    err = cursor.BeginWrite()
    if err != nil {
        return
    }
    cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
        {Value: "server_uuid", IsNull: false} ,
        {Value: "a2d605d4-67df-11e4-bfdd-08002792fa42",         IsNull: false} ,
    }}
    close(cursor.Rows)
    return
}

func onSqlUnixTimestamp(peer *Peer) (err error) {
    //select unix_timestamp()
    cols := [1]mysql.ColumnDefinition{
        {
            Catalog: "def",
            Name: "unix_timestamp()",
            Decimals: 127,
            CharacterSet: mysql.LATIN1_SWEDISH_CI,
            Type: mysql.MYSQL_TYPE_LONGLONG,
            ColumnLength: 11,
        },
    }
    cursor := mysql.Cursor {
        Columns: cols[:],
        ReadWriter: peer.Conn,
        Buffer: peer.Buffer[:],
    }
    err = cursor.BeginWrite()
    if err != nil {
        return
    }
    cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
        {Value: strconv.FormatInt(time.Now().Unix(), 10), IsNull: false} ,
    }}
    close(cursor.Rows)
    return
}

func (peer *Peer) onCmdBinlogDump(cmdPacket *mysql.BaseCommandPacket) (err error) {
    dump := mysql.ComBinglogDump{}
    dump.FromBuffer(peer.Buffer[:cmdPacket.PacketLength])
    fmt.Println(dump)
    return
}

func (peer *Peer) onCmdPing(cmdPacket *mysql.BaseCommandPacket) (err error) {
    return
}

func (peer *Peer) onCmdUnknown(cmdPacket *mysql.BaseCommandPacket) (err error) {
    errPacket := mysql.BuildErrPacket(mysql.ER_UNKNOWN_COM_ERROR)
    errPacket.PacketSeq = cmdPacket.PacketSeq + 1
    err = mysql.WritePacketTo(&errPacket, peer.Conn, peer.Buffer[:])
    return
}

func isTemporaryNetError(err error) bool {
    if ne, ok := err.(net.Error); ok && ne.Temporary() {
        return true
    }
    return false
}


func (self *Server) GetNextConnId() uint32 {
    return atomic.AddUint32(&self.NextConnId, 1)
}

var normalizeRegEx, _=regexp.Compile("[ ]*([ ~!%^&*()=+<>,/.-])[ ]*")
func NormalizeSpecialQuery(query string) string {
    return strings.ToLower(normalizeRegEx.ReplaceAllString(query, "$1"))
}

/*
sql slave executed:

SELECT UNIX_TIMESTAMP();
SHOW VARIABLES LIKE 'SERVER_ID';
SET @master_heartbeat_period= 1799999979520;
SET @master_binlog_checksum= @@global.binlog_checksum;
SELECT @master_binlog_checksum;
SELECT @@GLOBAL.GTID_MODE;
SHOW VARIABLES LIKE 'SERVER_UUID';
 =>
select unix_timestamp()
show variables like 'server_id'
set @master_heartbeat_period=1799999979520
set @master_binlog_checksum=@@global.binlog_checksum
select @master_binlog_checksum
select @@global.gtid_mode
show variables like 'server_uuid'

#HY000Unknown system variable 'binlog_checksum'
#HY000Unknown system variable 'GTID_MODE'

SELECT VERSION()
*/
