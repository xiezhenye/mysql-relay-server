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
)

type Server struct {
    Addr        string
    Peers       map[uint32]Peer
    Version     string
    NextConnId  uint32
    Closed      chan uint32
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

func (self *Peer) Auth() (err error) {
    handshake := mysql.BuildHandShakePacket(self.Server.Version, self.ConnId)
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
    fmt.Println(auth)
    hash2 := mysql.Hash2("12345678")
    authed := mysql.CheckAuth(handshake.AuthString, hash2[:], []byte(auth.AuthResponse))
    fmt.Println(authed)
    if authed {
        ok := mysql.OkPacket{}
        ok.PacketSeq = auth.PacketSeq + 1
        err = mysql.WritePacketTo(&ok, self.Conn, self.Buffer[:])
    } else {
        
    }
    return
}

func (self *Server) Run() (err error) {
    var listen net.Listener
    var conn   net.Conn
    listen, err = net.Listen("tcp", self.Addr)
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
    time.Sleep(60 * time.Second)
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

SELECT UNIX_TIMESTAMP()
SHOW VARIABLES LIKE 'SERVER_ID'
SET @master_heartbeat_period= 1799999979520
SET @master_binlog_checksum= @@global.binlog_checksum
SELECT @master_binlog_checksum
SELECT @@GLOBAL.GTID_MODE
SHOW VARIABLES LIKE 'SERVER_UUID'
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
*/
