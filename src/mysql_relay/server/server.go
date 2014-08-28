package server

import (
    "net"
    "sync/atomic"
    "regexp"
    "strings"
    "mysql_relay/util"
)

type Server struct {
    Addr        string
    Peers       map[uint32]Peer
    NextConnId  uint32
    Closed  chan uint32
}

type Peer struct {
    ConnId  uint32
    Conn    net.Conn
}

func (self *Peer) Close() {
    self.Conn.Close()
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
    go func() {
        for closed := range self.Closed {
            delete(self.Peers[closed])
        }
    }
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
        self.Peers[connId] = Peer{ConnId:self.GetNextConnId(), Conn:conn}
        go self.Handle(&self.Peers[connId])
    }
}

func (self *Server) Handle(peer *Peer) {
    defer func() {
        peer.Close()
        self.Closed<-peer.ConnId
    }
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