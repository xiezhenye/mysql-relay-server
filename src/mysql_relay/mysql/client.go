package mysql

import (
    "net"
    //"fmt"
)

const (
    BUFFER_SIZE = 1024
)

const (
    RELAY_CLIENT_CAP = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION
)


type Client struct {
    ServerAddr string
    Username   string
    Password   string
    ServerId   uint32
    Seq        byte
    Conn       net.Conn
    Buffer     [BUFFER_SIZE]byte
}

func (self *Client) Connect() (err error){
    self.Conn, err = net.Dial("tcp", self.ServerAddr)
    if err != nil {
        return
    }
    handshake, err := self.handshake()
    if err != nil {
        return
    }
    _, err = self.auth(handshake)
    if err != nil {
        return
    }
    return
}

func (self *Client) handshake() (handshake HandShakePacket, err error) {
    handshake, err = ReadHandShake(self.Conn, self.Buffer[:])
    if err == nil {
        self.Seq = 0
    }
    return
}

func (self *Client) auth(handshake HandShakePacket) (ret OkPacket, err error) {
    authPacket, err := buildAuthPacket(self.Username, self.Password, handshake)
    if err != nil {
        return
    }
    ret, err = Auth(authPacket, self.Conn, self.Buffer[:])
    if err == nil {
        self.Seq = ret.PacketSeq
    }
    return
}

func (self *Client) Command(command Command) (ret OkPacket, err error) {
    ret, err = ExecCommand(command, self.Conn, self.Buffer[:])
    return
}


type BinlogEventStream struct {
    ret  chan BinlogEventPacket
    errs chan error
    canRead chan struct{}
}

func (self *BinlogEventStream) GetChan() <-chan BinlogEventPacket {
    return self.ret
}

func (self *BinlogEventStream) Continue() {
    self.canRead<-struct{}{}
}

func (self *BinlogEventStream) GetError() error {
    err, _ := <- self.errs
    return err
}

func (self *Client)DumpBinlog(cmdBinlogDump ComBinglogDump) (ret BinlogEventStream) {
    var err error
    _, err = self.Command(&QueryCommand{"SET @master_binlog_checksum='NONE';"})
    if err != nil {
        ret.errs<-err
        close(ret.errs)
        close(ret.canRead)
        return
    }
    ret.ret  = make(chan BinlogEventPacket)
    ret.errs = make(chan error)
    ret.canRead = make(chan struct{})
    go func() {
        err := DumpBinlogTo(cmdBinlogDump, self.Conn, ret.canRead, ret.ret, self.Buffer[:])
        if err != nil {
            ret.errs<-err
        }
        close(ret.errs)
        close(ret.canRead)
    }()
    return
}
