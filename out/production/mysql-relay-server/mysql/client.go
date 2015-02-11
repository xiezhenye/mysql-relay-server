package mysql

import (
    "net"
//    "fmt"
)

const (
    CLIENT_BUFFER_SIZE = 1024
)

type Client struct {
    ServerAddr string
    Username   string
    Password   string
    ServerId   uint32
    Seq        byte
    Conn       net.Conn
    Buffer     [CLIENT_BUFFER_SIZE]byte
}

func (self *Client) Connect() (err error){
    self.Conn, err = net.Dial("tcp", self.ServerAddr)
    if err != nil {
        return
    }
    handshake, err := ReadHandShake(self.Conn, self.Buffer[:])
    if err != nil {
        return
    } else {
        self.Seq = 0
    }
    authPacket, err := buildAuthPacket(self.Username, self.Password, handshake)
    if err != nil {
        return
    }
    
    ret, err := SendAuth(authPacket, self.Conn, self.Buffer[:])
    if err == nil {
        self.Seq = ret.PacketSeq
    }
    return
}


func (self *Client) Command(command Command) (ret OkPacket, err error) {
    ret, err = SendCommand(command, self.Conn, self.Buffer[:])
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
    _, err = self.Command(&QueryCommand{Query:"SET @master_binlog_checksum='NONE';"})
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
