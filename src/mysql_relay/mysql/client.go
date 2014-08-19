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
    return
}

func (self *Client) Handshake() (handshake HandShakePacket, err error) {
    handshake, err = ReadHandShake(self.Conn, self.Buffer[:])
    if err == nil {
        self.Seq = 0
    }
    return
}

func (self *Client) Auth(handshake HandShakePacket) (ret OkPacket, err error) {
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
    cmdPacket := CommandPacket{Command:command}
    err = WritePacketTo(&cmdPacket, self.Conn, self.Buffer[:])
    if err != nil {
        return
    }
    packet, err := ReadGenericResponsePacket(self.Conn, self.Buffer[:])
    if err != nil {
        return
    }
    ret, err = packet.ToOk()
    return
}

func (self *Client)DumpBinlog(cmdBinlogDump ComBinglogDump) (ret <-chan BinlogEventPacket, errs <-chan error) {
    logChan := make(chan BinlogEventPacket)
    errChan := make(chan error)
    ret = logChan
    errs = errChan
    go func() {
        err := DumpBinlogTo(cmdBinlogDump, self.Conn, logChan, self.Buffer[:])
        if err != nil {
            errChan<-err
            close(errChan)
        }
    }()
    return
}
