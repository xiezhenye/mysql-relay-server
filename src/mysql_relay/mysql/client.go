package mysql

import (
	"net"
	//    "fmt"
	//	"bufio"
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
	NetTimeout uint32
	Buffer     [CLIENT_BUFFER_SIZE]byte
}

func (self *Client) Connect() (err error) {
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
