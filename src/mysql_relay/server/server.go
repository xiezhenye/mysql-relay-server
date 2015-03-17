package server

import (
	"fmt"
	"io"
	"io/ioutil"
	"mysql_relay/mysql"
	"mysql_relay/relay"
	"mysql_relay/util"
	"net"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
)

type Server struct {
	Addr       string
	Peers      map[uint32]*Peer
	NextConnId uint32
	Closed     chan uint32
	//
	Config
	Upstreams map[string]*relay.BinlogRelay
}

const PEER_BUFFER_SIZE = 1024

type Peer struct {
	ConnId         uint32
	Server         *Server
	User           string
	Conn           net.Conn
	ClientServerId uint32
	Buffer         [PEER_BUFFER_SIZE]byte
	seq            byte
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
		errPacket := mysql.ErrPacket{
			ErrorCode:    mysql.ER_HOST_NOT_PRIVILEGED,
			SqlState:     "",
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
		self.User = auth.Username
		err = mysql.WritePacketTo(&okPacket, self.Conn, self.Buffer[:])
	} else {
		errPacket := mysql.BuildErrPacket(mysql.ER_ACCESS_DENIED_ERROR, auth.Username, self.RemoteIP(), "yes")
		errPacket.PacketSeq = auth.PacketSeq + 1
		err = mysql.WritePacketTo(&errPacket, self.Conn, self.Buffer[:])
		if err == nil {
			err = errPacket.ToError()
		}
	}
	return
}

func (self *Peer) GetRelay() *relay.BinlogRelay {
	if self.User == "" {
		return nil
	}
	user, ok := self.Server.Config.Users[self.User]
	if !ok {
		return nil
	}
	relay, ok := self.Server.Upstreams[user.Upstream]
	if !ok {
		return nil
	}
	return relay
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

func (self *Server) Init() {
	self.Upstreams = make(map[string]*relay.BinlogRelay)
	self.Closed = make(chan uint32)
	self.Peers = make(map[uint32]*Peer)
}

func (self *Server) StartUpstreams() (err error) {
	for name, upstreamConfig := range self.Config.Upstreams {
		c := mysql.Client{
			ServerAddr: upstreamConfig.ServerAddr,
			Username:   upstreamConfig.Username,
			Password:   upstreamConfig.Password,
			ServerId:   upstreamConfig.ServerId,
		}
		err = c.Connect()
		if err != nil {
			return
		}
		self.Upstreams[name] = new(relay.BinlogRelay)
		self.Upstreams[name].Init(name, c, upstreamConfig.LocalDir, upstreamConfig.StartFile)
		go self.Upstreams[name].Run()
	}
	return
}

func (self *Server) Run() (err error) {
	self.Init()
	err = self.StartUpstreams()
	if err != nil {
		return
	}
	err = self.BeginListen()
	return
}

func (self *Server) BeginListen() (err error) {
	var listen net.Listener
	listen, err = net.Listen("tcp", self.Config.Server.Addr)
	if err != nil {
		return
	}
	defer listen.Close()
	go func() {
		for closed := range self.Closed {
			delete(self.Peers, closed)
		}
	}()
	for {
		var delayer util.AutoDelayer
		var conn net.Conn
		conn, err = listen.Accept()
		if err != nil {
			if isTemporaryNetError(err) {
				delayer.Delay()
				continue
			} else {
				return
				// TODO: cleanup goroutines
			}
		} else {
			delayer.Reset()
		}
		connId := self.GetNextConnId()
		self.Peers[connId] = &Peer{ConnId: connId, Conn: conn, Server: self}
		go self.handle(self.Peers[connId])
	}
}

func (self *Server) handle(peer *Peer) {
	defer func() {
		peer.Close()
		self.Closed <- peer.ConnId
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
		fmt.Println("Command: " + mysql.CommandNames[cmdPacket.Type])
		switch cmdPacket.Type {
		case mysql.COM_QUERY:
			err = peer.onCmdQuery(&cmdPacket)
		case mysql.COM_BINLOG_DUMP:
			err = peer.onCmdBinlogDump(&cmdPacket)
		case mysql.COM_PING:
			err = peer.onCmdPing(&cmdPacket)
		case mysql.COM_QUIT:
			err = peer.onCmdQuit(&cmdPacket)
			break
		case mysql.COM_REGISTER_SLAVE:
			err = peer.onCmdRegisterSlave(&cmdPacket)
		default:
			err = peer.onCmdUnknown(&cmdPacket)
		}
		if err != nil {
			fmt.Println(err.Error())
		}
		reader := cmdPacket.GetReader(peer.Conn, peer.Buffer[:])
		io.Copy(ioutil.Discard, &reader)
	}
}

func (peer *Peer) SendOk(seq byte) (err error) {
	okPacket := mysql.OkPacket{}
	okPacket.PacketSeq = seq
	err = mysql.WritePacketTo(&okPacket, peer.Conn, peer.Buffer[:])
	return
}

func (peer *Peer) onCmdRegisterSlave(cmdPacket *mysql.BaseCommandPacket) (err error) {
	regSlave := mysql.ComRegisterSlave{}
	regSlave.FromBuffer(peer.Buffer[:cmdPacket.PacketLength])
	peer.ClientServerId = regSlave.ServerId
	return peer.SendOk(cmdPacket.PacketSeq + 1)
	return
}

func (peer *Peer) onCmdBinlogDump(cmdPacket *mysql.BaseCommandPacket) (err error) {
	dump := mysql.ComBinglogDump{}
	dump.FromBuffer(peer.Buffer[:cmdPacket.PacketLength])
	relay := peer.GetRelay()
	fmt.Printf("peer %s: dump from %s:%d\n", peer.RemoteAddr(), dump.BinlogFilename, dump.BinlogPos)
	currentIndex := relay.FindIndex(dump.BinlogFilename)
	if currentIndex < 0 {
		// binlog not exists
		fmt.Printf("peer %s: binlog not exists\n", peer.RemoteAddr())
		// TODO: return error code
		return
	}
	// TODO: send fake binlog description event and rotate event
	currentPos := dump.BinlogPos
	relayIndex, relayPos := relay.CurrentPosition()
	peer.seq = cmdPacket.PacketSeq + 1
	// TODO: check for last pos

	var delayer util.AutoDelayer
	var file *os.File
	for {
		binlog := relay.BinlogInfoByIndex(currentIndex)
		if file != nil {
			fmt.Printf("peer %s: close %s\n", peer.RemoteAddr(), file.Name())
			file.Close()
		}
		currentFilePath := relay.NameToPath(binlog.Name)
		fmt.Printf("peer %s: open %s\n", peer.RemoteAddr(), currentFilePath)
		file, err = os.Open(currentFilePath)
		if err != nil {
			return err
		}
		peer.sendFakeRotateEvent(relay.NameByIndex(currentIndex), uint64(currentPos))
		endPos := binlog.Size
		for {
			err = peer.sendBinlog(file, currentPos, endPos)
			if err != nil {
				return
			}
			currentPos = relayPos
			if currentIndex < relayIndex {
				break // not last file
			}
			relayIndex, relayPos = relay.CurrentPosition()
			for currentIndex == relayIndex && currentPos >= relayPos {
				delayer.Delay()
				relayIndex, relayPos = relay.CurrentPosition()
			}
			if currentIndex == relayIndex { // last file
				endPos = relayPos
			} else { // already rotated
				binlog = relay.BinlogInfoByIndex(currentIndex)
				endPos = binlog.Size
			}
		}
		currentPos = 4
		currentIndex++
	}
	return
}

func (peer *Peer) sendFakeRotateEvent(name string, position uint64) (err error) {
	fakeRotateEvent := mysql.RotateEvent{Name: name, Position: position}
	packet := fakeRotateEvent.BuildFakePacket(peer.Server.Server.ServerId)
	fmt.Println(packet.String())
	packet.PacketSeq = peer.seq
	peer.seq++
	err = mysql.WritePacketTo(&packet, peer.Conn, peer.Buffer[:])
	if err != nil {
		fmt.Println(err.Error())
	}
	return
}
func (peer *Peer) sendBinlog(file *os.File, from uint32, to uint32) (err error) {
	fmt.Printf("peer %s: send %d:%d\n", peer.RemoteAddr(), from, to)
	file.Seek(int64(from), 0)
	//var buffer [8192]byte
	var event mysql.BinlogEventPacket
	var n int64
	pos := from
	peer.Buffer[0] = '\x00'
	for {
		p, _ := file.Seek(0, os.SEEK_CUR)
		fmt.Printf("ftell.before: %d\n", p)
		_, err = file.Read(peer.Buffer[1 : mysql.BinlogEventHeaderSize+1])
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		event.FromBuffer(peer.Buffer[:])
		event.PacketLength = event.EventSize + 1 //
		event.PacketSeq = peer.seq
		peer.seq++
		fmt.Println("event: " + event.String())
		if event.LogPos-event.EventSize != pos {
			err = fmt.Errorf("bad pos: pos: %d, LogPos: %d, EventSize: %d", pos, event.LogPos, event.EventSize) //mysql.BuildErrPacket(mysql.ER_BINLOG_LOGGING_IMPOSSIBLE, "")
			// TODO: output mysql error pkt
			return
		}
		err = event.Reset(false)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		reader := event.GetReader(file, peer.Buffer[:mysql.BinlogEventHeaderSize+1])
		reader.WithProtocolHeader = true
		n, err = io.Copy(peer.Conn, &reader)
		fmt.Printf("%d bytes sent to peer\n", n)

		p, _ = file.Seek(0, os.SEEK_CUR)
		fmt.Printf("ftell.after: %d\n", p)

		// TODO: validate checksum
		pos += uint32(n - 5)
		if err != nil {
			fmt.Println(err.Error())
		}
		if n == 0 {
			return
		}
		if pos >= to {
			return
		}

	}
	return
}

func (peer *Peer) onCmdQuit(cmdPacket *mysql.BaseCommandPacket) (err error) {
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

var normalizeRegEx, _ = regexp.Compile("[ ]*([ ~!%^&*()=+<>,/.-])[ ]*")

func NormalizeSpecialQuery(query string) string {
	return strings.ToLower(normalizeRegEx.ReplaceAllString(query, "$1"))
}
