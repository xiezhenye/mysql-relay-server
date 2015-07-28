package server

import (
	"fmt"
	"hash/crc32"
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
	"time"
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
	defer util.RecoverToError(&err)
	if !self.Server.CheckHost(self.RemoteIP()) {
		errPacket := mysql.ErrPacket{
			ErrorCode:    mysql.ER_HOST_NOT_PRIVILEGED,
			SqlState:     "",
			ErrorMessage: fmt.Sprintf(mysql.SERVER_ERR_MESSAGES[mysql.ER_HOST_NOT_PRIVILEGED], self.RemoteIP()),
		}
		err = mysql.WritePacketTo(&errPacket, self.Conn, self.Buffer[:])
		return
	}
	//fmt.Println(self.RemoteIP())
	handshake := mysql.BuildHandShakePacket(self.Server.Config.Server.Version, self.ConnId)
	util.Assert0(mysql.WritePacketTo(&handshake, self.Conn, self.Buffer[:]))
	//fmt.Println(handshake)
	var auth mysql.AuthPacket
	util.Assert0(mysql.ReadPacketFrom(&auth, self.Conn, self.Buffer[:]))
	user, ok := self.Server.Config.Users[auth.Username]
	authed := false
	if ok && hostContains(user.Host, self.RemoteIP()) {
		hash2 := mysql.Hash2(user.Password)
		authed = mysql.CheckAuth(handshake.AuthString, hash2[:], []byte(auth.AuthResponse))
	}
	//fmt.Println(authed)
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
		fmt.Println("starting " + name)
		c := mysql.Client{
			ServerAddr: upstreamConfig.ServerAddr,
			Username:   upstreamConfig.Username,
			Password:   upstreamConfig.Password,
			ServerId:   upstreamConfig.ServerId,
		}
		go func() {
			nTry := uint32(0)
			for {
				if nTry < upstreamConfig.MaxRetryTimes {
					fmt.Printf("try connecting %d\n", nTry)
					err := c.Connect()
					if err != nil {
						fmt.Println("connect failed")
						time.Sleep(time.Duration(upstreamConfig.RetryInterval) * time.Second)
						nTry++
						continue
					} else {
						nTry = uint32(0)
					}
				} else {
					break
				}
				fmt.Printf("connected %d\n", nTry)
				relay := new(relay.BinlogRelay)
				if upstreamConfig.ReadTimeout > 0 {
					c.Conn = util.NewTimeoutConn(c.Conn, upstreamConfig.ReadTimeout)
				}
				err = relay.Init(name, c, upstreamConfig.LocalDir, upstreamConfig.StartFile)
				if err != nil {
					return
				}
				relay.SetSemisync(upstreamConfig.Semisync)
				self.Upstreams[name] = relay
				_ = relay.Run()
			}
			fmt.Println("upstram ended")
		}()
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
		go func() {
			defer func() {
				self.Peers[connId].Close()
				self.Closed <- connId
			}()
			self.handle(self.Peers[connId])
		}()
	}
}

func (self *Server) handle(peer *Peer) {
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
	defer util.RecoverToError(&err)

	dump := mysql.ComBinglogDump{}
	dump.FromBuffer(peer.Buffer[:cmdPacket.PacketLength])
	relay := peer.GetRelay()
	fmt.Printf("peer %s: dump from %s:%d\n", peer.RemoteAddr(), dump.BinlogFilename, dump.BinlogPos)
	currentIndex := relay.FindIndex(dump.BinlogFilename)
	if currentIndex < 0 {
		// binlog not exists
		fmt.Printf("peer %s: binlog not exists\n", peer.RemoteAddr())
		// TODO: wait for binlog
		// TODO: return error code
		return
	}
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
		file = util.Assert1(os.Open(currentFilePath)).(*os.File)
		fmt.Printf("peer %s: send fake RotateEvent\n", peer.RemoteAddr())
		util.Assert0(peer.sendFakeRotateEvent(relay.NameByIndex(currentIndex), uint64(currentPos)))
		endPos := binlog.Size
		if currentPos > mysql.LOG_POS_START {
			fmt.Printf("peer %s: send fake FormatDescriptionEvent\n", peer.RemoteAddr())
			util.Assert0(peer.sendFakeFormatDescriptionEvent(file))
		}
		for {
			util.Assert0(peer.sendBinlog(file, currentPos, endPos))
			if currentIndex < relayIndex {
				break // not last file
			}
			currentPos = endPos
			relayIndex, relayPos = relay.CurrentPosition()
			for currentIndex == relayIndex && currentPos >= relayPos {
				//fmt.Printf("Waiting for update (%d, %d)!\n", relayIndex, relayPos)
				delayer.Delay()
				relayIndex, relayPos = relay.CurrentPosition()
			}
			currentSize := relay.BinlogInfoByIndex(currentIndex).Size
			if currentIndex != relayIndex && endPos == currentSize {
				break
			}
			endPos = currentSize
		}
		currentPos = mysql.LOG_POS_START
		currentIndex++
	}
	return
}

func (peer *Peer) sendFakeRotateEvent(name string, position uint64) (err error) {
	fakeRotateEvent := mysql.RotateEvent{Name: name, Position: position}
	packet := fakeRotateEvent.BuildFakePacket(peer.Server.Server.ServerId)
	fmt.Println("fake rotate event: " + packet.String())
	packet.PacketSeq = peer.seq
	peer.seq++
	err = mysql.WritePacketTo(&packet, peer.Conn, peer.Buffer[:])
	return
}

func (peer *Peer) sendFakeFormatDescriptionEvent(file *os.File) (err error) {
	file.Seek(mysql.LOG_POS_START, 0)
	peer.Buffer[4] = '\x00'
	_, err = file.Read(peer.Buffer[5 : mysql.BinlogEventHeaderSize+5])
	if err != nil {
		fmt.Printf("read fde failed: %s\n", err.Error())
		return
	}
	var event mysql.BinlogEventPacket
	event.FromBuffer(peer.Buffer[4:])

	if event.EventType != mysql.FORMAT_DESCRIPTION_EVENT {
		err = fmt.Errorf("Not a FORMAT_DESCRIPTION_EVENT")
		return
	}

	fmt.Println("fde read: " + event.String())
	_, err = file.Read(peer.Buffer[mysql.BinlogEventHeaderSize+5 : event.EventSize+5])
	if err != nil {
		return
	}
	//fmt.Printf("buffer: %v\n", peer.Buffer[:event.EventSize+5])
	event.PacketHeader = mysql.PacketHeader{PacketLength: event.EventSize + 1, PacketSeq: peer.seq}
	event.BodyLength = int(event.EventSize + 1 - mysql.BinlogEventHeaderSize)
	event.LogPos = 0
	event.ToBuffer(peer.Buffer[4:])
	mysql.ENDIAN.PutUint32(peer.Buffer[:], event.PacketHeader.ToUint32())

	var fde mysql.FormatDescriptionEvent
	err = fde.Parse(&event, peer.Buffer[5:])
	if err != nil {
		fmt.Println("parse fde failed! %s\n", err.Error())
		return
	}
	fmt.Printf("FDE: %v\n", fde)

	if fde.ChecksumAlgorism == 1 {
		//rewrite checksum!
		checksum := crc32.ChecksumIEEE(peer.Buffer[5 : event.EventSize+1])
		fmt.Printf("fake fde: rewrite checksum of %v == %08x\n", peer.Buffer[5:event.EventSize+1], checksum)
		mysql.ENDIAN.PutUint32(peer.Buffer[event.EventSize+1:], checksum)
	}

	fmt.Printf("fde packet: %v!!!!!!!\n", peer.Buffer[:event.PacketLength+4])
	peer.Conn.Write(peer.Buffer[:event.PacketLength+4])
	peer.seq++
	return
}

func (peer *Peer) sendBinlog(file *os.File, from uint32, to uint32) (err error) {
	defer util.RecoverToError(&err)

	fmt.Printf("peer %s: send %d:%d\n", peer.RemoteAddr(), from, to)
	if from >= to {
		return
	}
	file.Seek(int64(from), 0)
	var n int64
	pos := from
	peer.Buffer[0] = '\x00'
	for {
		_ = util.Assert1(file.Read(peer.Buffer[1 : mysql.BinlogEventHeaderSize+1]))
		var event mysql.BinlogEventPacket
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
		util.Assert0(event.Reset(false))
		reader := event.GetReader(file, peer.Buffer[:mysql.BinlogEventHeaderSize+1])
		reader.WithProtocolHeader = true
		n, err = io.Copy(peer.Conn, &reader)
		//fmt.Printf("%d bytes sent to peer\n", n)

		// TODO: validate checksum
		pos += uint32(event.EventSize) // n-5
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
	// TODO: support quit command
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
