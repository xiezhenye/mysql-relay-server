package relay

import (
	"fmt"
	"io"
	"io/ioutil"
	"mysql_relay/mysql"
	"mysql_relay/util"
	"os"
	"sync"
)

type BinlogIndexEventPosEntry struct {
	Index uint32
	Pos   uint32
}

type BinlogIndexEntry struct {
	Name  string
	Size  uint32
	Count uint32
	//EventPos  []BinlogIndexEventPosEntry
}

func (self *BinlogIndexEntry) Append(size uint32) {
	//if self.Count % 256 == 0 {
	//    self.EventPos = append(self.EventPos, BinlogIndexEventPosEntry{ Index:self.Count, Pos:self.Size })
	//}
	self.Count++
	self.Size += size
}

type BinlogRelay struct {
	name       string
	localDir   string
	startFile  string
	startPos   uint32
	syncBinlog int

	lock sync.RWMutex

	client mysql.Client
	buf    [32768]byte

	fileIndex       []BinlogIndexEntry
	curFileId       int
	semisync        bool
	heartbeatPeriod uint32
	networkTimeout  uint32
	logger          util.Logger
}

type writeTask struct {
	buffer []byte
	name   string
	pos    int64
	size   uint32
	seq    byte
	ack    bool
}

func (self *BinlogRelay) Init(name string, client mysql.Client, localDir string, startFile string) (err error) {
	self.name = name
	self.client = client
	self.localDir = localDir
	self.startFile = startFile
	self.fileIndex = make([]BinlogIndexEntry, 0, 16)
	self.syncBinlog = 1

	logPath := localDir + string(os.PathSeparator) + "relay.log"
	err = self.logger.ToFile(logPath)
	if err != nil {
		return
	}
	self.logger.SetToStderr(false)
	self.logger.SetPrefix("[upstream:" + name + "]")
	self.logger.Info("relay inited")
	self.ReloadPos()
	return
}

func (self *BinlogRelay) SetSemisync(b bool) {
	self.semisync = b
}

func (self *BinlogRelay) SetHeartbeatPeriod(n uint32) {
	self.heartbeatPeriod = n
}

func (self *BinlogRelay) ReloadPos() error {
	// TODO: use binlog list
	filename := self.startFile
	for {
		stat, err := os.Stat(self.NameToPath(filename))
		if os.IsNotExist(err) {
			break
		} else if err != nil {
			self.logger.Error(err.Error())
			return err
		}
		self.startFile = filename
		self.startPos = uint32(stat.Size())

		self.fileIndex = append(self.fileIndex, BinlogIndexEntry{
			Name: filename,
			Size: uint32(stat.Size()),
		})
		self.curFileId = len(self.fileIndex) - 1

		filename, err = mysql.NextBinlogName(self.startFile)
		if err != nil {
			self.logger.Error(err.Error())
			return err
		}
	}
	self.logger.Info("continue dump at %s:%d", self.startFile, self.startPos)
	return nil
}

func (self *BinlogRelay) appendIndex(name string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.fileIndex = append(self.fileIndex, BinlogIndexEntry{
		Name:  name,
		Size:  0,
		Count: 0,
		//EventPos: make([]BinlogIndexEventPosEntry, 0, 16),
	})
	self.curFileId = len(self.fileIndex) - 1
	self.fileIndex[self.curFileId].Size = 4
}

func (self *BinlogRelay) appendEvent(size uint32) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.fileIndex[self.curFileId].Append(size)
	self.logger.Info("append: %d: %v", self.curFileId, self.fileIndex[self.curFileId])
}

func (self *BinlogRelay) CurrentPosition() (index int, pos uint32) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.curFileId, self.fileIndex[index].Size
}

func (self *BinlogRelay) FindIndex(name string) int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	for i, index := range self.fileIndex {
		if index.Name == name {
			return i
		}
	}
	return -1
}

func (self *BinlogRelay) NameByIndex(index int) string {
	return self.BinlogInfoByIndex(index).Name
}

func (self *BinlogRelay) BinlogInfoByIndex(index int) BinlogIndexEntry {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.fileIndex[index]
}

func (self *BinlogRelay) PathByIndex(index int) string {
	name := self.NameByIndex(index)
	return self.NameToPath(name)
}

func (self *BinlogRelay) NameToPath(name string) string {
	return self.localDir + string(os.PathSeparator) + name
}

func (self *BinlogRelay) getFileToWrite(name string, pos int64) (f *os.File, err error) {
	path := self.NameToPath(name)
	//self.logger.Info("write at %s:%d", path, task.pos)
	if pos <= mysql.LOG_POS_START {
		f, err = os.Create(path)
		if err != nil {
			return
		}
		// binlog header
		_, err = f.Write([]byte{'\xfe', 'b', 'i', 'n'})
		self.appendIndex(name)

	} else {
		f, err = os.OpenFile(path, os.O_RDWR, 0664)
	}
	if err != nil {
		return
	}
	return
}

func (self *BinlogRelay) writeBinlog(bufChanIn chan<- []byte, bufChanOut <-chan writeTask) (err error) {
	self.logger.Info("writer begin")
	name := ""
	seq := byte(0)
	var f *os.File
	defer func() {
		close(bufChanIn)
		self.logger.Info("writer ended")
		if err != nil {
			self.logger.Error("writer: " + err.Error())
		}
	}()
	defer util.RecoverToError(&err)

	ib := 0
	eventSize := uint32(0)
	for task := range bufChanOut {
		//self.logger.Info("got task %s:%d", task.name, task.pos)
		if task.name != name { // file rotated!
			self.logger.Info("writer rotated to " + task.name)
			if f != nil {
				f.Close()
			}
			f = util.Assert1(self.getFileToWrite(task.name, task.pos)).(*os.File)
			name = task.name
		}
		//self.logger.Info("write at %d", task.pos)
		_ = util.Assert1(f.WriteAt(task.buffer[:task.size], task.pos))
		eventSize += task.size
		if seq != task.seq { // next event
			//self.logger.Info("append event %d", task.seq)
			seq = task.seq
			ib++
			if ib >= self.syncBinlog {
				//self.logger.Info("sync file")
				util.Assert0(f.Sync())
				if self.semisync && task.ack {
					go self.client.SendSemisyncAck(task.name, uint64(task.pos))
				}
				self.appendEvent(eventSize)
				ib = 0
			}
			eventSize = 0
		}
		bufChanIn <- task.buffer
	}
	return
}

func (self *BinlogRelay) dumpBinlog(bufChanIn <-chan []byte, bufChanOut chan<- writeTask) (err error) {
	defer func() {
		close(bufChanOut)
		self.logger.Info("dumper ended")
		if err != nil {
			self.logger.Error("dumper: " + err.Error())
		}
	}()
	defer util.RecoverToError(&err)

	if self.startPos < mysql.LOG_POS_START {
		self.startPos = mysql.LOG_POS_START
	}
	stream := util.Assert1(self.client.DumpBinlog(mysql.ComBinglogDump{
		BinlogFilename: self.startFile,
		BinlogPos:      self.startPos,
		ServerId:       self.client.ServerId,
	}, self.semisync, self.heartbeatPeriod)).(*mysql.BinlogEventStream)
	self.semisync = stream.IsSemisync()
	filename := self.startFile
	hasBinlogChecksum := false
	curPos := self.startPos

	self.logger.Info("dumper start at %s:%d", filename, curPos)

	for event := stream.Next(); event != nil; event = stream.Next() {
		event.HasChecksum = hasBinlogChecksum
		self.logger.Info(fmt.Sprintf("event: { %s }", event.String()))
		switch event.EventType {
		case mysql.FORMAT_DESCRIPTION_EVENT:
			var formatDescription mysql.FormatDescriptionEvent
			formatDescription.Parse(event, self.client.Buffer[:])
			hasBinlogChecksum = (formatDescription.ChecksumAlgorism == 1)

		case mysql.ROTATE_EVENT:
			// change to next file!
			var rotate mysql.RotateEvent
			util.Assert0(rotate.Parse(event, self.client.Buffer[:]))
			filename = rotate.Name
			curPos = uint32(rotate.Position)
			self.logger.Info("rotate event: %s:%d", filename, curPos)
		}

		if event.IsFake() {
			continue
		}

		util.Assert0(event.Reset(false))
		reader := event.GetReader(self.client.Conn, self.client.Buffer[:])
		if event.Semisync == mysql.SEMISYNC_NO {
			io.CopyN(ioutil.Discard, &reader, 1) // Ok byte
		} else {
			io.CopyN(ioutil.Discard, &reader, 3) // Ok byte and semisync bytes
		}
		n := 0
		for {
			buffer := <-bufChanIn
			n, err = reader.Read(buffer)
			if n > 0 {
				//self.logger.Info("writeTask: {name:%s, pos:%d, size:%d, bufsize:%d}", filename, curPos, n, len(buffer))
				bufChanOut <- writeTask{
					name:   filename,
					buffer: buffer,
					size:   uint32(n),
					seq:    event.PacketSeq,
					pos:    int64(curPos),
					ack:    event.Semisync == mysql.SEMISYNC_ACK,
				}
				curPos += uint32(n)
			}
			if err == io.EOF {
				break
			}
			util.Assert0(err)
		}
	}
	err = stream.GetError()
	return
}

func (self *BinlogRelay) Run() error {
	nBuffers := 4
	bufChanIn := make(chan []byte, nBuffers)
	bufChanOut := make(chan writeTask, nBuffers)

	sz := len(self.buf) / nBuffers
	for i := 0; i < nBuffers; i++ {
		bufChanIn <- self.buf[i*sz : i*sz+sz]
	}
	return util.Barrier{
		func() error { return self.dumpBinlog(bufChanIn, bufChanOut) },
		func() error { return self.writeBinlog(bufChanIn, bufChanOut) },
	}.Run()
}
