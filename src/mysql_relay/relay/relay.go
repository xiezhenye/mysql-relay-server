package relay

import
(
    "fmt"
    "mysql_relay/mysql"
    "mysql_relay/util"
    "os"
    "io"
    "io/ioutil"
    "sync"
)

type BinlogIndexEventPosEntry struct{
    Index int
    Pos int
}

type BinlogIndexEntry struct {
    Name      string
    Size      int
    Count     int
    //EventPos  []BinlogIndexEventPosEntry
}

func (self *BinlogIndexEntry) Append(size int) {
    //if self.Count % 256 == 0 {
    //    self.EventPos = append(self.EventPos, BinlogIndexEventPosEntry{ Index:self.Count, Pos:self.Size })
    //}
    self.Count++
    self.Size+= size
}

type BinlogRelay struct {
    localDir   string
    startFile  string
    startPos   uint32
    syncBinlog int
    
    lock       sync.RWMutex
    
    client     mysql.Client
    buf        [8192]byte
    
    fileIndex  []BinlogIndexEntry
    curFileId  int
    
    logger     util.Logger
    
}

type writeTask struct {
    buffer []byte
    name   string
    pos    int64
    size   int
    seq    byte
}

func (self *BinlogRelay) Init(client mysql.Client, localDir string, startFile string) {
    self.client = client
    self.localDir = localDir
    self.startFile = startFile
    self.fileIndex = make([]BinlogIndexEntry, 0, 16)
    self.syncBinlog = 1
    self.logger.Init(localDir + string(os.PathSeparator) + "relay.log")
    self.logger.Info("relay inited")
    self.ReloadPos()
}

func (self *BinlogRelay) ReloadPos() {
    for {
        filename := mysql.NextBinlogName(self.startFile)
        if stat, err := os.Stat(filename); os.IsNotExist(err) {
            break
        }
        self.startFile = filename
    }
    self.startPos = uint32(stat.Size())
    self.logger.Info(fmt.Sprintf("reload pos to %s:%ud", self.startFile, self.startPos))
}

func (self *BinlogRelay) appendIndex(name string) {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.fileIndex = append(self.fileIndex, BinlogIndexEntry{
        Name: name,
        Size: 0,
        Count: 0,
        //EventPos: make([]BinlogIndexEventPosEntry, 0, 16),
    })
    self.curFileId = len(self.fileIndex) - 1
    self.fileIndex[self.curFileId].Size = 4
}

func (self *BinlogRelay) appendEvent(size int) {
    self.lock.Lock()
    defer self.lock.Unlock()
    self.fileIndex[self.curFileId].Append(size)
}

func (self *BinlogRelay) CurrentPosition() (index int, pos int) {
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
    self.lock.RLock()
    defer self.lock.RUnlock()
    return self.fileIndex[index].Name
}

func (self *BinlogRelay) PathByIndex(index int) string {
    name := self.NameByIndex(index)
    return self.NameToPath(name)
}

func (self *BinlogRelay) NameToPath(name string) string {
    return self.localDir + string(os.PathSeparator) + name
}

func (self *BinlogRelay) writeBinlog(bufChanIn chan<-[]byte, bufChanOut <-chan writeTask) (err error){
    name := ""
    seq := byte(0)
    var f *os.File
    defer func(){
        close(bufChanIn)
        self.logger.Info("writer ended")
        if err != nil {
            self.logger.Err("writer: " + err.Error())
        }
    }()
    ib := 0
    eventSize := 0
    for task := range bufChanOut {
        if task.name != name {
            self.logger.Info(fmt.Sprintf("writer rotated to %s", task.name))
            // file rotated!
            if f != nil {
                f.Close()
            }
            path := self.NameToPath(task.name)
            if task.pos <= mysql.LOG_POS_START {
                f, err = os.Create(path)
                if err != nil {
                    return
                }
                // binlog header
                _, err = f.Write([]byte{'\xfe','b','i','n'})
                self.appendIndex(task.name)
                
            } else {
                f, err = os.OpenFile(path, os.O_RDWR, 0666)
            }
            if err != nil {
                return
            }
            name = task.name
        }
        _, err = f.WriteAt(task.buffer[:task.size], task.pos)
        if err != nil {
            return
        }
        eventSize+= task.size
        if seq != task.seq { // next event
            self.appendEvent(eventSize)
            seq = task.seq
            ib++
            if ib >= self.syncBinlog {
                ib = 0
                f.Sync()
            }
            eventSize = 0
        }
        bufChanIn<-task.buffer
    }
    
    return
}

func (self *BinlogRelay) dumpBinlog(bufChanIn <-chan []byte, bufChanOut chan<-writeTask) (err error) {
    defer func(){
        close(bufChanOut)
        self.logger.Info("dumper ended")
        if err != nil {
            self.logger.Err("dumper: " + err.Error())
        }
    }()
    if self.startPos < mysql.LOG_POS_START {
        self.startPos = mysql.LOG_POS_START
    }
    stream := self.client.DumpBinlog(mysql.ComBinglogDump{
        BinlogFilename: self.startFile,
        BinlogPos: self.startPos,
        ServerId: self.client.ServerId,
    })
    
    filename := self.startFile
    rotateUsed := true
    hasBinlogChecksum := false
    curPos := self.startPos
    for event := range stream.GetChan() {
        if event.EventType == mysql.FORMAT_DESCRIPTION_EVENT {
            var formatDescription mysql.FormatDescriptionEvent
            formatDescription.Parse(&event, self.client.Buffer[:])
            hasBinlogChecksum = (formatDescription.ChecksumAlgorism == 1)
            // a new binlog file
            if !rotateUsed {
                // missing last rotate event
                // auto build new the filename
                filename, err = mysql.NextBinlogName(filename)
                if err != nil {
                    return
                }
            }
            rotateUsed = false
            self.logger.Info(fmt.Sprintf("dumper rotated to %s, checksum: %t", filename, hasBinlogChecksum))
        }
        event.HasChecksum = hasBinlogChecksum
        event.Reset(true)
        reader := event.GetReader(self.client.Conn, self.client.Buffer[:])
        if event.IsFake() {
            // fake event should be ignored!
            io.Copy(ioutil.Discard, &reader)
        } else {
            io.CopyN(ioutil.Discard, &reader, 1) //discard first ok byte
            n := 0
            for {
                buffer := <-bufChanIn
                n, err = reader.Read(buffer)
                if n > 0 {
                    bufChanOut<-writeTask{name:filename, buffer:buffer, size: n, seq: event.PacketSeq, pos: int64(curPos)}
                }
                if err != nil {
                    if err == io.EOF {
                        break
                    } else {
                        return
                    }
                }
            }
        }
        if event.EventType == mysql.ROTATE_EVENT {
            // change to next file!
            var rotate mysql.RotateEvent
            err = rotate.Parse(&event, self.client.Buffer[:])
            if err != nil {
                return
            }
            filename = rotate.Name
            rotateUsed = true
        }
        curPos+= event.LogPos
        stream.Continue()
    }
    err = stream.GetError()
    if err != nil {
        return
    }
    return
}

func (self *BinlogRelay) Run() error {
    nBuffers := 8
    bufChanIn := make(chan []byte, nBuffers)
    bufChanOut := make(chan writeTask, nBuffers)
    
    sz := len(self.buf) / nBuffers
    for i := 0; i < nBuffers; i++ {
        bufChanIn<-self.buf[i*sz : i*sz+sz]
    }
    return util.Barrier{
        func() error { return self.dumpBinlog(bufChanIn, bufChanOut) },
        func() error { return self.writeBinlog(bufChanIn, bufChanOut) },
    }.Run()
}



