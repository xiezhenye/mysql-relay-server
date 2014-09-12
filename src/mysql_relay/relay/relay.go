package relay

import
(
//    "fmt"
    "mysql_relay/mysql"
    "mysql_relay/util"
    "os"
    "io"
    "io/ioutil"
)

type BinlogRelay struct {
    client     mysql.Client
    localDir   string
    startFile  string
    buf        [8192]byte
}

type writeTask struct {
    buffer []byte
    name   string
    size   int
    seq    byte
}

func (self *BinlogRelay) Init(client mysql.Client, localDir string, startFile string) {
    self.client = client
    self.localDir = localDir
    self.startFile = startFile
}

func (self *BinlogRelay) writeBinlog(bufChanIn chan<-[]byte, bufChanOut <-chan writeTask) (err error){
    name := ""
    seq := byte(0)
    var f *os.File
    defer func(){
        close(bufChanIn)
    }()
    for task := range bufChanOut {
        if task.name != name {
            // file rotated!
            if f != nil {
                f.Close()
            }
            path := self.localDir+string(os.PathSeparator)+task.name
            f, err = os.Create(path)
            if err != nil {
                return
            }
            
            // binlog header
            _, err = f.Write([]byte{'\xfe','b','i','n'})
            if err != nil {
                return
            }
            name = task.name
            
        }
        _, err = f.Write(task.buffer[:task.size])
        if err != nil {
            return
        }
        if seq != task.seq {
            seq = task.seq
        } else {
            f.Sync()
        }
        bufChanIn<-task.buffer
    }
    return
}

func (self *BinlogRelay) dumpBinlog(bufChanIn <-chan []byte, bufChanOut chan<-writeTask) (err error) {
    defer func(){
        close(bufChanOut)
    }()
    stream := self.client.DumpBinlog(mysql.ComBinglogDump{
        BinlogFilename: self.startFile,
        BinlogPos: 4,
        ServerId: self.client.ServerId,
    })
    
    filename := self.startFile
    rotateUsed := true
    hasBinlogChecksum := false
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
                    bufChanOut<-writeTask{name:filename, buffer:buffer, size: n, seq: event.PacketSeq}
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



