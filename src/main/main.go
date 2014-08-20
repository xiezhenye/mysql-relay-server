package main

import (
    "mysql_relay/mysql"
    "fmt"
    "io/ioutil"
    "io"
)

func main() {
    var c mysql.Client
    var err error
    //c.ServerAddr = "127.0.0.1:3306"
    c.ServerAddr = "192.168.56.102:3306"
    c.Username   = "root"
    c.Password   = "12345678"
    c.ServerId   = 12
    err = c.Connect()
    if err != nil {
        fmt.Println(err)
        return
    }
    handshake, err := c.Handshake()
    if err != nil {
        fmt.Println("ERR:"+err.Error())
        return
    }
    var ok mysql.OkPacket
    ok, err = c.Auth(handshake)
    if err != nil {
        fmt.Println("ERR:"+err.Error())
        return
    }
    fmt.Println(ok)
    
    ok, err = c.Command(&mysql.QueryCommand{"SET @master_binlog_checksum='NONE';"})
    if err != nil {
        fmt.Println("ERR:"+err.Error())
        return
    }
    fmt.Println(ok)
    
    stream := c.DumpBinlog(mysql.ComBinglogDump{
        BinlogFilename:"log-bin.000005",
        BinlogPos:568,//5,
        ServerId:c.ServerId,
    })
    for event := range stream.GetChan() {
        //fmt.Println(event)
        reader := event.GetReader(c.Conn, c.Buffer[:], true)
        io.Copy(ioutil.Discard, &reader)
        stream.Continue()
    }
    err = stream.GetError()
    if err != nil {
        fmt.Println(err)
    }
}

