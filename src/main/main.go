package main

import (
    "mysql_relay/mysql"
    "fmt"
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
    events, errChan := c.DumpBinlog(mysql.ComBinglogDump{
        BinlogFilename:"log-bin.000001",
        BinlogPos:4,
        ServerId:c.ServerId,
    })
    if err != nil {
        fmt.Println("ERR:"+err.Error())
        return
    }
    for event := range events {
        fmt.Println(event)
    }
    err, _ = <- errChan
    if err != nil {
        fmt.Println(err)
    }
}

