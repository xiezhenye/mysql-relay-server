package main

import (
    "mysql_relay/mysql"
    "mysql_relay/relay"
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
    var relay relay.BinlogRelay
    relay.Init(c, "D:\\test\\binlog", "log-bin.000001")
    relay.Run()
}

