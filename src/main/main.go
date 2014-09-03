package main

import (
    //"mysql_relay/mysql"
    //"mysql_relay/relay"
    "mysql_relay/server"
    "fmt"
)

func main() {
    var err error
    /*
    c := mysql.Client {
        ServerAddr: "127.0.0.1:3306"
        ServerAddr: "192.168.56.102:3306"
        Username:   "root"
        Password:   "12345678"
        ServerId:   12
    }
    err = c.Connect()
    if err != nil {
        fmt.Println(err)
        return
    }
    var relay relay.BinlogRelay
    relay.Init(c, "D:\\test\\binlog", "log-bin.000001")
    err = relay.Run()
    if err != nil {
        fmt.Println(err)
    }
    */
    s := server.Server {
        Addr: ":13306",
        Version: "5.6.19-debug-log",
    }
    err = s.Run()
    if err != nil {
        fmt.Println(err)
    }
}

