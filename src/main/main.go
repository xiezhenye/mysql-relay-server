package main

import (
	//"mysql_relay/mysql"
	//"mysql_relay/relay"
	"fmt"
	"mysql_relay/server"
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

	s := server.Server{
		Config: server.Config{
			Upstreams: map[string]server.UpstreamConfig{
				"local": server.UpstreamConfig{
					Name:       "local",
					LocalDir:   "D:\\test\\binlog",
					StartFile:  "log-bin.000001",
					ServerAddr: "127.0.0.1:3306",
					Username:   "root",
					Password:   "12345678",
					ServerId:   12,
				},
			},
			Users: map[string]server.UserConfig{
				"repl": server.UserConfig{
					Name:     "repl",
					Host:     "192.168.56.0/24",
					Password: "12345678",
					Upstream: "local",
				},
			},
			Server: server.ServerConfig{
				Addr:     ":13306",
				Version:  "5.6.19-log",
				ServerId: 1,
				Uuid:     "a2d605d4-67df-11e4-bfdd-08002792fa42",
			},
		},
	}

	err = s.Run()
	if err != nil {
		fmt.Println(err)
	}
}
