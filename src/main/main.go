package main

import (
	"fmt"
	"mysql_relay/server"
    "flag"
)

func main() {
	var err error
    var conf server.Config
    var confPath string
    flag.StringVar(&confPath, "conf", "", "config file path")
    flag.Parse()
    err = conf.FromJsonFile(confPath)
    if err != nil {
        fmt.Println(err)
        return
    }
    s := server.Server {
		Config: conf,
    }
    
    /*
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
            Log: "D:\\test\\binlog\\server.log",
		},
	}
    */
	err = s.Run()
	if err != nil {
		fmt.Println(err)
	}
}
