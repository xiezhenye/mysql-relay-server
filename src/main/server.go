package main

import (
	"flag"
	"fmt"
	"mysql_relay/server"
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
	s := server.Server{
		Config: conf,
	}

	err = s.Run()
	if err != nil {
		fmt.Println(err)
	}
}
