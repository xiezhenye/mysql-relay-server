package server

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type Config struct {
	Upstreams map[string]UpstreamConfig
	Users     map[string]UserConfig
	Server    ServerConfig
	Log       string
}

type UpstreamConfig struct {
	Name       string
	LocalDir   string
	StartFile  string
	ServerAddr string
	Username   string
	Password   string
	ServerId   uint32
	Semisync   bool
}

type UserConfig struct {
	Name     string
	Host     string
	Password string
	Upstream string
}

type ServerConfig struct {
	Addr     string
	ServerId uint32
	Uuid     string
	Version  string
}

func (self *Config) FromJson(buf []byte) error {
	return json.Unmarshal(buf, self)
}

func (self *Config) FromJsonFile(path string) (err error) {
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	var buf []byte
	buf, err = ioutil.ReadAll(f)
	if err != nil {
		return
	}
	err = self.FromJson(buf)
	return
}
