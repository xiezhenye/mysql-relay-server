package server

type Config struct {
    Upstreams map[string] UpstreamConfig
    Users     map[string] UserConfig
    Server    ServerConfig
    
}

type UpstreamConfig struct {
    Name       string
    LocalDir   string
    StartFile  string
    ServerAddr string
    Username   string
    Password   string
    ServerId   uint32
}

type UserConfig struct {
    Name         string
    Host         string
    Password     string
    Upstream     string
}

type ServerConfig struct {
    Addr      string
    ServerId  uint32
    Uuid      string
    Version   string
}
