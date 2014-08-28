package server

type Config struct {
    Upstreams map[string] Upstream
    Users     map[string] User
    Server    Server
    
}

type Upstream struct {
    Name       string
    LocalDir   string
    StartFile  string
    ServerAddr string
    Username   string
    Password   string
    ServerId   uint32
}

type User struct {
    Name         string
    Password     string
    Upstream     string
}

type Server struct {
    Addr      string
    ServerId  uint32
    Uuid      string
}
