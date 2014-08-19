package mysql

import ("fmt")

type Error struct {
    Code    int
    Message string
}

func (self Error) Error() string {
    return fmt.Sprintf("%d: %s", self.Code, self.Message)
}

var (
BAD_PACKET           = Error{1, "bad packet"}
BAD_HANDSHAKE_PACKET = Error{2, "bad handshake packet"}
NOT_SUCH_PACET_TYPE  = Error{3, "not such packet type"}
BUFFER_NOT_SUFFICIENT  = Error{4, "buffer not sufficient"}
)
