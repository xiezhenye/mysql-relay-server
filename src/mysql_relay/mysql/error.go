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
NOT_GENERIC_RESPONSE_PACKET  = Error{5, "not generic response packet"}
NOT_OK_PACKET  = Error{6, "not ok packet"}
NOT_ERR_PACKET  = Error{7, "not err packet"}
NOT_EOF_PACKET  = Error{8, "not eof packet"}
BYTES_READ_NOT_CORRECT = Error{9, "bytes read not correct"}
PACKET_SEQ_NOT_CORRECT = Error{10, "packet seq not correct"}
NOT_SUCH_EVENT  = Error{11, "not such event"}
PROTOCOL_NOT_SUPPORTED  = Error{12, "protocol not supported"}
SERVER_CAPABILITY_NOT_SUFFICIENT = Error{13, "server capability not sufficient"}
LENENCINT_IS_NULL = Error{14, "is null"}
NOT_VALID_LENENCINT = Error{15, "not valid lenencint"}
)

