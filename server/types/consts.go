package types


const (
	TIME_LAYOUT_TIDB = "2006-01-02 15:04:05"
)

const (
	HDR_CONTENT_LEN = "Content-Length"
	CTX_REQ_ID = "ctx_req_id"
)

const (
	MAXMUM_INO_VALUE = uint64(1 << 64 -1)
)

const (
	COMMON_FILE = iota + 1
	DIR_FILE
)

const (
	Read = iota + 1
	ReadAndWrite
	ReadWriteAndExec
)
