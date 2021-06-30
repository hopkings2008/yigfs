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
	FILE_PERM = 644
	DIR_PERM = 755
)

const (
	RootDirIno uint64 = iota + 1
	RootParentDirIno
)

const (
	MachineDown = iota
	MachineUp
)

const (
	NotDeleted = iota
	Deleted
)

const (
	NotExisted = iota + 1
	Existed
)

const (
	DeleteBlocks = "delete_blocks"
	DeleteFile = "delete_file"
)

const (
	MaxDeleteBlocksNum = 3000
	MaxDeleteFileBlocksNum = 50000
	MaxDeleteSegBlocks = 3000
)

const (
	DeleteBlocksGroup = "deleteGroup"
	DeleteBlocksTopic = "deleteBlocksTopic"
)