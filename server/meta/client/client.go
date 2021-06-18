package client

import (
        "context"

        "github.com/hopkings2008/yigfs/server/types"
)

// DB Client Interface
type Client interface {
	// List dir files
	ListDirFiles(ctx context.Context, dir *types.GetDirFilesReq) (dirFilesResp []*types.GetDirFileInfo, offset uint64, err error)
	// Create file
	CreateFile(ctx context.Context, file *types.CreateFileReq) (err error)
	// Init root dirs
	InitRootDirs(ctx context.Context, rootDir *types.InitDirReq) (err error)
	// Get file attr from parent ino
	GetDirFileInfo(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error)
	// Get file info from ino
	GetFileInfo(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error)
	// Create or update zone
	CreateOrUpdateZone(ctx context.Context, zone *types.InitDirReq) (err error)
	// Get file leader
	GetFileLeaderInfo(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error)
	// Create or update file leader
	CreateOrUpdateFileLeader(ctx context.Context, leader *types.GetLeaderReq) (err error)
	// Get one update machine
	GetOneUpMachine(ctx context.Context, zone *types.GetLeaderReq) (leader string, err error)
	// Get machine indo
	GetMachineInfo(ctx context.Context, zone *types.GetLeaderReq) (resp *types.GetMachineInfoResp, err error)
	// Set file attr
	SetFileAttr(ctx context.Context, file *types.SetFileAttrReq) (err error)
	// get segment leader
	GetSegmentLeader(ctx context.Context, segment *types.GetSegLeaderReq) (resp *types.LeaderInfo, err error)
	// create segment and zone info
	CreateSegmentInfoAndZoneInfo(ctx context.Context, segment *types.CreateSegmentReq, maxSize int) (err error)
	// get include offset index segments
	GetIncludeOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (getSegs map[interface{}][]*types.BlockInfo, err error)
	// get greater than offset index segments
	GetGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (getSegs map[interface{}][]*types.BlockInfo, err error)
	// get segments block info
	GetSegsBlockInfo(ctx context.Context, seg *types.GetSegmentReq, segs map[interface{}][]*types.BlockInfo) (resp *types.GetSegmentResp, err error)
	// update segment block info
	UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error)
	// get incomplete upload segments
	GetIncompleteUploadSegs(ctx context.Context, segInfo *types.GetIncompleteUploadSegsReq, segs []*types.IncompleteUploadSegInfo) (segsResp *types.GetIncompleteUploadSegsResp, err error)
	// get the slowest growing segment
	GetTheSlowestGrowingSeg(ctx context.Context, segReq *types.GetSegmentReq, segIds []*types.IncompleteUploadSegInfo) (isExisted bool, resp *types.GetTheSlowestGrowingSeg, err error)
	// get blocks by the target segment id
	GetBlocksBySegId(ctx context.Context, seg *types.GetTheSlowestGrowingSeg) (resp *types.GetSegmentResp, err error)
	// get segments by leader
	GetSegsByLeader(ctx context.Context, seg *types.GetIncompleteUploadSegsReq) (segsResp []*types.IncompleteUploadSegInfo, err error)
	// check whether the file has segments or not
	IsFileHasSegments(ctx context.Context, seg *types.GetSegmentReq) (isExisted bool, err error)
	// get segments info for the file
	GetAllExistedFileSegs(ctx context.Context, file *types.DeleteFileReq) (segs map[interface{}]struct{}, err error)
	// delete blocks in file_blocks table
	DeleteFileBlocks(ctx context.Context, file *types.DeleteFileReq) (err error) 
	// delete the targe file
	DeleteFile(ctx context.Context, file *types.DeleteFileReq) (err error)
	// delete segment blocks
	DeleteSegBlocks(ctx context.Context, file *types.DeleteFileReq) (err error)
	// delete segments info
	DeleteSegInfo(ctx context.Context, file *types.DeleteFileReq, segs map[interface{}]struct{}) (err error)
	// insert or update blocks in file_blocks and seg_blocks table
	InsertOrUpdateFileAndSegBlocks(ctx context.Context, segInfo *types.DescriptBlockInfo, segs []*types.CreateBlocksInfo, isUpdateInfo bool, blocksNum int) (err error)
	// update size and blocks number for the file
	UpdateSizeAndBlocksNum(ctx context.Context, file *types.GetFileInfoReq) (err error)
	// check segments machine
	CheckSegsmachine(ctx context.Context, zone *types.GetSegLeaderReq, segs []*types.CreateBlocksInfo) (isValid bool, err error)
	// remove segments in seg_blocks table.
	RemoveSegBlocks(ctx context.Context, segs []*types.CreateBlocksInfo, blocksNum int) (err error)
}

