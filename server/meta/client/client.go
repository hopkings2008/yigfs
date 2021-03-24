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
	// Get segment info
	GetFileSegmentInfo(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error)
	// Create segment
	CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq) (mergeNumber int, err error)
	// get segment leader
	GetSegmentLeaderInfo(ctx context.Context, segment *types.GetSegLeaderReq) (resp *types.LeaderInfo, err error)
	// create segment leader
	CreateSegmentLeader(ctx context.Context, segment *types.CreateSegmentReq) (err error)
	// get covered existed blocks by blocks to be uploaded
	GetCoveredExistedBlocks(ctx context.Context, seg *types.CreateSegmentReq, startAddr, endAddr, tag int64) (blocks map[int64][]int64, err error)
	// get covered blocks to be uploaded
	GetCoverBlocks(ctx context.Context, seg *types.CreateSegmentReq, startAddr, endAddr, tag int64) (blocks map[int64][]int64, err error)
	// delete the block
	DeleteBlock(ctx context.Context, seg *types.CreateSegmentReq, blockId int64) (err error)
	// update the target file size and blocks number
	UpdateFileSizeAndBlocksNum(ctx context.Context, file *types.CreateSegmentReq, size uint64, blocksNum uint32) (err error)
	// get the file size and blocks number
	GetFileSizeAndBlocksNum(ctx context.Context, file *types.CreateSegmentReq) (size uint64, blocksNum uint32, err error)
}

