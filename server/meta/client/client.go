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
	// create segment info
	CreateSegmentInfo(ctx context.Context, segment *types.CreateSegmentReq) (err error)
	// get covered blocks
	GetCoveredExistedBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (blocks []*types.BlockInfo, err error)
	// deleted blocks
	DeleteBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, blocks []*types.BlockInfo) (err error)
	// insert segment block and check whether it can be merge or not.
	InsertSegmentBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (blockId int64, err error)
	// get merge block info from file_blocks table
	GetMergeBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, fileBlockResp *types.FileBlockInfo, err error)
	// update file block info
	UpdateBlock(ctx context.Context, block *types.FileBlockInfo) (err error)
	// create file block
	CreateFileBlock(ctx context.Context, block *types.FileBlockInfo) (err error)
	// get the block that offset in uploading blocks
	GetOffsetInUploadingBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error)
	// get the existed block that uploading block's offset between it's offset and end_addr
	GetOffsetInExistedBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error)
	// get the existed block that covered the uploading block
	GetCoveredUploadingBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error)
	// deal overlapping blocks
	DealOverlappingBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, updateBlocks []*types.FileBlockInfo, deleteBlocks []*types.FileBlockInfo, insertBlocks []*types.FileBlockInfo) (err error)
	// get all blocks size and number for the target file
	GetFileBlockSize(ctx context.Context, file *types.GetFileInfoReq) (blocksSize uint64, blocksNum uint32, err error)
	// update file size and blocks number
	UpdateFileSizeAndBlocksNum(ctx context.Context, file *types.GetFileInfoReq, size uint64, blocksNum uint32) (err error)
	// get include offset index segments
	GetIncludeOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (segmentMap map[interface{}][]int64, offsetMap map[int64]int64, err error)
	// get greater than offset index segments
	GetGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (segmentMap map[interface{}][]int64, offsetMap map[int64]int64, err error)
	// get segments block info
	GetSegsBlockInfo(ctx context.Context, seg *types.GetSegmentReq, segmentMap map[interface{}][]int64, offsetMap map[int64]int64) (resp *types.GetSegmentResp, err error)
	// update segment block info
	UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error)
	// get incomplete upload segments
	GetIncompleteUploadSegs(ctx context.Context, segInfo *types.GetIncompleteUploadSegsReq, segs []*types.IncompleteUploadSegInfo) (segsResp *types.GetIncompleteUploadSegsResp, err error)
	// update seg's size
	UpdateSegSize(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error)
	// get the slowest growing segment
	GetTheSlowestGrowingSeg(ctx context.Context, segReq *types.GetSegmentReq, segIds []*types.IncompleteUploadSegInfo) (isExisted bool, resp *types.GetTheSlowestGrowingSeg, err error)
	// get blocks by the target segment id
	GetBlocksBySegId(ctx context.Context, seg *types.GetTheSlowestGrowingSeg) (resp *types.GetSegmentResp, err error)
	// get segments by leader
	GetSegsByLeader(ctx context.Context, seg *types.GetIncompleteUploadSegsReq) (segsResp []*types.IncompleteUploadSegInfo, err error)
	// check whether the file has segments or not
	IsFileHasSegments(ctx context.Context, seg *types.GetSegmentReq) (isExisted bool, err error)
	// merge the block in segment_blocks table
	MergeSegmentBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (err error)
	// check whether the block can be merge in segment_blocks table or not.
	IsBlockCanMerge(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isCanMerge bool, resp *types.BlockInfo, err error)
}

