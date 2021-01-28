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
	CreateFile(ctx context.Context, file *types.FileInfo) (err error)
        // Init root dir
	InitRootDir(ctx context.Context, rootDir *types.InitDirReq) (err error)
	//Init root parent dir
	InitParentDir(ctx context.Context, rootDir *types.InitDirReq) (err error)
	// Get file attr from parent ino
	GetDirFileInfo(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error)
	// Get file info from ino
	GetFileInfo(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error)
	// Create or update zone
	CreateOrUpdateZone(ctx context.Context, zone *types.InitDirReq) (err error)
	// Get leader
	GetLeaderInfo(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error)
	// Create or update leader
	CreateOrUpdateLeader(ctx context.Context, leader *types.GetLeaderReq) (err error)
	// Get one update machine
	GetOneUpMachine(ctx context.Context, zone *types.GetLeaderReq) (leader string, err error)
	// Get machine indo
	GetMachineInfo(ctx context.Context, zone *types.GetLeaderReq) (resp *types.GetMachineInfoResp, err error)
}

