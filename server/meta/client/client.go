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
        // Create and update root dir
	CreateAndUpdateRootDir(ctx context.Context, rootDir *types.FileInfo) (err error)
	// Get file attr from parent ino
	GetDirFileInfo(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error)
	// Get file info from ino
	GetFileInfo(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error)
}

