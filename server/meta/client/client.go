package client

import (
        "context"

        "github.com/hopkings2008/yigfs/server/types"
)

// DB Client Interface
type Client interface {
        // List dir files
	ListDirFiles(ctx context.Context, files *types.GetDirFilesReq) (dirFilesResp []*types.GetDirFilesResp, err error)
        // Create file
        CreateFile(ctx context.Context, file *types.CreateDirFileReq) (resp *types.CreateDirFileResp, err error)
        // Create and update root dir
        CreateAndUpdateRootDir(ctx context.Context, rootDir *types.CreateDirFileReq) (err error)
}

