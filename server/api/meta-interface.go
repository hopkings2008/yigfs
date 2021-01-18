package api

import (
	"context"
	"github.com/hopkings2008/yigfs/server/types"
)


type YigFsLayer interface {
	ListDirFiles(ctx context.Context, files *types.GetDirFilesReq) (resp *types.YigFsMetaResp)
}
