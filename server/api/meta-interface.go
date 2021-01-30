package api

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


type YigFsLayer interface {
	ListDirFiles(ctx context.Context, dir *types.GetDirFilesReq) (listDirFilesResp []*types.GetDirFileInfo, offset uint64, err error)
	GetDirFileAttr(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error)
	GetFileAttr(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error)
	InitDirAndZone(ctx context.Context, rootDir *types.InitDirReq) (err error)
	GetLeader(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error)
	CreateOrUpdateLeader(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error)
	CreateFile(ctx context.Context, file *types.CreateFileReq) (resp *types.CreateFileResp, err error)
	SetFileAttr(ctx context.Context, file *types.SetFileAttrReq) (resp *types.SetFileAttrResp, err error)
}
