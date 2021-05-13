package meta

import (
	"context"
	
	"github.com/hopkings2008/yigfs/server/types"
)


func (m *Meta) ListDirFiles(ctx context.Context, dir *types.GetDirFilesReq) (dirFilesResp []*types.GetDirFileInfo, offset uint64, err error) {
	return m.Client.ListDirFiles(ctx, dir)
}

func (m *Meta) CreateFile(ctx context.Context, file *types.CreateFileReq) (err error) {
	return m.Client.CreateFile(ctx, file)
}

func(m *Meta) GetDirFileInfo(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error) {
	return m.Client.GetDirFileInfo(ctx, file)
}

func(m *Meta) GetFileInfo(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error) {
	return m.Client.GetFileInfo(ctx, file)
}

func(m *Meta) InitRootDirs(ctx context.Context, rootDir *types.InitDirReq) (err error) {
	return m.Client.InitRootDirs(ctx, rootDir) 
}

func(m *Meta) SetFileAttr(ctx context.Context, file *types.SetFileAttrReq) (err error) {
	return m.Client.SetFileAttr(ctx, file)
}

func(m *Meta) UpdateFileSizeAndBlocksNum(ctx context.Context, file *types.GetFileInfoReq, size uint64, blocksNum uint32) (err error) {
	return m.Client.UpdateFileSizeAndBlocksNum(ctx, file, size, blocksNum)
}

func(m *Meta) DeleteFile(ctx context.Context, file *types.DeleteFileReq) (err error) {
	return m.Client.DeleteFile(ctx, file)
}
