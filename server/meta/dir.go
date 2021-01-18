package meta

import (
        "context"

        "github.com/hopkings2008/yigfs/server/types"
)


func (m *Meta) ListDirFiles(ctx context.Context, files *types.GetDirFilesReq) (dirFilesResp []*types.GetDirFilesResp, err error) {
        return m.Client.ListDirFiles(ctx, files)
}

func (m *Meta) CreateFile(ctx context.Context, file *types.CreateDirFileReq) (resp *types.CreateDirFileResp, err error) {
        return m.Client.CreateFile(ctx, file)
}

func(m *Meta) CreateAndUpdateRootDir(ctx context.Context, rootDir *types.CreateDirFileReq) (err error) {
        return m.Client.CreateAndUpdateRootDir(ctx, rootDir)
}
