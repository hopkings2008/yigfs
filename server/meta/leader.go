package meta

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) GetFileLeaderInfo(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error) {
	return m.Client.GetFileLeaderInfo(ctx, leader)
}

func(m *Meta) CreateOrUpdateFileLeader(ctx context.Context, leader *types.GetLeaderReq) (err error) {
	return m.Client.CreateOrUpdateFileLeader(ctx, leader)
}
