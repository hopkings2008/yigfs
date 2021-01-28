package meta

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) GetLeaderInfo(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error) {
	return m.Client.GetLeaderInfo(ctx, leader)
}

func(m *Meta) CreateOrUpdateLeader(ctx context.Context, leader *types.GetLeaderReq) (err error) {
	return m.Client.CreateOrUpdateLeader(ctx, leader)
}
