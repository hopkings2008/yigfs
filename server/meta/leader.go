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

func(m *Meta) GetSegmentLeaderInfo(ctx context.Context, segment *types.GetSegLeaderReq) (resp *types.LeaderInfo, err error) {
	return m.Client.GetSegmentLeaderInfo(ctx, segment)
}

func(m *Meta) CreateSegmentLeader(ctx context.Context, segment *types.CreateSegmentReq) (err error) {
	return m.Client.CreateSegmentLeader(ctx, segment)
}
