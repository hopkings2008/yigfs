package meta

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) GetSegmentInfo(ctx context.Context, segment *types.GetSegLeaderReq) (resp *types.LeaderInfo, err error) {
	return m.Client.GetSegmentInfo(ctx, segment)
}

func(m *Meta) CreateSegmentInfo(ctx context.Context, segment *types.CreateSegmentReq) (err error) {
	return m.Client.CreateSegmentInfo(ctx, segment)
}

func(m *Meta) UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	return m.Client.UpdateSegBlockInfo(ctx, seg)
}