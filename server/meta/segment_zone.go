package meta

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) GetSegmentLeader(ctx context.Context, segment *types.GetSegLeaderReq) (resp *types.LeaderInfo, err error) {
	return m.Client.GetSegmentLeader(ctx, segment)
}

func(m *Meta) GetSegsByLeader(ctx context.Context, seg *types.GetIncompleteUploadSegsReq) (segsResp []*types.IncompleteUploadSegInfo, err error) {
	return m.Client.GetSegsByLeader(ctx, seg)
}