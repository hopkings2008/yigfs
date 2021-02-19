package meta

import (
        "context"

        "github.com/hopkings2008/yigfs/server/types"
)


func (m *Meta) GetFileSegmentInfo(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error) {
        return m.Client.GetFileSegmentInfo(ctx, seg)
}

func(m *Meta) CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq) (err error) {
	return m.Client.CreateFileSegment(ctx, seg)
}
