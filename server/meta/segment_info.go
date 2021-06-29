package meta

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) CreateSegmentInfoAndZoneInfo(ctx context.Context, segment *types.CreateSegmentReq, maxSize int) (err error) {
	return m.Client.CreateSegmentInfoAndZoneInfo(ctx, segment, maxSize)
}

func(m *Meta) UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	return m.Client.UpdateSegBlockInfo(ctx, seg)
}

func(m *Meta) GetIncompleteUploadSegs(ctx context.Context, segInfo *types.GetIncompleteUploadSegsReq, segs []*types.IncompleteUploadSegInfo) (segsResp *types.GetIncompleteUploadSegsResp, err error) {
	return m.Client.GetIncompleteUploadSegs(ctx, segInfo, segs)
}

func(m *Meta) GetTheSlowestGrowingSeg(ctx context.Context, segReq *types.GetSegmentReq, segIds []*types.IncompleteUploadSegInfo) (isExisted bool, resp *types.SegmentInfo, err error) {
	return m.Client.GetTheSlowestGrowingSeg(ctx, segReq, segIds)
}

func(m *Meta) DeleteSegInfo(ctx context.Context, file *types.DeleteFileReq, segs map[interface{}][]int) (err error) {
	return m.Client.DeleteSegInfo(ctx, file, segs) 
}