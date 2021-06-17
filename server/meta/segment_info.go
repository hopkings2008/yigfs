package meta

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) CreateSegmentInfo(ctx context.Context, segment *types.CreateSegmentReq) (err error) {
	return m.Client.CreateSegmentInfo(ctx, segment)
}

func(m *Meta) UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	return m.Client.UpdateSegBlockInfo(ctx, seg)
}

func(m *Meta) GetIncompleteUploadSegs(ctx context.Context, segInfo *types.GetIncompleteUploadSegsReq, segs []*types.IncompleteUploadSegInfo) (segsResp *types.GetIncompleteUploadSegsResp, err error) {
	return m.Client.GetIncompleteUploadSegs(ctx, segInfo, segs)
}

func(m *Meta) UpdateSegSize(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	return m.Client.UpdateSegSize(ctx, seg)
}

func(m *Meta) GetTheSlowestGrowingSeg(ctx context.Context, segReq *types.GetSegmentReq, segIds []*types.IncompleteUploadSegInfo) (isExisted bool, resp *types.GetTheSlowestGrowingSeg, err error) {
	return m.Client.GetTheSlowestGrowingSeg(ctx, segReq, segIds)
}

func(m *Meta) DeleteSegInfo(ctx context.Context, file *types.DeleteFileReq, segs map[interface{}]struct{}) (err error) {
	return m.Client.DeleteSegInfo(ctx, file, segs) 
}