package meta

import (
        "context"

        "github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) GetFileSegmentInfo(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error) {
        return m.Client.GetFileSegmentInfo(ctx, seg)
}

func(m *Meta) CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq) (mergeNumber int, err error) {
	return m.Client.CreateFileSegment(ctx, seg)
}

func(m *Meta) GetCoveredExistedBlocks(ctx context.Context, seg *types.CreateSegmentReq, startAddr, endAddr, tag int64) (blocks map[int64][]int64, err error) {
	return m.Client.GetCoveredExistedBlocks(ctx, seg, startAddr, endAddr, tag)
}

func(m *Meta) DeleteBlock(ctx context.Context, seg *types.CreateSegmentReq, blockId int64) (err error) {
	return m.Client.DeleteBlock(ctx, seg, blockId)
}

func(m *Meta) GetCoverBlocks(ctx context.Context, seg *types.CreateSegmentReq, startAddr, endAddr, tag int64) (blocks map[int64][]int64, err error) {
	return m.Client.GetCoverBlocks(ctx, seg, startAddr, endAddr, tag)
}
