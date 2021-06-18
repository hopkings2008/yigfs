package meta

import (
	"context"
	
	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) GetIncludeOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (getSegs map[interface{}][]*types.BlockInfo, err error) {
	return m.Client.GetIncludeOffsetIndexSegs(ctx, seg, checkOffset)
}

func(m *Meta)GetGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (getSegs map[interface{}][]*types.BlockInfo, err error) {
	return m.Client.GetGreaterOffsetIndexSegs(ctx, seg, checkOffset)
}

func(m *Meta) GetBlocksBySegId(ctx context.Context, seg *types.GetTheSlowestGrowingSeg) (resp *types.GetSegmentResp, err error) {
	return m.Client.GetBlocksBySegId(ctx, seg)
}

func(m *Meta) IsFileHasSegments(ctx context.Context, seg *types.GetSegmentReq) (isExisted bool, err error) {
	return m.Client.IsFileHasSegments(ctx, seg)
}

func(m *Meta) GetAllExistedFileSegs(ctx context.Context, file *types.DeleteFileReq) (segs map[interface{}]struct{}, err error) {
	return m.Client.GetAllExistedFileSegs(ctx, file)
}

func(m *Meta) DeleteFileBlocks(ctx context.Context, file *types.DeleteFileReq) (err error)  {
	return m.Client.DeleteFileBlocks(ctx, file) 
}

func(m *Meta) InsertOrUpdateFileAndSegBlocks(ctx context.Context, segInfo *types.DescriptBlockInfo, segs []*types.CreateBlocksInfo, isUpdateInfo bool, blocksNum int) (err error) {
	return m.Client.InsertOrUpdateFileAndSegBlocks(ctx, segInfo, segs, isUpdateInfo, blocksNum)
}
