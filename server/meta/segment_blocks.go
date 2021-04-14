package meta

import (
	"context"
	
	"github.com/hopkings2008/yigfs/server/types"
)


func (m *Meta) InsertSegmentBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (blockId int64, isCanMerge bool, err error) {
	return m.Client.InsertSegmentBlock(ctx, blockInfo, block)
}

func(m *Meta) GetSegsBlockInfo(ctx context.Context, seg *types.GetSegmentReq, segmentMap map[interface{}][]int64, offsetMap map[int64]int64) (resp *types.GetSegmentResp, err error) {
	return m.Client.GetSegsBlockInfo(ctx, seg, segmentMap, offsetMap)
}