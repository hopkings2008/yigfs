package meta

import (
	"context"
	
	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) GetSegsBlockInfo(ctx context.Context, seg *types.GetSegmentReq, segs map[interface{}][]*types.BlockInfo) (resp *types.GetSegmentResp, err error) {
	return m.Client.GetSegsBlockInfo(ctx, seg, segs)
}

func(m *Meta) DeleteSegBlocks(ctx context.Context, segs map[interface{}][]int) (err error) {
	return m.Client.DeleteSegBlocks(ctx, segs)
}

func(m *Meta) RemoveSegBlocks(ctx context.Context, segs []*types.CreateBlocksInfo, blocksNum int) (err error) {
	return m.Client.RemoveSegBlocks(ctx, segs, blocksNum)
}