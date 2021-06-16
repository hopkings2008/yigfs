package meta

import (
	"context"
	
	"github.com/hopkings2008/yigfs/server/types"
)


func (m *Meta) GetCoveredExistedBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (blocks []*types.BlockInfo, err error) {
	return m.Client.GetCoveredExistedBlocks(ctx, blockInfo, block)
}

func(m *Meta) DeleteBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, blocks []*types.BlockInfo) (err error) {
	return m.Client.DeleteBlocks(ctx, blockInfo, blocks)
}

func(m *Meta) GetMergeBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, fileBlockResp *types.FileBlockInfo, err error) {
	return m.Client.GetMergeBlock(ctx, blockInfo, block)
}

func(m *Meta) UpdateBlock(ctx context.Context, block *types.FileBlockInfo) (err error) {
	return m.Client.UpdateBlock(ctx, block)
}

func(m *Meta) CreateFileBlock(ctx context.Context, block *types.FileBlockInfo) (err error) {
	return m.Client.CreateFileBlock(ctx, block)
}

func(m *Meta) GetOffsetInUploadingBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error) {
	return m.Client.GetOffsetInUploadingBlock(ctx, blockInfo, block)
}

func(m *Meta) GetOffsetInExistedBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error) {
	return m.Client.GetOffsetInExistedBlock(ctx, blockInfo, block)
}

func(m *Meta) DealOverlappingBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, updateBlocks []*types.FileBlockInfo, deleteBlocks []*types.FileBlockInfo, insertBlocks []*types.FileBlockInfo) (err error) {
	return m.Client.DealOverlappingBlocks(ctx, blockInfo, updateBlocks, deleteBlocks, insertBlocks)
}

func(m *Meta) GetCoveredUploadingBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error) {
	return m.Client.GetCoveredUploadingBlock(ctx, blockInfo, block)
}

func(m *Meta) GetFileBlockSize(ctx context.Context, file *types.GetFileInfoReq) (blocksSize uint64, blocksNum uint32, err error) {
	return m.Client.GetFileBlockSize(ctx, file)
}

func(m *Meta) GetIncludeOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (segmentMap map[interface{}][]int64, offsetMap map[int64]int64, err error) {
	return m.Client.GetIncludeOffsetIndexSegs(ctx, seg, checkOffset)
}

func(m *Meta) GetGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (segmentMap map[interface{}][]int64, offsetMap map[int64]int64, err error) {
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

func(m *Meta) InsertOrUpdateBlock(ctx context.Context, block *types.FileBlockInfo) (err error) {
	return m.Client.InsertOrUpdateBlock(ctx, block)
}
