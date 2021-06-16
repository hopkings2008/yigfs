package storage

import (
	"context"
	"fmt"
	"sync"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/types"
)

var (
	waitgroup sync.WaitGroup
)

func getIncludeOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64, 
	yigFs *YigFsStorage) (segmentsMap map[interface{}][]int64, offsetMap map[int64]int64, err error) {
	segmentsMap, offsetMap, err = yigFs.MetaStorage.Client.GetIncludeOffsetIndexSegs(ctx, seg, checkOffset)
	if err != nil && err != ErrYigFsNoTargetSegment {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to getIncludeOffsetIndexSegs, region: %s, bucket: %s, ino: %d, generation: %d, checkOffset: %d",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, checkOffset))
		return segmentsMap, offsetMap, err
	} else if err == ErrYigFsNoTargetSegment || len(segmentsMap) == 0 {
		return segmentsMap, offsetMap, nil
	}

	return
}

func getGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64, 
	yigFs *YigFsStorage) (segmentsMap map[interface{}][]int64, offsetMap map[int64]int64, err error) {
	segmentsMap, offsetMap, err = yigFs.MetaStorage.Client.GetGreaterOffsetIndexSegs(ctx, seg, checkOffset)
	if err != nil && err != ErrYigFsNoTargetSegment {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to getGreaterOffsetIndexSegs, region: %s, bucket: %s, ino: %d, generation: %d, checkOffset: %d",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, checkOffset))
		return segmentsMap, offsetMap, err
	} else if err == ErrYigFsNoTargetSegment || len(segmentsMap) == 0 {
		return segmentsMap, offsetMap, nil
	}

	return
}

func (yigFs *YigFsStorage) GetFileSegmentsInfo(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error) {
	var checkOffset int64 = 0

	if seg.Offset > 0 {
		checkOffset = seg.Offset

		if seg.Size > 0 {
			checkOffset = seg.Offset + int64(seg.Size)
		}
	}

	resp = &types.GetSegmentResp {}

	if checkOffset > 0 {
		var includeSegs = make(map[interface{}][]int64)
		var includeOffset = make(map[int64]int64)
		waitgroup.Add(1)
		go func() {
			defer waitgroup.Done()
			includeSegs, includeOffset, err = getIncludeOffsetIndexSegs(ctx, seg, checkOffset, yigFs)
			if err != nil {
				return
			}
		}()

		greaterSegs, greaterOffset, err := getGreaterOffsetIndexSegs(ctx, seg, checkOffset, yigFs)
		if err != nil {
			waitgroup.Wait()
			return resp, err
		}

		waitgroup.Wait()

		for segmentId, includeBlocks := range includeSegs {
			isSegIdEqual := false
			includeSegIds := segmentId.([2]uint64)
			for segmentId, greaterBlocks := range greaterSegs {
				greaterSegIds := segmentId.([2]uint64)
				if includeSegIds[0] == greaterSegIds[0] && includeSegIds[1] == greaterSegIds[1] {
					greaterSegs[segmentId] = append(includeBlocks, greaterBlocks...)
					for blockId, offset := range includeOffset{
						greaterOffset[blockId] = offset
					}
					isSegIdEqual = true
					break
				}
			}

			if !isSegIdEqual {
				greaterSegs[segmentId] = includeBlocks
				for blockId, offset := range includeOffset {
					greaterOffset[blockId] = offset
				}
			}
		}

		helper.Logger.Info(ctx, fmt.Sprintf("req: greaterSegs: %v, includeSegs: %v", greaterSegs, includeSegs))
		getGreatherBlocksResp, err := yigFs.MetaStorage.Client.GetSegsBlockInfo(ctx, seg, greaterSegs, greaterOffset)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("getGreaterOffsetIndexSegs: Failed to get blocks info, region: %s, bucket: %s, ino: %d, generation: %d",
				seg.Region, seg.BucketName, seg.Ino, seg.Generation))
			return resp, err
		}

		resp.Segments = getGreatherBlocksResp.Segments
	
	} else {
		greaterSegs, greaterOffset, err := getGreaterOffsetIndexSegs(ctx, seg, checkOffset, yigFs)
		if err != nil {
			return resp, err
		}

		getGreatherBlocksResp, err := yigFs.MetaStorage.Client.GetSegsBlockInfo(ctx, seg, greaterSegs, greaterOffset)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("getGreaterOffsetIndexSegs: Failed to get blocks info, region: %s, bucket: %s, ino: %d, generation: %d",
				seg.Region, seg.BucketName, seg.Ino, seg.Generation))
			return resp, err
		}

		resp.Segments = getGreatherBlocksResp.Segments
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get segment info, region: %s, bucket: %s, ino: %d, generation: %d, checkOffset: %d",
		seg.Region, seg.BucketName, seg.Ino, seg.Generation, checkOffset))
	return
}

func dealPartialOverlapBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo, yigFs *YigFsStorage) (err error) {
	isGetInUploading, blockResp, err := yigFs.MetaStorage.Client.GetOffsetInUploadingBlock(ctx, blockInfo, block)
	if err != nil {
		return
	}

	isGetInExisted, existedResp, err := yigFs.MetaStorage.Client.GetOffsetInExistedBlock(ctx, blockInfo, block)
	if err != nil {
		return
	}

	updateBlocks := make([]*types.FileBlockInfo, 0)
	insertBlocks := make([]*types.FileBlockInfo, 0)
	deleteBlocks := make([]*types.FileBlockInfo, 0)

	var size int
	if isGetInExisted {
		size = int(block.Offset - existedResp.Offset)
		updateExistedBlock := &types.FileBlockInfo{
			BlockId:          existedResp.BlockId,
			Offset:           existedResp.Offset,
			Size:             size,
			FileBlockEndAddr: block.Offset,
		}
		updateBlocks = append(updateBlocks, updateExistedBlock)

		size = int(existedResp.FileBlockEndAddr - block.Offset)
		deleteExistedBlock := &types.FileBlockInfo{
			SegmentId0:       existedResp.SegmentId0,
			SegmentId1:       existedResp.SegmentId1,
			BlockId:          existedResp.BlockId,
			Offset:           block.Offset,
			FileBlockEndAddr: existedResp.FileBlockEndAddr,
			Size:             size,
			Ctime:            existedResp.Ctime,
		}
		helper.Logger.Info(ctx, fmt.Sprintf("dealPartialOverlapBlocks, Ctime: %v, blockId: %v", existedResp.Ctime, block.BlockId))
		deleteBlocks = append(deleteBlocks, deleteExistedBlock)
	}

	if isGetInUploading {
		size = int(block.FileBlockEndAddr - blockResp.Offset)
		deleteBlock := &types.FileBlockInfo{
			SegmentId0:       blockResp.SegmentId0,
			SegmentId1:       blockResp.SegmentId1,
			BlockId:          blockResp.BlockId,
			Offset:           blockResp.Offset,
			FileBlockEndAddr: block.FileBlockEndAddr,
			Size:             size,
			Ctime:            blockResp.Ctime,
		}
		deleteBlocks = append(deleteBlocks, deleteBlock)

		size = int(blockResp.FileBlockEndAddr - block.FileBlockEndAddr)
		insertBlock := &types.FileBlockInfo{
			SegmentId0:       blockResp.SegmentId0,
			SegmentId1:       blockResp.SegmentId1,
			BlockId:          blockResp.BlockId,
			Offset:           block.FileBlockEndAddr,
			FileBlockEndAddr: blockResp.FileBlockEndAddr,
			Size:             size,
			Ctime:            blockResp.Ctime,
		}
		helper.Logger.Info(ctx, fmt.Sprintf("dealPartialOverlapBlocks, Ctime: %v, blockId: %v", blockResp.Ctime, block.BlockId))
		insertBlocks = append(insertBlocks, insertBlock)
	}

	err = yigFs.MetaStorage.Client.DealOverlappingBlocks(ctx, blockInfo, updateBlocks, deleteBlocks, insertBlocks)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to dealPartialOverlapBlocks, offset: %v, blockId: %v", block.Offset, block.BlockId))
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to dealPartialOverlapBlocks, offset: %v", block.Offset))
	return
}

func dealFullCoveredUploadingBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo, yigFs *YigFsStorage) (err error) {
	helper.Logger.Info(ctx, fmt.Sprintf("start to dealFullCoveredUploadingBlocks, offset: %v", block.Offset))
	isExisted, blockResp, err := yigFs.MetaStorage.Client.GetCoveredUploadingBlock(ctx, blockInfo, block)
	if err != nil {
		return
	}
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to GetCoveredUploadingBlock, isExisted: %v", isExisted))

	updateBlocks := make([]*types.FileBlockInfo, 0)
	insertBlocks := make([]*types.FileBlockInfo, 0)
	deleteBlocks := make([]*types.FileBlockInfo, 0)

	var size int
	if isExisted {
		if blockResp.Offset != block.Offset {
			size = int(block.Offset - blockResp.Offset)
			updateExistedBlock := &types.FileBlockInfo{
				BlockId:          blockResp.BlockId,
				Offset:           blockResp.Offset,
				Size:             size,
				FileBlockEndAddr: block.Offset,
			}
			updateBlocks = append(updateBlocks, updateExistedBlock)
		}

		deleteExistedBlock := &types.FileBlockInfo{
			SegmentId0:       blockResp.SegmentId0,
			SegmentId1:       blockResp.SegmentId1,
			BlockId:          blockResp.BlockId,
			Offset:           block.Offset,
			FileBlockEndAddr: block.FileBlockEndAddr,
			Size:             block.Size,
			Ctime:            blockResp.Ctime,
		}
		helper.Logger.Info(ctx, fmt.Sprintf("dealFullCoveredUploadingBlocks, Ctime: %v, blockId: %v", blockResp.Ctime, block.BlockId))
		deleteBlocks = append(deleteBlocks, deleteExistedBlock)

		if blockResp.FileBlockEndAddr != block.FileBlockEndAddr {
			size = int(blockResp.FileBlockEndAddr - block.FileBlockEndAddr)
			insertExistedBlock := &types.FileBlockInfo{
				SegmentId0:       blockResp.SegmentId0,
				SegmentId1:       blockResp.SegmentId1,
				BlockId:          blockResp.BlockId,
				Offset:           block.FileBlockEndAddr,
				FileBlockEndAddr: blockResp.FileBlockEndAddr,
				Size:             size,
				Ctime:            blockResp.Ctime,
			}
			insertBlocks = append(insertBlocks, insertExistedBlock)
		}

		err = yigFs.MetaStorage.Client.DealOverlappingBlocks(ctx, blockInfo, updateBlocks, deleteBlocks, insertBlocks)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to dealFullCoveredUploadingBlocks, offset: %v, blockId: %v", block.Offset, block.BlockId))
			return
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to dealFullCoveredUploadingBlocks, offset: %v", block.Offset))
	return
}

func checkCoveredExistedBlocksAndDeleted(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo, yigFs *YigFsStorage) (err error) {
	coveredBlocks, err := yigFs.MetaStorage.Client.GetCoveredExistedBlocks(ctx, blockInfo, block)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetCoveredExistedBlocks, offset: %v", block.Offset))
		return
	}

	if len(coveredBlocks) == 0 {
		return
	}

	err = yigFs.MetaStorage.Client.DeleteBlocks(ctx, blockInfo, coveredBlocks)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to deleted blocks, coveredBlocks: %v", coveredBlocks))
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("deletedBlocks is: %v", coveredBlocks))
	return
}

func insertSegAndFileBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo, yigFs *YigFsStorage) (err error) {
	// insert segment block
	blockId, err := yigFs.MetaStorage.Client.InsertSegmentBlock(ctx, blockInfo, block)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to InsertSegmentBlock, offset: %v", block.Offset))
		return
	}

	//insert file block
	blockReq := &types.FileBlockInfo{
		Region:     blockInfo.Region,
		BucketName: blockInfo.BucketName,
		Ino:        blockInfo.Ino,
		Generation: blockInfo.Generation,
		SegmentId0: blockInfo.SegmentId0,
		SegmentId1: blockInfo.SegmentId1,
		BlockId: blockId,
		Size: block.Size,
		Offset: block.Offset,
		FileBlockEndAddr: block.FileBlockEndAddr,
	}
	err = yigFs.MetaStorage.Client.CreateFileBlock(ctx, blockReq)
	if err != nil {
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to insert file block, offset: %v, blockId: %v", block.Offset, block.BlockId))
	return
}

func mergeSegAndFileBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block, mergeSegBlock *types.BlockInfo, 
	mergeFileBlock *types.FileBlockInfo, yigFs *YigFsStorage) (err error) {
	// merge segment block
	mergeSegBlockReq := &types.BlockInfo {
		Size: mergeSegBlock.Size + block.Size,
		SegEndAddr: block.SegStartAddr + block.Size,
		BlockId: block.BlockId,
	}
	err = yigFs.MetaStorage.Client.MergeSegmentBlock(ctx, blockInfo, mergeSegBlockReq)
	if err != nil {
		return err
	}

	// merge file block
	blockReq := &types.FileBlockInfo{
		Region:     blockInfo.Region,
		BucketName: blockInfo.BucketName,
		Ino:        blockInfo.Ino,
		Generation: blockInfo.Generation,
		Offset: mergeFileBlock.Offset,
		BlockId: block.BlockId,
		Size: mergeFileBlock.Size + block.Size,
		FileBlockEndAddr: mergeFileBlock.FileBlockEndAddr + int64(block.Size),
	}
	err = yigFs.MetaStorage.Client.UpdateBlock(ctx, blockReq)
	if err != nil {
		return err
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to merge file and segment block, offset: %v, blockId: %v", block.Offset, block.BlockId))
	return nil
}

func insertOrMergeFileandSegBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo, yigFs *YigFsStorage) (err error) {
	// check whether the block can be merge in segment_blocks table or not.
	isCanMerge, segBlock, err := yigFs.MetaStorage.Client.IsBlockCanMerge(ctx, blockInfo, block)
	if err != nil {
		return err
	}

	if isCanMerge {
		// if can merge, then check whether the block can be merge in file_blocks table or not.
		block.BlockId = segBlock.BlockId
		isExisted, fileBlock, err := yigFs.MetaStorage.Client.GetMergeBlock(ctx, blockInfo, block)
		if err != nil {
			return err
		}

		if isExisted {
			// if can merge, then merge it into segment_blocks table and file_blocks table.
			err = mergeSegAndFileBlock(ctx, blockInfo, block, segBlock, fileBlock, yigFs)
			if err != nil {
				return err
			}

			helper.Logger.Info(ctx, fmt.Sprintf("Succeed to merge the block into file_blocks and segment_blocks tables, offset: %v", block.Offset))
			return nil
		}
	}

	// if it not merge in segment_blocks table or file_blocks table, then insert it into segment_blocks table and file_blocks table.
	err = insertSegAndFileBlock(ctx, blockInfo, block, yigFs)
	if err != nil {
		return err
	}

	return
}

func insertOrUpdateFileBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo, yigFs *YigFsStorage) (err error) {
	// insert segment block
	blockId, err := yigFs.MetaStorage.Client.InsertSegmentBlock(ctx, blockInfo, block)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to InsertSegmentBlock, offset: %v", block.Offset))
		return
	}

	//insert file block
	blockReq := &types.FileBlockInfo {
		Region:     blockInfo.Region,
		BucketName: blockInfo.BucketName,
		Ino:        blockInfo.Ino,
		Generation: blockInfo.Generation,
		SegmentId0: blockInfo.SegmentId0,
		SegmentId1: blockInfo.SegmentId1,
		BlockId: blockId,
		Size: block.Size,
		Offset: block.Offset,
		FileBlockEndAddr: block.FileBlockEndAddr,
	}
	err = yigFs.MetaStorage.Client.InsertOrUpdateBlock(ctx, blockReq)
	if err != nil {
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to insert seg block and insert/update file block, offset: %v, blockId: %v", block.Offset, block.BlockId))
	return
}

func (yigFs *YigFsStorage) CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq, isLeaderExisted int) (err error) {
	// Perform the following operations for each block:
	// 1. get existed blocks fully covered by the uploading block, then deleted them.
	// 2. deal partial overlap blocks by the uploading block.
	// 3. deal fully covered uploading blocks.
	// 4. check the uploading block in segment_blocks table and file_blocks table can be merged or not.
	// if it can merge, merge it into segment_blocks table and file_blocks table.
	// else insert it into segment_blocks table and file_blocks table.

	blocksNum := len(seg.Segment.Blocks)
	if blocksNum == 0 {
		helper.Logger.Warn(ctx, "No blocks to upload")
		return
	}

	// if the seg leader is not existed, create it.
	// update size for segment_info.
	waitgroup.Add(1)
	go func() {
		defer waitgroup.Done()
		if isLeaderExisted == types.NotExisted {
			err = yigFs.MetaStorage.Client.CreateSegmentInfo(ctx, seg)
			if err != nil {
				return
			}
		}

		// get max end_addr and update max_end_addr.
		maxEnd := seg.Segment.Blocks[blocksNum-1].SegEndAddr
		if blocksNum > 1 {
			for _, block := range seg.Segment.Blocks {
				if block.SegEndAddr > maxEnd {
					maxEnd = block.SegEndAddr
				}
			}
		}

		segInfo := &types.UpdateSegBlockInfo{
			SegmentId0: seg.Segment.SegmentId0,
			SegmentId1: seg.Segment.SegmentId1,
			Size: maxEnd,
		}

		updateReq := &types.UpdateSegBlockInfoReq{
			ZoneId: seg.ZoneId,
			Region: seg.Region,
			BucketName: seg.BucketName,
			SegBlockInfo: segInfo,
		}

		err = yigFs.MetaStorage.Client.UpdateSegSize(ctx, updateReq)
		if err != nil {
			return
		}
	}()

	blockInfo := &types.DescriptBlockInfo{
		Region:     seg.Region,
		BucketName: seg.BucketName,
		Ino:        seg.Ino,
		Generation: seg.Generation,
		SegmentId0: seg.Segment.SegmentId0,
		SegmentId1: seg.Segment.SegmentId1,
	}

	for _, block := range seg.Segment.Blocks {
		// 1. get existed blocks fully covered by the uploading block, then deleted them.
		block.FileBlockEndAddr = block.Offset + int64(block.Size)

		waitgroup.Add(1)
		go func() {
			defer waitgroup.Done()
			err = checkCoveredExistedBlocksAndDeleted(ctx, blockInfo, block, yigFs)
			if err != nil {
				return
			}
		}()

		// 2. deal partial overlap blocks by the uploading block.
		waitgroup.Add(1)
		go func() {
			defer waitgroup.Done()
			err = dealPartialOverlapBlocks(ctx, blockInfo, block, yigFs)
			if err != nil {
				return
			}
		}()

		// 3. deal fully covered uploading blocks.
		err = dealFullCoveredUploadingBlocks(ctx, blockInfo, block, yigFs)
		if err != nil {
			waitgroup.Wait()
			return err
		}

		// 4. check the uploading block in segment_blocks table and file_blocks table can be merged or not.
		// if it can merge, merge it into segment_blocks table and file_blocks table.
		// else insert it into segment_blocks table and file_blocks table.
		//err = insertOrMergeFileandSegBlock(ctx, blockInfo, block, yigFs)
		err = insertOrUpdateFileBlocks(ctx, blockInfo, block, yigFs)
		if err != nil {
			waitgroup.Wait()
			return err
		}

		waitgroup.Wait()
	}

	return
}

func(yigFs *YigFsStorage) UpdateFileSizeAndBlock(ctx context.Context, file *types.GetFileInfoReq) (err error) {
	// get all block size and blocks number.
	allSize, allNumber, err := yigFs.MetaStorage.Client.GetFileBlockSize(ctx, file)
	if err != nil {
		return err
	}
	// update file size and blocks number.
	err = yigFs.MetaStorage.Client.UpdateFileSizeAndBlocksNum(ctx, file, allSize, allNumber)
	if err != nil {
		return
	}

	return
}

func(yigFs *YigFsStorage) GetTheSlowestGrowingSeg(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error) {
	resp = &types.GetSegmentResp{}
	segReq := &types.GetIncompleteUploadSegsReq {
		ZoneId: seg.ZoneId,
		Region: seg.Region,
		BucketName: seg.BucketName,
		Machine: seg.Machine,
	}
	segIds, err := yigFs.MetaStorage.Client.GetSegsByLeader(ctx, segReq)
	switch err {
	case ErrYigFsNoTargetSegment:
		resp.Segments = make([]*types.SegmentInfo, 0)
		helper.Logger.Warn(ctx, fmt.Sprintf("getSegsByLeader is None, zone: %v, region: %v, bucket: %v, machine: %v", 
			seg.ZoneId, seg.Region, seg.BucketName, seg.Machine))
		return resp, nil
	case nil:
		// 1. get the slowest growing segment.
		isExisted, segInfo, getErr := yigFs.MetaStorage.Client.GetTheSlowestGrowingSeg(ctx, seg, segIds)
		if err != nil {
			return resp, getErr
		}

		if !isExisted {
			resp.Segments = make([]*types.SegmentInfo, 0)
			helper.Logger.Warn(ctx, fmt.Sprintf("getTheSlowestGrowingSeg is None, zone: %v, region: %v, bucket: %v, machine: %v", 
				seg.ZoneId, seg.Region, seg.BucketName, seg.Machine))
			return resp, nil
		}

		// 2. get all the blocks info for the slowest growing segment. 
		segInfo.Leader = seg.Machine
		resp, err = yigFs.MetaStorage.Client.GetBlocksBySegId(ctx, segInfo)
		if err != nil {
			return
		}

		helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get the slowest growing seg, zone: %v, region: %s, bucket: %s, machine: %v",
			seg.ZoneId, seg.Region, seg.BucketName, seg.Machine))
		return
	default:
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the slowest growing seg, zone: %v, region: %s, bucket: %s, machine: %v",
			seg.ZoneId, seg.Region, seg.BucketName, seg.Machine))
		return
	}
}

func(yigFs *YigFsStorage) IsFileHasSegments(ctx context.Context, seg *types.GetSegmentReq) (isExisted bool, err error) {
	isExisted, err = yigFs.MetaStorage.Client.IsFileHasSegments(ctx, seg)
	if err != nil {
		return
	}
	return
}
