package storage

import (
	"context"
	"fmt"
	"sync"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/bwmarrin/snowflake"
)


var (
	waitgroup sync.WaitGroup
)

func(yigFs *YigFsStorage) GetFileSegmentInfo(ctx context.Context, file *types.GetSegmentReq) (resp *types.GetSegmentResp, err error)  {
	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}
	var  startBlockId int

	getSegInfoResp, err := yigFs.MetaStorage.Client.GetFileSegmentInfo(ctx, file)
	if err != nil && err != ErrYigFsNoTargetSegment {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get segment info, region: %s, bucket: %s, ino: %d, generation: %d, offset: %d, size: %d",
			file.Region, file.BucketName, file.Ino, file.Generation, file.Offset, file.Size))
		return resp, err
	} else if err == ErrYigFsNoTargetSegment || len(getSegInfoResp.Segments) == 0 {
		return resp, nil
	}

	if file.Offset > 0 {
		fileSize := file.Offset
		
		if file.Size > 0 {
			fileSize = file.Offset + int64(file.Size)
		}
		
		segments := getSegInfoResp.Segments
		for _, seg := range segments {
			blocks := seg.Blocks
			for key, block := range blocks {
				startBlockId = -1
				if block.SegStartAddr < fileSize && block.SegEndAddr >= fileSize {
					startBlockId = key
					break
				} else if block.SegStartAddr >= fileSize {
					startBlockId = key
					break
				}
			}

			if startBlockId != -1 {
				segment := &types.SegmentInfo {
					Blocks: []types.BlockInfo{},
				}

				segment.SegmentId0 = seg.SegmentId0
				segment.SegmentId1 = seg.SegmentId1
				segment.Leader = seg.Leader
				segment.MaxSize = seg.MaxSize
				segment.Blocks = blocks[startBlockId:]

				resp.Segments = append(resp.Segments, segment)
			}
		}

		helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get segment info, region: %s, bucket: %s, ino: %d, generation: %d, offset: %d, size: %d",
			file.Region, file.BucketName, file.Ino, file.Generation, file.Offset, file.Size))
		return resp, nil
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get segment info, region: %s, bucket: %s, ino: %d, generation: %d, offset: %d, size: %d", 
		file.Region, file.BucketName, file.Ino, file.Generation, file.Offset, file.Size))
	return getSegInfoResp, nil
}

func findCheckIndex(ctx context.Context, seg *types.CreateSegmentReq) (minStart, maxStart, minEnd, maxEnd int64, increasedSize uint64, increasedNum uint32) {
	uploadingBlocksNum := len(seg.Segment.Blocks)

	minStart = seg.Segment.Blocks[0].Offset
	maxEnd = seg.Segment.Blocks[0].Offset + int64(seg.Segment.Blocks[0].Size)
	maxStart = seg.Segment.Blocks[0].Offset
	minEnd = seg.Segment.Blocks[0].Offset + int64(seg.Segment.Blocks[0].Size)

	increasedSize = uint64(seg.Segment.Blocks[0].Size)
	increasedNum = uint32(uploadingBlocksNum)

	if uploadingBlocksNum > 1 {
		for i := 1; i < uploadingBlocksNum; i++ {
			start := seg.Segment.Blocks[i].Offset
			if start < minStart {
				minStart = start
			} else if start > maxStart {
				maxStart = start
			}

			end := seg.Segment.Blocks[i].Offset + int64(seg.Segment.Blocks[i].Size)
			if end < minEnd {
				minEnd = end
			} else if end > maxEnd {
				maxEnd = end
			}

			increasedSize += uint64(seg.Segment.Blocks[i].Size)
		}
	}
	return
}

func(yigFs *YigFsStorage) CreateSegmentInfo(ctx context.Context, seg *types.CreateSegmentReq, isExisted int) (err error) {
	// 1. Find the min and max index for the blocks to be uploaded.
	// all the blocks size and blocks number marked as increased.
	// 2. Find already existed blocks contained by the block to be uploaded, then deleted them, and mark the blocks size and blocks number as decreased.
	// 3. Find already existed blocks that contained the block to be uploaded, this blocks to be uploaded's size and blocks number marked as decreased.
	// 4. upload blocks or merge blocks. if merge blocks, the blocks number marked as decreased.
	// 5. if the segment does not have leader, create it.
	// 6. get the file size and calculate new file size and blocks number.
	// 7. finally update the file size and blocks number.

	if len(seg.Segment.Blocks) == 0 {
		helper.Logger.Warn(ctx, "No blocks to upload")
		return
	}

	node, createBlockErr := snowflake.NewNode(1)
	if createBlockErr != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create blockId, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	tag := int64(node.Generate())

	// 1. Find the min and max index for the blocks to be uploaded.
	// all the blocks size and blocks number marked as increased.
	minStart, maxStart, minEnd, maxEnd, allBlocksSize, allBlocksNumber := findCheckIndex(ctx, seg)

	// 2. Find already existed blocks contained by the block to be uploaded, then deleted them, and mark the blocks size and blocks number as decreased.
	var coveredExistedBlocks = make([]uint64, 0)
	waitgroup.Add(1)
	go func() {
		err = foundCoveredExistedBlocksAndDeleted(ctx, seg, yigFs, minStart, maxEnd, tag, &coveredExistedBlocks)
		if err != nil {
			return
		}
	}()

	// 3. Find already existed blocks that contained the block to be uploaded, this blocks to be uploaded's size and blocks number marked as decreased.
	var coveredUploadingBlocks = make([]uint64, 0)
	waitgroup.Add(1)
	go func() {
		err = foundCoveredUploadingBlocks(ctx, seg, yigFs, maxStart, minEnd, tag, &coveredUploadingBlocks)
		if err != nil {
			return
		}
	}()

	// 4. upload blocks or merge blocks. if merge blocks, the blocks number marked as decreased.
	mergeNumber, err := yigFs.MetaStorage.Client.CreateFileSegment(ctx, seg)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment info, region: %s, bucket: %s, ino: %d, generation: %d, seg_id0: %d, seg_id1: %d, err: %v",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1, err))
		return
	}

	// 5. if the segment does not have leader, create it.
	if isExisted == types.NotExisted {
		err = createSegLeader(ctx, seg, yigFs)
		if err != nil {
			return
		}
	}
	
	// 6. get the file size and calculate new file size and blocks number.
	getFileSize, getFileNumber, err := yigFs.MetaStorage.Client.GetFileSizeAndBlocksNum(ctx, seg)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get file size and blocks number, region: %s, bucket: %s, ino: %d, generation: %d, seg_id0: %d, seg_id1: %d",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1))
		return err
	}

	waitgroup.Wait()

	// 7. finally update the file size and blocks number.
	allBlocksSize += getFileSize 

	coveredExistedBlocksLength := len(coveredExistedBlocks)
	if coveredExistedBlocksLength > 0 {
		for _, size := range coveredExistedBlocks {
			allBlocksSize -= size
		}
	}

	coveredUploadingBlocksLength := len(coveredUploadingBlocks)
	if coveredUploadingBlocksLength > 0 {
		for _, size := range coveredUploadingBlocks {
			allBlocksSize -= size
		}
	}

	allBlocksNumber += getFileNumber - uint32(mergeNumber) - uint32(coveredExistedBlocksLength) - uint32(coveredUploadingBlocksLength)
	helper.Logger.Info(ctx, fmt.Sprintf("mergeNumber: %v, allSize: %v, allNum: %v", mergeNumber, allBlocksSize, allBlocksNumber))

	if allBlocksNumber != getFileNumber || allBlocksSize != getFileSize {
		err = yigFs.MetaStorage.Client.UpdateFileSizeAndBlocksNum(ctx, seg, allBlocksSize, allBlocksNumber)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to update file size and blocks number, err: %v", err))
			return
		}
	}

	return
}

func foundCoveredUploadingBlocks(ctx context.Context, seg *types.CreateSegmentReq, yigFs *YigFsStorage, 
		maxStart, minEnd, tag int64, coveredUploadingBlocks *[]uint64) (err error) {
	defer waitgroup.Done()
	containBlocks, err := yigFs.MetaStorage.Client.GetCoveredUploadingBlocks(ctx, seg, maxStart, minEnd, tag)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the contain segment blocks, err: %v", err))
		return
	}

	for _, block := range seg.Segment.Blocks {
		for _, blockInfo := range containBlocks {
			if blockInfo[0] <= block.Offset && (blockInfo[0] + blockInfo[1] > block.Offset + int64(block.Size)) || 
				(blockInfo[0] < block.Offset && (blockInfo[0] + blockInfo[1] >= block.Offset + int64(block.Size))) {
				*coveredUploadingBlocks = append(*coveredUploadingBlocks, uint64(block.Size))
				break
			}
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("containBlocks: %v, coveredUploadingBlocks: %v", containBlocks, coveredUploadingBlocks))
	return
}

func foundCoveredExistedBlocksAndDeleted(ctx context.Context, seg *types.CreateSegmentReq, yigFs *YigFsStorage, 
		minStart, maxEnd, tag int64, coveredExistedBlocks *[]uint64) (err error) {
	defer waitgroup.Done()
	existedBlocks, err := yigFs.MetaStorage.Client.GetCoveredExistedBlocks(ctx, seg, minStart, maxEnd, tag)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to get the covered segment blocks, err: %v", err))
		return
	}
	
	// check and deleted covered blocks
	for _, block := range seg.Segment.Blocks {
		for blockId, blockInfo := range existedBlocks {
			if block.Offset <= blockInfo[0] && (block.Offset + int64(block.Size) >= blockInfo[0] + blockInfo[1]) {
				seg.CoveredBlockOffset = blockInfo[0]
				err = yigFs.MetaStorage.Client.DeleteBlock(ctx, seg, blockId)
				if err != nil {
					helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete covered existed-block from tidb, blockId: %d, err: %v", blockId, err))
					return
				}

				delete(existedBlocks, blockId)
				//update coveredExistedBlocks
				*coveredExistedBlocks = append(*coveredExistedBlocks, uint64(blockInfo[1]))
			}
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("existedBlocks: %v, coveredExistedBlocks: %v", existedBlocks, coveredExistedBlocks))
	return
}

func createSegLeader(ctx context.Context, seg *types.CreateSegmentReq, yigFs *YigFsStorage) (err error) {
	// if the segment leader does not existed, create it.
	err = yigFs.MetaStorage.Client.CreateSegmentLeader(ctx, seg)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create the segment leader, err: %v", err))
		return
	}
	
	return
}

func(yigFs *YigFsStorage) UpdateSegment(ctx context.Context, seg *types.CreateSegmentReq, isExisted int) (updateSegResp *types.UpdateSegResp, err error) {
	// 1. Find the min and max index for the blocks to be uploaded, update the increased number and size.
	// 2. Find already existed blocks contained by the block to be uploaded, then deleted them, and update the decreased size and number.
	// 3. Find already existed blocks that contained the block to be uploaded, and update the decreased size and number.
	// 4. upload blocks or merge blocks. if merge blocks, the blocks number marked as decreased.
	// 5. if the segment does not have leader, create it.
	// 6. finally return the decreased and increased size/number.
	if len(seg.Segment.Blocks) == 0 {
		helper.Logger.Warn(ctx, "No blocks to upload")
		return
	}

	node, createBlockErr := snowflake.NewNode(1)
	if createBlockErr != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create blockId, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	tag := int64(node.Generate())

	updateSegResp = &types.UpdateSegResp{}

	// 1. Find the min and max index for the blocks to be uploaded, update the increased number and size.
	minStart, maxStart, minEnd, maxEnd, increasedSize, increasedNumber := findCheckIndex(ctx, seg)

	// 2. Find already existed blocks contained by the block to be uploaded, then deleted them, and mark the blocks size and blocks number as decreased.
	var coveredExistedBlocks = make([]uint64, 0)
	waitgroup.Add(1)
	go func() {
		err = foundCoveredExistedBlocksAndDeleted(ctx, seg, yigFs, minStart, maxEnd, tag, &coveredExistedBlocks)
		if err != nil {
			return
		}
	}()

	// 3. Find already existed blocks that contained the block to be uploaded, this blocks to be uploaded's size and blocks number marked as decreased.
	var coveredUploadingBlocks = make([]uint64, 0)
	waitgroup.Add(1)
	go func() {
		err = foundCoveredUploadingBlocks(ctx, seg, yigFs, maxStart, minEnd, tag, &coveredUploadingBlocks)
		if err != nil {
			return
		}
	}()

	// 4. upload blocks or merge blocks. if merge blocks, the blocks number marked as decreased.
	mergeNumber, err := yigFs.MetaStorage.Client.CreateFileSegment(ctx, seg)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment info, region: %s, bucket: %s, ino: %d, generation: %d, seg_id0: %d, seg_id1: %d, err: %v",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1, err))
		return
	}

	// 5. if the segment does not have leader, create it.
	if isExisted == types.NotExisted {
		err = createSegLeader(ctx, seg, yigFs)
		if err != nil {
			return
		}
	}

	// 6. finally return the decreased and increased size/number.
	waitgroup.Wait()

	var decreasedSize uint64 = 0
	var decreasedNumber uint32 = 0

	coveredExistedBlocksLength := len(coveredExistedBlocks)
	if coveredExistedBlocksLength > 0 {
		for _, size := range coveredExistedBlocks {
			decreasedSize += size
		}
	}

	coveredUploadingBlocksLength := len(coveredUploadingBlocks)
	if coveredUploadingBlocksLength > 0 {
		for _, size := range coveredUploadingBlocks {
			decreasedSize += size
		}
	}

	decreasedNumber += uint32(coveredExistedBlocksLength) + uint32(coveredUploadingBlocksLength) + uint32(mergeNumber)
	
	updateSegResp = &types.UpdateSegResp {
		IncreasedSize: increasedSize,
		DecreasedSize: decreasedSize,
		IncreasedNumber: increasedNumber,
		DecreasedNumber: decreasedNumber,
	}

	return
}

func(yigFs *YigFsStorage) GetFileSizeAndBlocksNum(ctx context.Context, seg *types.CreateSegmentReq) (size uint64, number uint32, err error) {
	size, number, err = yigFs.MetaStorage.Client.GetFileSizeAndBlocksNum(ctx, seg)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get file size and blocks number, region: %s, bucket: %s, ino: %d, generation: %d, seg_id0: %d, seg_id1: %d",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1))
		return
	}

	return size, number, nil
}

func(yigFs *YigFsStorage) UpdateFileSizeAndBlocksNum(ctx context.Context, seg *types.CreateSegmentReq, allBlocksSize uint64, allBlocksNumber uint32) (err error) {
	err = yigFs.MetaStorage.Client.UpdateFileSizeAndBlocksNum(ctx, seg, allBlocksSize, allBlocksNumber)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to update file size and blocks number, err: %v", err))
		return
	}

	return
}