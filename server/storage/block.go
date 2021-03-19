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

func(yigFs *YigFsStorage) CreateSegmentInfo(ctx context.Context, seg *types.CreateSegmentReq, isExisted int) (err error) {
	// 1. Find the min and max value of the start address and the end address of the blocks to be uploaded.
	// all the blocks size and blocks number marked as increased.
	// 2. Find already existed blocks contained by the block to be uploaded, deleted it, and mark the blocks size and blocks number as decreased.
	// 3. Find already existed blocks that contained the block to be uploaded, this blocks to be uploaded's size and blocks number marked as decreased.
	// 4. upload blocks or merge blocks. if merge blocks, the blocks number marked as decreased.
	// 5. if the segment does not have leader, create it.
	// 6. get teh file size and calculate new file size and blocks number.
	// 7. finally update the file size and blocks number.

	// 1. Find the min and max value of the start address and the end address of the blocks to be uploaded.
	// all the blocks size and blocks number marked as increased.
	if len(seg.Segment.Blocks) == 0 {
		helper.Logger.Warn(ctx, "No blocks to upload")
		return
	}

	var minStart int64 = seg.Segment.Blocks[0].SegStartAddr
	var maxEnd  int64 = seg.Segment.Blocks[0].SegEndAddr
	var maxStart int64 = seg.Segment.Blocks[0].SegStartAddr
	var minEnd  int64 = seg.Segment.Blocks[0].SegEndAddr

	var allBlocksNumber uint32 = 0
	var allBlocksSize uint64 = 0

	var coveredExistedBlocks = make([]uint64, 0)
	var coveredUploadingBlocks = make([]uint64, 0)

	uploadingBlocksNum := len(seg.Segment.Blocks)

	for i := 0; i < uploadingBlocksNum; i ++ {
		if i > 0 {
			if seg.Segment.Blocks[i].SegStartAddr < minStart {
				minStart = seg.Segment.Blocks[i].SegStartAddr
			} else {
				maxStart = seg.Segment.Blocks[i].SegStartAddr
			}

			if seg.Segment.Blocks[i].SegEndAddr > maxEnd {
				maxEnd = seg.Segment.Blocks[i].SegEndAddr
			} else {
				minEnd = seg.Segment.Blocks[i].SegEndAddr
			}
		}

		allBlocksSize += uint64(seg.Segment.Blocks[i].Size)
	}

	allBlocksNumber += uint32(uploadingBlocksNum)

	// 2. Find already existed blocks contained by the block to be uploaded, deleted it, and mark the blocks size and blocks number as decreased.
	waitgroup.Add(1)
	go func() {
		err = foundCoveredBlocksAndDeleted(ctx, seg, yigFs, minStart, maxEnd, &coveredExistedBlocks)
		if err != nil {
			return
		}
	}()

	// 3. Find already existed blocks that contained the block to be uploaded, this blocks to be uploaded's size and blocks number marked as decreased.
	waitgroup.Add(1)
	go func() {
		err = foundCoveredUploadingBlocks(ctx, seg, yigFs, maxStart, minEnd, &coveredUploadingBlocks)
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
	
	// 6. get teh file size and calculate new file size and blocks number.
	size, number, err := yigFs.MetaStorage.Client.GetFileSizeAndBlocksNum(ctx, seg)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get file size and blocks number, region: %s, bucket: %s, ino: %d, generation: %d, seg_id0: %d, seg_id1: %d",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1))
		return err
	}

	waitgroup.Wait()

	// 7. finally update the file size and blocks number.
	allBlocksSize += size 

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

	allBlocksNumber += number - uint32(mergeNumber) - uint32(coveredExistedBlocksLength) - uint32(coveredUploadingBlocksLength)
	helper.Logger.Info(ctx, fmt.Sprintf("mergeNumber: %v, allSize: %v, allNum: %v", mergeNumber, allBlocksSize, allBlocksNumber))

	err = yigFs.MetaStorage.Client.UpdateFileSizeAndBlocksNum(ctx, seg, allBlocksSize, allBlocksNumber)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to update file size and blocks number, err: %v", err))
		return
	}

	return
}

func foundCoveredUploadingBlocks(ctx context.Context, seg *types.CreateSegmentReq, yigFs *YigFsStorage, maxStart, minEnd int64, coveredUploadingBlocks *[]uint64) (err error) {
	defer waitgroup.Done()
	containBlocks, err := yigFs.MetaStorage.Client.GetCoverBlocks(ctx, seg, maxStart, minEnd)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the contain segment blocks, err: %v", err))
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("foundCoveredUploadingBlocks, containBlocks: %v", containBlocks))

	for _, block := range seg.Segment.Blocks {
		for _, blockInfo := range containBlocks {
			if blockInfo[0] < block.SegStartAddr && blockInfo[1] > block.SegEndAddr {
				*coveredUploadingBlocks = append(*coveredUploadingBlocks, uint64(block.Size))
				break
			}
		}
	}

	return
}

func foundCoveredBlocksAndDeleted(ctx context.Context, seg *types.CreateSegmentReq, yigFs *YigFsStorage, minStart, maxEnd int64, coveredExistedBlocks *[]uint64) (err error) {
	defer waitgroup.Done()
	existedBlocks, err := yigFs.MetaStorage.Client.GetCoveredExistedBlocks(ctx, seg, minStart, maxEnd)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to get the covered segment blocks, err: %v", err))
		return
	}
	helper.Logger.Info(ctx, fmt.Sprintf("GetCoveredExistedBlocks, existedBlocks: %v", existedBlocks))
	
	// check and deleted covered blocks
	for _, block := range seg.Segment.Blocks {
		for blockId, blockInfo := range existedBlocks {
			if block.SegStartAddr <= blockInfo[0] && block.SegEndAddr >= blockInfo[1] {
				err = yigFs.MetaStorage.Client.DeleteBlock(ctx, seg, blockId)
				if err != nil {
					helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete covered existed-block from tidb, blockId: %d, err: %v", blockId, err))
					return
				}

				delete(existedBlocks, blockId)

				//update deleted size and number
				*coveredExistedBlocks = append(*coveredExistedBlocks, uint64(blockInfo[2]))
			}
		}
	}

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