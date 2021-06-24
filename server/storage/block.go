package storage

import (
	"context"
	"fmt"
	"sync"
	"math"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/types"
)

var (
	waitgroup sync.WaitGroup
	maxUploadNum = 5000
)

func getIncludeOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64, 
	yigFs *YigFsStorage) (segments map[interface{}][]*types.BlockInfo, err error) {
	segments, err = yigFs.MetaStorage.Client.GetIncludeOffsetIndexSegs(ctx, seg, checkOffset)
	if err != nil && err != ErrYigFsNoTargetSegment {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to getIncludeOffsetIndexSegs, region: %s, bucket: %s, ino: %d, generation: %d, checkOffset: %d",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, checkOffset))
		return segments, err
	} else if err == ErrYigFsNoTargetSegment || len(segments) == 0 {
		return segments, nil
	}

	return
}

func getGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64, 
	yigFs *YigFsStorage) (segments map[interface{}][]*types.BlockInfo, err error) {
	segments, err = yigFs.MetaStorage.Client.GetGreaterOffsetIndexSegs(ctx, seg, checkOffset)
	if err != nil && err != ErrYigFsNoTargetSegment {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to getGreaterOffsetIndexSegs, region: %s, bucket: %s, ino: %d, generation: %d, checkOffset: %d",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, checkOffset))
		return segments, err
	} else if err == ErrYigFsNoTargetSegment || len(segments) == 0 {
		return segments, nil
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
		var includeSegs = make(map[interface{}][]*types.BlockInfo)
		waitgroup.Add(1)
		go func() {
			defer waitgroup.Done()
			includeSegs, err = getIncludeOffsetIndexSegs(ctx, seg, checkOffset, yigFs)
			if err != nil {
				return
			}
		}()

		greaterSegs, err := getGreaterOffsetIndexSegs(ctx, seg, checkOffset, yigFs)
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
					isSegIdEqual = true
					break
				}
			}

			if !isSegIdEqual {
				greaterSegs[segmentId] = includeBlocks
			}
		}

		helper.Logger.Info(ctx, fmt.Sprintf("req: greaterSegs: %v, includeSegs: %v", greaterSegs, includeSegs))
		getGreatherBlocksResp, err := yigFs.MetaStorage.Client.GetSegsBlockInfo(ctx, seg, greaterSegs)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("getGreaterOffsetIndexSegs: Failed to get blocks info, region: %s, bucket: %s, ino: %d, generation: %d",
				seg.Region, seg.BucketName, seg.Ino, seg.Generation))
			return resp, err
		}

		resp.Segments = getGreatherBlocksResp.Segments
	
	} else {
		greaterSegs, err := getGreaterOffsetIndexSegs(ctx, seg, checkOffset, yigFs)
		if err != nil {
			return resp, err
		}

		getGreatherBlocksResp, err := yigFs.MetaStorage.Client.GetSegsBlockInfo(ctx, seg, greaterSegs)
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

func (yigFs *YigFsStorage) CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq) (err error) {
	blocksNum := len(seg.Segment.Blocks)
	if blocksNum == 0 {
		helper.Logger.Warn(ctx, "No blocks to upload")
		return
	}

	segInfo := &types.DescriptBlockInfo {
		Region: seg.Region,
		BucketName: seg.BucketName,
		Ino: seg.Ino,
		Generation: seg.Generation,
		ZoneId: seg.ZoneId,
	}

	maxEnd := seg.Segment.Blocks[blocksNum-1].SegStartAddr + seg.Segment.Blocks[blocksNum-1].Size
	for _, block := range seg.Segment.Blocks {
		segEndAddr := block.SegStartAddr + block.Size
		if segEndAddr > maxEnd {
			maxEnd = segEndAddr
		}
	}

	// if the seg leader is not existed, create it.
	// update size for segment_info.
	segReq := types.CreateBlocksInfo {
		SegmentId0: seg.Segment.SegmentId0,
		SegmentId1: seg.Segment.SegmentId1,
		Leader: seg.Machine,
		Capacity: seg.Segment.Capacity,
		MaxSize: maxEnd,
	}

	segsReq := make([]*types.CreateBlocksInfo, 0)
	if blocksNum > maxUploadNum {
		cycleNums := int(math.Ceil(float64(blocksNum)/float64(maxUploadNum)))
		for i := 0; i < cycleNums; i++ {
			// update segments
			if i == cycleNums - 1 {
				alreadyUpload := (cycleNums - 1) * maxUploadNum
				lastUploadNum := blocksNum - alreadyUpload
				segReq.Blocks = seg.Segment.Blocks[alreadyUpload:]
				segsReq = append(segsReq, &segReq)
				helper.Logger.Info(ctx, "cycleNums is : %v, lastUploadNum is: %v, alreadyUpload: %v", cycleNums, lastUploadNum, alreadyUpload)
				err = uploadBlocks(ctx, segInfo, segsReq, lastUploadNum, true, yigFs)
				if err != nil {
					return
				}
			} else {
				segReq.Blocks = seg.Segment.Blocks[i * maxUploadNum: (i+1) * maxUploadNum]
				segsReq = append(segsReq, &segReq)
				err = uploadBlocks(ctx, segInfo, segsReq, maxUploadNum, false, yigFs)
				if err != nil {
					return
				} else {
					segsReq = segsReq[:0]
				}
			}
		}
	} else {
		segReq.Blocks = seg.Segment.Blocks
		segsReq = append(segsReq, &segReq)
		err = uploadBlocks(ctx, segInfo, segsReq, len(segReq.Blocks), true, yigFs)
		if err != nil {
			return
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create file segment, region: %s, bucket: %s, ino: %d, generation: %d,",
		seg.Region, seg.BucketName, seg.Ino, seg.Generation))
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

		segInfo.Leader = seg.Machine
		segInfo.Blocks = make([]*types.BlockInfo, 0)
		resp.Segments = append(resp.Segments, segInfo)

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
 
func uploadBlocks(ctx context.Context, segInfo *types.DescriptBlockInfo, segs []*types.CreateBlocksInfo, blocksNum int, isUpdateInfo bool, yigFs *YigFsStorage) (err error) {
	err = yigFs.MetaStorage.Client.InsertOrUpdateFileAndSegBlocks(ctx, segInfo, segs, isUpdateInfo, blocksNum)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to insert or update blocks, region: %s, bucket: %s, ino: %v, generation: %v, err: %v",
			segInfo.Region, segInfo.BucketName, segInfo.Ino, segInfo.Generation, err))
		return
	}
	return
}

func removeBlocks(ctx context.Context, segs []*types.CreateBlocksInfo, blocksNum int, yigFs *YigFsStorage) (err error) {
	err = yigFs.MetaStorage.Client.RemoveSegBlocks(ctx, segs, blocksNum)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to remove seg blocks, blocksNum: %v", blocksNum))
		return
	}
	return
}

func execUpdateBlocks(ctx context.Context, segInfo *types.DescriptBlockInfo, segsReq []*types.CreateBlocksInfo, blocksNum int, 
	isUpdateInfo bool, action int, yigFs *YigFsStorage) (err error) {
	if action == types.UpdateSegs {
		err = uploadBlocks(ctx, segInfo, segsReq, blocksNum, isUpdateInfo, yigFs)
		if err != nil {
			return
		}
	} else if action == types.RemoveSegs {
		err = removeBlocks(ctx, segsReq, blocksNum, yigFs)
		if err != nil {
			return
		}
	}
	return
}

func delRemoveAndUploadSegs(ctx context.Context, segInfo *types.DescriptBlockInfo, segs []*types.CreateBlocksInfo, 
	yigFs *YigFsStorage, action int) (allBlocksNum uint32, maxSize uint64, err error) {
	var currentBlocksNum int
	segsReq := make([]*types.CreateBlocksInfo, 0)
	var fileSize uint64

	for _, seg := range segs {
		blocksNum := len(seg.Blocks)
		if blocksNum == 0 {
			helper.Logger.Warn(ctx, fmt.Sprintf("The segment does not have blocks to update, seg_id0: %v, seg_id1: %v", seg.SegmentId0, seg.SegmentId1))
			continue
		}

		allBlocksNum += uint32(blocksNum)

		maxEnd := seg.Blocks[blocksNum-1].SegStartAddr + seg.Blocks[blocksNum-1].Size
		for _, block := range seg.Blocks {
			segEndAddr := block.SegStartAddr + block.Size
			if segEndAddr > maxEnd {
				maxEnd = segEndAddr
			}
		}

		// get the segment max size to update segment info's size.
		segReq := types.CreateBlocksInfo {
			SegmentId0: seg.SegmentId0,
			SegmentId1: seg.SegmentId1,
			Leader: seg.Leader,
			Capacity: seg.Capacity,
			MaxSize: maxEnd,
		}
		segsReq = append(segsReq, &segReq)

		for j, block := range seg.Blocks {
			segReq.Blocks = append(segReq.Blocks, block)
			currentBlocksNum++
			if currentBlocksNum == maxUploadNum {
				err = execUpdateBlocks(ctx, segInfo, segsReq, maxUploadNum, true, action, yigFs)
				if err != nil {
					helper.Logger.Error(ctx, fmt.Sprintf("Failed to exec update blocks, segsNum: %v, err: %v", len(segsReq), err))
					return
				} else {
					segsReq = segsReq[:0]
					segReq.Blocks = segReq.Blocks[:0]
					currentBlocksNum = 0
					if j < blocksNum - 1 {
						segsReq = append(segsReq, &segReq)
					}
				}
			}

			fileSize = uint64(block.Offset) + uint64(block.Size)
			if fileSize > maxSize {
				maxSize = fileSize
			}
		}
	}

	if len(segsReq) > 0 {
		remainBlocksNum := 0
		for _, seg := range segsReq {
			remainBlocksNum += len(seg.Blocks)
		}
		err = execUpdateBlocks(ctx, segInfo, segsReq, remainBlocksNum, true, action, yigFs)
		if err != nil {
			return
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to del blocks, action: %v, region: %s, bucket: %s, ino: %d, generation: %d",
		action, segInfo.Region, segInfo.BucketName, segInfo.Ino, segInfo.Generation))
	return
}

func (yigFs *YigFsStorage) UpdateFileSegments(ctx context.Context, segs *types.UpdateSegmentsReq) (allBlocksNum uint32, maxSize uint64, err error) {
	segInfo := &types.DescriptBlockInfo {
		Region: segs.Region,
		BucketName: segs.BucketName,
		Ino: segs.Ino,
		Generation: segs.Generation,
		ZoneId: segs.ZoneId,
	}

	if len(segs.Segments) > 0 {
		allBlocksNum, maxSize, err = delRemoveAndUploadSegs(ctx, segInfo, segs.Segments, yigFs, types.UpdateSegs)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to upload segments, region: %s, bucket: %s, ino: %d, generation: %d,",
				segs.Region, segs.BucketName, segs.Ino, segs.Generation))
			return 0, 0, err
		}
	}

	removeSegsNum := len(segs.RemoveSegments)
	if removeSegsNum > 0 {
		helper.Logger.Info(ctx, fmt.Sprintf("Begin to remove segments, region: %s, bucket: %s, ino: %d, generation: %d, removeSegsNum: %v",
			segs.Region, segs.BucketName, segs.Ino, segs.Generation, removeSegsNum))
		_, _, err = delRemoveAndUploadSegs(ctx, segInfo, segs.RemoveSegments, yigFs, types.RemoveSegs)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to remove segments, region: %s, bucket: %s, ino: %d, generation: %d,",
				segs.Region, segs.BucketName, segs.Ino, segs.Generation))
			return 0, 0, err
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update file segments, region: %s, bucket: %s, ino: %d, generation: %d,",
		segs.Region, segs.BucketName, segs.Ino, segs.Generation))
	return
}
