package storage

import (
	"context"
	"log"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)


func(yigFs *YigFsStorage) GetFileSegmentInfo(ctx context.Context, file *types.GetSegmentReq) (resp *types.GetSegmentResp, err error)  {
	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}
	var  startBlockId int

	getSegInfoResp := &types.GetSegmentResp{}
	getSegInfoResp, err = yigFs.MetaStorage.Client.GetFileSegmentInfo(ctx, file)
	if err != nil && err != ErrYigFsNoTargetSegment {
		log.Printf("Failed to get segment info, region: %s, bucket: %s, ino: %d, generation: %d, offset: %d, size: %d",
			file.Region, file.BucketName, file.Ino, file.Generation, file.Offset, file.Size)
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
				segment.Blocks = blocks[startBlockId:]

				resp.Segments = append(resp.Segments, segment)
			}
		}

		log.Printf("Succeed to get segment info, region: %s, bucket: %s, ino: %d, generation: %d, offset: %d, size: %d",
			file.Region, file.BucketName, file.Ino, file.Generation, file.Offset, file.Size)
		return resp, nil
	}

	log.Printf("Succeed to get segment info, region: %s, bucket: %s, ino: %d, generation: %d, offset: %d, size: %d", 
		file.Region, file.BucketName, file.Ino, file.Generation, file.Offset, file.Size)
	return getSegInfoResp, nil
}

func(yigFs *YigFsStorage) CreateSegmentInfo(ctx context.Context, seg *types.CreateSegmentReq) (err error) {
	err = yigFs.MetaStorage.Client.CreateFileSegment(ctx, seg)
	if err != nil {
		log.Printf("Failed to create segment info, region: %s, bucket: %s, ino: %d, generation: %d, seg_id0: %d, seg_id1: %d",
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1)
		return
	}
	return
}
