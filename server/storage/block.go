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
	startBlockId := -1

	getSegInfoResp, err := yigFs.MetaStorage.Client.GetFileSegmentInfo(ctx, file)
	if err != nil && err != ErrYigFsNoTargetSegment {
		log.Printf("Failed to get segment info, region: %s, bucket: %s, ino: %d, generation: %d, offset: %d, size: %d",
			file.Region, file.BucketName, file.Ino, file.Generation, file.Offset, file.Size)
		return resp, err
	} else if err == ErrYigFsNoTargetSegment || len(getSegInfoResp.Segments) == 0 {
		return resp, nil
	}

	if file.Offset > 0 && file.Size == 0 {
		segments := getSegInfoResp.Segments
		for _, seg := range segments {
			blocks := seg.Blocks
			for key, block := range blocks {
				if block.Offset < file.Offset && block.Offset + int64(block.Size) > file.Offset || block.Offset >= file.Offset {
					startBlockId = key
					break
				}
			}

			if startBlockId != -1 {
				segment := &types.SegmentInfo {
					Blocks: []*types.BlockInfo{},
				}
				segment.SegmentId = seg.SegmentId
				segment.Blocks = blocks[startBlockId:]
				resp.Segments = append(resp.Segments, segment)
			}
		}
		return resp, nil
	}

	if file.Offset > 0 && file.Size > 0 {
		segments := getSegInfoResp.Segments
		for _, seg := range segments {
			blocks := seg.Blocks
			for key, block := range blocks {
				fileAddress := file.Offset + int64(file.Size)
				if block.Offset <= fileAddress && block.Offset + int64(block.Size) >= fileAddress {
					startBlockId = key
					break
				}
			}

			if startBlockId != -1 {
				segment := &types.SegmentInfo {
					Blocks: []*types.BlockInfo{},
				}
				segment.SegmentId = seg.SegmentId
				segment.Blocks = append(segment.Blocks, blocks[startBlockId])
				resp.Segments = append(resp.Segments, segment)
			}
		}
		return resp, nil
	}
	return getSegInfoResp, nil
}

func(yigFs *YigFsStorage) CreateSegmentInfo(ctx context.Context, seg *types.CreateSegmentReq) (err error) {
	err = yigFs.MetaStorage.Client.CreateFileSegment(ctx, seg)
	if err != nil {
		log.Printf("Failed to create segment info, region: %s, bucket: %s, ino: %d, generation: %d, seg_id: %d", 
			seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId)
		return
	}
	return
}
