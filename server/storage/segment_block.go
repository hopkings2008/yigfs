package storage

import (
	"context"
	"fmt"

	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
	. "github.com/hopkings2008/yigfs/server/error"
)


func(yigFs *YigFsStorage) UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	err = yigFs.MetaStorage.Client.UpdateSegBlockInfo(ctx, seg)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to update segment block info, zone: %s, region: %s, bucket: %s, seg_id0: %v, seg_id1: %v, err: %v", 
			seg.ZoneId, seg.Region, seg.BucketName, seg.SegBlockInfo.SegmentId0, seg.SegBlockInfo.SegmentId1, err))
		return
	}

	return
}

func(yigFs *YigFsStorage) GetIncompleteUploadSegs(ctx context.Context, seg *types.GetIncompleteUploadSegsReq) (segs *types.GetIncompleteUploadSegsResp, err error) {
	getSegsResp, err := yigFs.MetaStorage.Client.GetSegsByLeader(ctx, seg)
	switch err {
	case ErrYigFsNoTargetSegment:
		segs.UploadSegments = make([]*types.IncompleteUploadSegInfo, 0)
		segs.RemoveSegments = make([]*types.RemoveSegInfo, 0)
		helper.Logger.Warn(ctx, fmt.Sprintf("GetIncompleteUploadSegs is None, zone: %v, region: %v, bucket: %v, machine: %v", 
			seg.ZoneId, seg.Region, seg.BucketName, seg.Machine))
		return
	case nil:
		segs, err = yigFs.MetaStorage.Client.GetIncompleteUploadSegs(ctx, seg, getSegsResp)
		if err != nil {
			return
		}

		if len(segs.UploadSegments) == 0 {
			helper.Logger.Warn(ctx, fmt.Sprintf("GetIncompleteUploadSegs is None, zone: %v, region: %v, bucket: %v, machine: %v", 
				seg.ZoneId, seg.Region, seg.BucketName, seg.Machine))
			segs.UploadSegments = make([]*types.IncompleteUploadSegInfo, 0)
		}

		if len(segs.RemoveSegments) == 0 {
			helper.Logger.Warn(ctx, fmt.Sprintf("GetRemoveSegs is None, zone: %v, region: %v, bucket: %v, machine: %v", 
				seg.ZoneId, seg.Region, seg.BucketName, seg.Machine))
			segs.RemoveSegments = make([]*types.RemoveSegInfo, 0)
		}
		return
	default:
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetIncompleteUploadSegs, zone: %v, region: %v, bucket: %v, machine: %v", 
			seg.ZoneId, seg.Region, seg.BucketName, seg.Machine))
		return
	}
}
