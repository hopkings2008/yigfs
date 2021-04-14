package storage

import (
	"context"
	"fmt"

	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
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

