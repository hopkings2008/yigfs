package api

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
)

func CheckUpdateSegmentsParams(ctx context.Context, segsReq *types.UpdateSegmentsReq) (err error) {
	if len(segsReq.Segments) == 0 {
		helper.Logger.Error(ctx, "No vaild segments to upload.")
		return ErrYigFsNoVaildSegments
	}

	// check request params
	if segsReq.ZoneId == "" || segsReq.BucketName == "" || segsReq.Ino == 0 {
		helper.Logger.Error(ctx, "Some UpdateSegmentsReq required parameters are missing.")
		return ErrYigFsMissingRequiredParams
	}

	for _, segment := range segsReq.Segments {
		// check segment leader
		if segment.Leader == "" {
			helper.Logger.Error(ctx, "UpdateSegmentsReq required segments leader are missing.")
			return ErrYigFsMissingSegmentLeader
		}
	}

	if segsReq.Region == "" {
		segsReq.Region = "cn-bj-1"
	}

	return
}
