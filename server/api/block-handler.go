package api

import (
	"context"
	"fmt"
	"sync"

	"github.com/kataras/iris"
	"github.com/google/uuid"
	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
)

var (
	waitgroup sync.WaitGroup
	lock sync.Mutex
)

func(yigFs MetaAPIHandlers) GetSegmentHandler(ctx iris.Context) {
	resp := &types.GetSegmentResp {
		Result: types.YigFsMetaError {},
	}
	defer GetSpendTime("GetSegmentHandler")()

	r := ctx.Request()
    reqContext := r.Context()

	// get req
	segReq := &types.GetSegmentReq{}
	if err := ctx.ReadJSON(&segReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read GetSegmentReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if segReq.BucketName == "" || segReq.Ino == 0 || segReq.ZoneId == "" {
		helper.Logger.Error(reqContext, "Some GetSegmentInfo required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if segReq.Region == "" {
		segReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	segReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get file segment from tidb
	resp, err := yigFs.YigFsAPI.GetFileSegmentInfo(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}
	
	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) CreateSegmentHandler(ctx iris.Context) {
	resp := &types.NonBodyResp {
		Result: types.YigFsMetaError {},
	}
	defer GetSpendTime("CreateSegmentHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	segReq := &types.CreateSegmentReq{}
	if err := ctx.ReadJSON(&segReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read CreateSegmentReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if segReq.ZoneId == "" || segReq.Machine == "" || segReq.BucketName == "" || segReq.Ino == 0 {
		helper.Logger.Error(reqContext, "Some CreateSegment required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}
	if segReq.Region == "" {
		segReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	segReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// check segment leader
	isExisted, err := yigFs.YigFsAPI.CheckSegmentLeader(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	// create segment info to tidb
	err = yigFs.YigFsAPI.CreateSegmentInfo(reqContext, segReq, isExisted)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) UpdateSegmentsHandler(ctx iris.Context) {
	resp := &types.NonBodyResp {
		Result: types.YigFsMetaError {},
	}
	defer GetSpendTime("UpdateSegmentsHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	segsReq := &types.UpdateSegmentsReq{}
	if err := ctx.ReadJSON(&segsReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read UpdateSegmentsReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check req params
	err := CheckUpdateSegmentsParams(reqContext, segsReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	uuidStr := uuid.New()
	segsReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	updateSegsResp := make([]types.UpdateSegResp, 0)
	// update segments
	for _, segment := range segsReq.Segments {
		segmentInfo := types.CreateBlocksInfo {
			SegmentId0: segment.SegmentId0,
			SegmentId1: segment.SegmentId1,
			MaxSize: segment.MaxSize,
			Blocks: segment.Blocks,
		}

		segReq := &types.CreateSegmentReq {
			Region: segsReq.Region,
			BucketName: segsReq.BucketName,
			ZoneId: segsReq.ZoneId,
			Machine: segment.Leader,
			Ino: segsReq.Ino,
			Segment: segmentInfo,
		}

		isExisted, err := yigFs.YigFsAPI.CheckSegmentLeader(reqContext, segReq)
		if err != nil {
			resp.Result = GetErrInfo(err)
			ctx.JSON(resp)
			return
		}

		// update segment info to tidb
		waitgroup.Add(1)
		go func () {
			updateSegResp, err := yigFs.YigFsAPI.UpdateSegment(reqContext, segReq, isExisted)
			if err != nil {
				resp.Result = GetErrInfo(err)
				ctx.JSON(resp)
				return
			}

			lock.Lock()
			updateSegsResp = append(updateSegsResp, *updateSegResp)
			lock.Unlock()

			waitgroup.Done()
		}()
	}

	// get file size and number
	seg := &types.CreateSegmentReq {
		Region: segsReq.Region,
		BucketName: segsReq.BucketName,
		Ino: segsReq.Ino,
		Generation: segsReq.Generation,
	}
	fileSize, fileNum, err := yigFs.YigFsAPI.GetFileSizeAndBlocksNum(reqContext, seg)
	if err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("UpdateSegmentsHandler: get file size and blocks number failed, err: %v", err))
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	waitgroup.Wait()
	// update file size and number, until goroutine all over.
	var finalSize uint64 = fileSize
	var finalNum uint32 = fileNum

	for _, segResp := range updateSegsResp {
		finalSize = finalSize + segResp.IncreasedSize - segResp.DecreasedSize
		finalNum =  finalNum + segResp.IncreasedNumber - segResp.DecreasedNumber
	}

	if finalSize != fileSize || finalNum != fileNum {
		err = yigFs.YigFsAPI.UpdateFileSizeAndBlocksNum(reqContext, seg, finalSize, finalNum)
		if err != nil {
			helper.Logger.Error(reqContext, fmt.Sprintf("UpdateSegmentsHandler: update file size and blocks number failed, err: %v", err))
			resp.Result = GetErrInfo(err)
			ctx.JSON(resp)
			return
		}
	}

	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}
