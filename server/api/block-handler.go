package api

import (
	"context"
	"fmt"

	"github.com/kataras/iris"
	"github.com/google/uuid"
	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
)

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
	err := yigFs.YigFsAPI.CheckSegmentLeader(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	// create segment info to tidb
	err = yigFs.YigFsAPI.CreateFileSegment(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	//  update file size and blocks number.
	file := &types.GetFileInfoReq {
		Region: segReq.Region,
		BucketName: segReq.BucketName,
		Ino: segReq.Ino,
		Generation: segReq.Generation,
	}
	err = yigFs.YigFsAPI.UpdateFileSizeAndBlocksNum(reqContext, file)
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
	err := CheckUpdateSegmentsParams(reqContext, segsReq, yigFs)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	uuidStr := uuid.New()
	segsReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// update segments
	err = yigFs.YigFsAPI.UpdateFileSegments(reqContext, segsReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	//  update file size and blocks number.
	file := &types.GetFileInfoReq{
		Region: segsReq.Region,
		BucketName: segsReq.BucketName,
		Ino: segsReq.Ino,
		Generation: segsReq.Generation,
	}
	err = yigFs.YigFsAPI.UpdateFileSizeAndBlocksNum(reqContext, file)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) GetSegmentsHandler(ctx iris.Context) {
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
	if segReq.BucketName == "" || segReq.Ino == 0 || segReq.ZoneId == "" || segReq.Machine == "" {
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

	// check whether the file has segments.
	isExisted, err := yigFs.YigFsAPI.IsFileHasSegments(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	if isExisted {
		// if the file has segments, return these segments info.
		resp, err = yigFs.YigFsAPI.GetFileSegmentsInfo(reqContext, segReq)
		if err != nil {
			resp.Result = GetErrInfo(err)
			ctx.JSON(resp)
			return
		}
	} else {
		// if the file does not have segments, return the slowest growing segment checking by the machine.
		resp, err = yigFs.YigFsAPI.GetTheSlowestGrowingSeg(reqContext, segReq)
		if err != nil {
			resp.Result = GetErrInfo(err)
			ctx.JSON(resp)
			return
		}
	}
	
	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) UpdateSegBlockInfoHandler(ctx iris.Context) {
	resp := &types.NonBodyResp {
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("UpdateSegBlockInfoHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	segReq := &types.UpdateSegBlockInfoReq{}
	if err := ctx.ReadJSON(&segReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read UpdateSegBlockInfoReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if segReq.BucketName == "" || segReq.ZoneId == "" {
		helper.Logger.Error(reqContext, "Some UpdateSegBlockInfo required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if segReq.Region == "" {
		segReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	segReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// update seg block info to tidb
	err := yigFs.YigFsAPI.UpdateSegBlockInfo(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}
	
	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) HeartBeatHandler(ctx iris.Context) {
	resp := &types.GetIncompleteUploadSegsResp{}
	defer GetSpendTime("HeartBeatHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	segReq := &types.GetIncompleteUploadSegsReq{}
	if err := ctx.ReadJSON(&segReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read GetIncompleteUploadSegsReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if segReq.BucketName == "" || segReq.ZoneId == "" || segReq.Machine == "" {
		helper.Logger.Error(reqContext, "Some HeartBeatHandler required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if segReq.Region == "" {
		segReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	segReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get incomplete upload segments by machine from tidb.
	var err error
	resp, err = yigFs.YigFsAPI.GetIncompleteUploadSegs(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}
	
	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}