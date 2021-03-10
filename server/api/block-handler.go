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
	err := yigFs.YigFsAPI.CheckSegmentLeader(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	// create segment info to tidb
	err = yigFs.YigFsAPI.CreateSegmentInfo(reqContext, segReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}
