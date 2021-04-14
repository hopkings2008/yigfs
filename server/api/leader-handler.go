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

func(yigFs MetaAPIHandlers) GetFileLeaderHandler(ctx iris.Context) {
	r := ctx.Request()
	reqContext := r.Context()

	resp := &types.GetLeaderResp {
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("GetLeaderHandler")()

	// get req
	leaderReq := &types.GetLeaderReq{}
	if err := ctx.ReadJSON(&leaderReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read GetLeaderReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if leaderReq.BucketName == "" || leaderReq.ZoneId == "" || leaderReq.Ino == 0 {
		helper.Logger.Error(reqContext, "Some geFileLeader required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
        	return
    	}	

    	if leaderReq.Region == "" {
		leaderReq.Region = "cn-bj-1"
    	}

	uuidStr := uuid.New()
	leaderReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get leader from tidb
	getLeaderResp, err := yigFs.YigFsAPI.GetFileLeader(reqContext, leaderReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = GetErrInfo(NoYigFsErr)
	resp.LeaderInfo = getLeaderResp.LeaderInfo
	ctx.JSON(resp)
	return
}
