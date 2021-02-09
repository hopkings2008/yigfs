package api

import (
	"context"
	"log"

	"github.com/kataras/iris"
	"github.com/google/uuid"
	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)

func(yigFs MetaAPIHandlers) GetFileLeaderHandler(ctx iris.Context) {
	resp := &types.GetLeaderResp {
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("GetLeaderHandler")()

	// get req
	leaderReq := &types.GetLeaderReq{}
	if err := ctx.ReadJSON(&leaderReq); err != nil {
		log.Printf("Failed to read GetLeaderReq from body, err: %v", err)
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if leaderReq.BucketName == "" || leaderReq.ZoneId == "" || leaderReq.Ino == 0 {
                log.Printf("Some geFileLeader required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
                return
        }

        if leaderReq.Region == "" {
                leaderReq.Region = "cn-bj-1"
        }


	r := ctx.Request()
        reqContext := r.Context()
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
