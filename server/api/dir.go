package api

import (
	"context"
	"log"

	"github.com/kataras/iris"
	"github.com/google/uuid"
	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)


func(yigFs MetaAPIHandlers) GetDirFilesHandler(ctx iris.Context) {
	resp := &types.GetDirFilesResp {
        	Result: types.YigFsMetaError {},
	}
	// get req
	dirReq := &types.GetDirFilesReq{}
	if err := ctx.ReadJSON(&dirReq); err != nil {
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		log.Printf("Failed to read GetDirFilesReq from body, err:", err)
		ctx.JSON(resp)
		return
	}

	r := ctx.Request()
	uuidStr := uuid.New()
	dirReq.Ctx = context.WithValue(r.Context(), types.CTX_REQ_ID, uuidStr)

	// get dir files
	if dirReq.Offset <= 1 {
		dirReq.Offset = 2
	}

	getDirFilesResp, offset, err := yigFs.YigFsAPI.ListDirFiles(r.Context(), dirReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
                ctx.JSON(resp)
                return

	}

	resp.Files = getDirFilesResp
	resp.Result = types.YigFsMetaError {
		ErrCode: 200,
		ErrMsg: "ok",
        }
	resp.Offset = offset

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) CreateFileHandler(ctx iris.Context) {
	resp := types.CreateFileResp {
		Result: types.YigFsMetaError{},
	}
	// get req
	fileReq := &types.FileInfo{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		log.Printf("Failed to read FileInfo from body, err: %v", err)
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	r := ctx.Request()
	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(r.Context(), types.CTX_REQ_ID, uuidStr)

	// create file
	err := yigFs.YigFsAPI.CreateFile(r.Context(), fileReq)
	if err != nil {
		log.Printf("Failed to create file, err: %v", err)
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = types.YigFsMetaError {
		ErrCode: 200,
		ErrMsg: "ok",
	}

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) GetDirFileAttrHandler(ctx iris.Context) {
	resp := &types.GetFileInfoResp{
		Result: types.YigFsMetaError{},
	}
	// get req
	fileReq := &types.GetDirFileInfoReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		log.Printf("Failed to read GetDirFileInfoReq from body, err: %v", err)
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	r := ctx.Request()
	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(r.Context(), types.CTX_REQ_ID, uuidStr)

	// get dir file attr from parent_ino
	getDirFileResp, err:= yigFs.YigFsAPI.GetDirFileAttr(r.Context(), fileReq)
	if err != nil {
		log.Printf("Failed to get dir file attr, err: %v", err)
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = types.YigFsMetaError {
		ErrCode: 200,
		ErrMsg: "ok",
	}
	resp.File = getDirFileResp

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) GetFileAttrHandler(ctx iris.Context) {
	resp := &types.GetFileInfoResp{
		Result: types.YigFsMetaError{},
	}
	// get req
	fileReq := &types.GetFileInfoReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		log.Printf("Failed to read GetFileInfoReq from body, err: %v", err)
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	r := ctx.Request()
	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(r.Context(), types.CTX_REQ_ID, uuidStr)

	// get file info from ino
	getFileAttrResp, err := yigFs.YigFsAPI.GetFileAttr(r.Context(), fileReq)
	if err != nil {
		log.Printf("Failed to get file attr, err: %v", err)
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = types.YigFsMetaError {
		ErrCode: 200,
		ErrMsg: "ok",
	}
	resp.File = getFileAttrResp

	ctx.JSON(resp)
	return
}
