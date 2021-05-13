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


func(yigFs MetaAPIHandlers) GetDirFilesHandler(ctx iris.Context) {
	resp := &types.GetDirFilesResp {
		Files: []*types.GetDirFileInfo{},
        	Result: types.YigFsMetaError {},
	}
	defer GetSpendTime("GetDirFiles")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	dirReq := &types.GetDirFilesReq{}
	if err := ctx.ReadJSON(&dirReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read GetDirFilesReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if dirReq.BucketName == "" || dirReq.ParentIno == 0 {
		helper.Logger.Error(reqContext, "Some GetDirFiles required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if dirReq.Region == "" {
		dirReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	dirReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get dir files from tidb
	if dirReq.Offset <= 0 {
		dirReq.Offset = 0
	}

	getDirFilesResp, offset, err := yigFs.YigFsAPI.ListDirFiles(reqContext, dirReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Files = getDirFilesResp
	resp.Result = GetErrInfo(NoYigFsErr)
	resp.Offset = offset

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) CreateFileHandler(ctx iris.Context) {
	resp := &types.CreateFileResp {
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("CreateFileHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	fileReq := &types.CreateFileReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read CreateDirFileInfo from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	err := CheckAndAssignmentFileInfo(reqContext, fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// create file
	resp, err = yigFs.YigFsAPI.CreateFile(reqContext, fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
	} else {
		resp.Result = GetErrInfo(NoYigFsErr)
	}
	
	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) GetDirFileAttrHandler(ctx iris.Context) {
	resp := &types.GetFileInfoResp{
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("GetDirFileAttrHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	fileReq := &types.GetDirFileInfoReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read GetDirFileInfoReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if fileReq.BucketName == "" || fileReq.FileName == "" || fileReq.ParentIno == 0 {
		helper.Logger.Error(reqContext, "Some GetDirFileAttr required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if fileReq.Region == "" {
		fileReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get dir file attr from parent_ino
	getDirFileResp, err:= yigFs.YigFsAPI.GetDirFileAttr(reqContext, fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = GetErrInfo(NoYigFsErr)
	resp.File = getDirFileResp

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) GetFileAttrHandler(ctx iris.Context) {
	resp := &types.GetFileInfoResp{
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("GetFileAttrHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	fileReq := &types.GetFileInfoReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read GetFileInfoReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if fileReq.BucketName == "" || fileReq.Ino == 0 {
		helper.Logger.Error(reqContext, "Some GetFileAttr required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if fileReq.Region == "" {
		fileReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get file info from ino
	getFileAttrResp, err := yigFs.YigFsAPI.GetFileAttr(reqContext, fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = GetErrInfo(NoYigFsErr)
	resp.File = getFileAttrResp

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) InitDirHandler(ctx iris.Context) {
	resp := &types.NonBodyResp {
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("InitDirHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	dirReq := &types.InitDirReq{}
	if err := ctx.ReadJSON(&dirReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read InitDirReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if dirReq.BucketName == "" || dirReq.Machine == "" || dirReq.ZoneId =="" {
		helper.Logger.Error(reqContext, "Some InitDirHandler required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if dirReq.Region == "" {
		dirReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	dirReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// init dir and zone
	err := yigFs.YigFsAPI.InitDirAndZone(reqContext, dirReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) SetFileAttrHandler(ctx iris.Context) {
	resp := &types.SetFileAttrResp {
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("SetFileAttrHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	fileReq := &types.SetFileAttrReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read SetFileAttrReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	err := CheckSetFileAttrParams(reqContext, fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// set file attr
	resp, err = yigFs.YigFsAPI.SetFileAttr(reqContext, fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	resp.Result = GetErrInfo(NoYigFsErr)

	ctx.JSON(resp)
	return
}

func(yigFs MetaAPIHandlers) DeleteFileHandler(ctx iris.Context) {
	resp := &types.NonBodyResp {
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("DeleteFileHandler")()

	r := ctx.Request()
	reqContext := r.Context()

	// get req
	fileReq := &types.DeleteFileReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		helper.Logger.Error(reqContext, fmt.Sprintf("Failed to read DeleteFileReq from body, err: %v", err))
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if fileReq.BucketName == "" || fileReq.Machine == "" || fileReq.ZoneId =="" {
		helper.Logger.Error(reqContext, "Some DeleteFileHandler required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if fileReq.Region == "" {
		fileReq.Region = "cn-bj-1"
	}

	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// check whether the machine is the file leader or not.
	isMatch, err := yigFs.YigFsAPI.CheckFileLeader(reqContext, fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	if !isMatch {
		resp.Result = GetErrInfo(ErrYigFsMachineNotMatchFileLeader)
		ctx.JSON(resp)
		return
	}

	// delete file
	err = yigFs.YigFsAPI.DeleteFile(reqContext, fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
	} else {
		resp.Result = GetErrInfo(NoYigFsErr)
	}
	
	ctx.JSON(resp)
	return
}