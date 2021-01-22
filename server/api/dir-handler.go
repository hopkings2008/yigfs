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
	defer GetSpendTime("GetDirFiles")()

	// get req
	dirReq := &types.GetDirFilesReq{}
	if err := ctx.ReadJSON(&dirReq); err != nil {
		log.Printf("Failed to read GetDirFilesReq from body, err: %v", err)
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if dirReq.BucketName == "" {
		log.Printf("Some GetDirFiles required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if dirReq.Region == "" {
		dirReq.Region = "cn-bj-1"
	}

	r := ctx.Request()
	reqContext := r.Context()
	uuidStr := uuid.New()
	dirReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get dir files from tidb
	if dirReq.Offset <= 1 {
		dirReq.Offset = 2
	}

	getDirFilesResp, offset, err := yigFs.YigFsAPI.ListDirFiles(reqContext, dirReq)
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
	resp := &types.CreateFileResp {
		Result: types.YigFsMetaError{},
	}
	defer GetSpendTime("CreateFileHandler")()

	// get req
	fileReq := &types.FileInfo{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		log.Printf("Failed to read CreateDirFileInfo from body, err: %v", err)
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	err := chechAndAssignmentFileInfo(fileReq)
	if err != nil {
		resp.Result = GetErrInfo(err)
		ctx.JSON(resp)
		return
	}

	r := ctx.Request()
	reqContext := r.Context()
	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// create file
	err = yigFs.YigFsAPI.CreateFile(reqContext, fileReq)
	if err != nil {
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
	defer GetSpendTime("GetDirFileAttrHandler")()

	// get req
	fileReq := &types.GetDirFileInfoReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		log.Printf("Failed to read GetDirFileAttrInfo from body, err: %v", err)
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if fileReq.BucketName == "" || fileReq.FileName == "" || fileReq.ParentIno == 0 {
		log.Printf("Some GetDirFileAttr required parameters are missing.")
		resp.Result = GetErrInfo(ErrYigFsMissingRequiredParams)
		ctx.JSON(resp)
		return
	}

	if fileReq.Region == "" {
		fileReq.Region = "cn-bj-1"
	}

	r := ctx.Request()
	reqContext := r.Context()
	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get dir file attr from parent_ino
	getDirFileResp, err:= yigFs.YigFsAPI.GetDirFileAttr(reqContext, fileReq)
	if err != nil {
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
	defer GetSpendTime("GetFileAttrHandler")()

	// get req
	fileReq := &types.GetFileInfoReq{}
	if err := ctx.ReadJSON(&fileReq); err != nil {
		log.Printf("Failed to read GetFileAttrInfo from body, err: %v", err)
		resp.Result = GetErrInfo(ErrYigFsInvaildParams)
		ctx.JSON(resp)
		return
	}

	// check request params
	if fileReq.Ino < 2 {
		log.Printf("Ino value is invalid, ino: %d", fileReq.Ino)
		resp.Result = GetErrInfo(ErrYigFsInvalidIno)
		ctx.JSON(resp)
		return
	}

	r := ctx.Request()
	reqContext := r.Context()
	uuidStr := uuid.New()
	fileReq.Ctx = context.WithValue(reqContext, types.CTX_REQ_ID, uuidStr)

	// get file info from ino
	getFileAttrResp, err := yigFs.YigFsAPI.GetFileAttr(reqContext, fileReq)
	if err != nil {
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

func chechAndAssignmentFileInfo(file *types.FileInfo) (err error) {
	if file.BucketName == "" || file.FileName == "" || file.Size == 0 || file.ParentIno == 0 {
		log.Printf("Some createFile required parameters are missing.")
		err = ErrYigFsMissingRequiredParams
		return
	}

	if file.Type == 0 {
		file.Type = types.COMMON_FILE
	}
	if file.Region == "" {
		file.Region = "cn-bj-1"
	}
	if file.Perm == 0 {
		file.Perm = types.Read
	}
	return nil
}

