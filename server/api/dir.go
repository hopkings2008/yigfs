package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"log"

	"github.com/google/uuid"
	"github.com/hopkings2008/yigfs/server/endpoint"
	"github.com/hopkings2008/yigfs/server/types"
)


func(yigFs MetaAPIHandlers) GetDirFilesHandler(w http.ResponseWriter, r *http.Request) {
	// check request
	ctx := r.Context()
	isValid, size, err := endpoint.IsRequestValid(ctx, r)
	if !isValid {
		code := endpoint.GetStatusCode(err)
		log.Fatal(err)
		w.WriteHeader(code)
	}

	// get req
	buf := make([]byte, size)
	n, err := r.Body.Read(buf)
	if err != nil {
		err = io.EOF
		log.Fatal("read GetDirFilesHandler body failed, err:", err)
		w.WriteHeader(500)
	}

	dirReq := &types.GetDirFilesReq{}
	err = json.Unmarshal(buf[:n], dirReq)
	if err != nil {
		log.Fatal("Failed to json GetDirFilesReq")
		w.WriteHeader(500)
	}

	uuidStr := uuid.New()
	dirReq.Ctx = context.WithValue(ctx, types.CTX_REQ_ID, uuidStr)

	// get dir files
	if dirReq.Offset <= 0 {
		dirReq.Offset = 1
	}

	dirFilesResp := yigFs.YigFsAPI.ListDirFiles(ctx, dirReq)
	if dirFilesResp.Err != nil {
		code := endpoint.GetStatusCode(dirFilesResp.Err)
		w.WriteHeader(code)
		w.Write([]byte(dirFilesResp.Err.Error()))
		return
	}

	// response result to client
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(dirFilesResp.Data)
	if err != nil {
		log.Fatal("Failed to write dirFilesResp, err:", err)
		w.WriteHeader(500)
	}
	return
}
