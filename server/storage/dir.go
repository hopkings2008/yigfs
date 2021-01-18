package storage

import (
	"context"
	"encoding/json"
	"log"

	"github.com/hopkings2008/yigfs/server/types"

)


func(yigFs *YigFsStorage) ListDirFiles(ctx context.Context, files *types.GetDirFilesReq) (resp *types.YigFsMetaResp) {
	dirFilesResp, err := yigFs.MetaStorage.Client.ListDirFiles(ctx, files)
	resp = &types.YigFsMetaResp{
		Err: nil,
	}
	if err != nil {
		log.Fatal("Failed to list dir files, bucket:", files.BucketName)
		resp.Err = err
		return
	}

	data, err := json.Marshal(dirFilesResp)
	if err != nil {
		log.Fatal("Failed to convert dirFilesResp to json, err:", err)
		resp.Err = err
		return
	}

	resp.Data = data
	return resp
}
