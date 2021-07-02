package storage

import (
	"context"
	"fmt"
	"encoding/json"
	"time"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/message/builder"
)


func(yigFs *YigFsStorage) ListDirFiles(ctx context.Context, dir *types.GetDirFilesReq) (listDirFilesResp []*types.GetDirFileInfo, offset uint64, err error) {
	listDirFilesResp, offset, err = yigFs.MetaStorage.Client.ListDirFiles(ctx, dir)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to list dir files, err: %v", err))
		return
	}

	if len(listDirFilesResp) == 0 {
		helper.Logger.Error(ctx, fmt.Sprintf("Not found files in target dir, region: %s, bucket: %s, offset: %d", dir.Region, dir.BucketName, dir.Offset))
		err = ErrYigFsNotFindTargetDirFiles
		return
	}
	return
}

func(yigFs *YigFsStorage) CreateFile(ctx context.Context, file *types.CreateFileReq) (resp *types.CreateFileResp, err error) {
	resp = &types.CreateFileResp {}
	var getUpFileLeaderResp = &types.GetLeaderResp{}
	
	// check file exist or not
	getFileReq := &types.GetDirFileInfoReq {
		Region: file.Region,
		BucketName: file.BucketName,
		ParentIno: file.ParentIno,
		FileName: file.FileName,
	}

	// get file
	var dirFileInfoResp = &types.FileInfo{}
	dirFileInfoResp, err = yigFs.MetaStorage.Client.GetDirFileInfo(ctx, getFileReq)

	switch err {
	case ErrYigFsNoSuchFile:
		// if file does not exist, create it.
		err = yigFs.MetaStorage.Client.CreateFile(ctx, file)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to create file, region: %s, bucket: %s, parent_ino: %d, filename: %s, err: %v", 
				file.Region, file.BucketName, file.ParentIno, file.FileName, err))
			return
		}

		// get file info
		dirFileInfoResp, err = yigFs.MetaStorage.Client.GetDirFileInfo(ctx, getFileReq)
		if err != nil {
			return
		}

		resp.File = dirFileInfoResp

		// leader info
		resp.LeaderInfo = &types.LeaderInfo {
			ZoneId: file.ZoneId,
			Leader: file.Machine,
		}
		return
	case nil:
		// if file exist, return ErrYigFsFileAlreadyExist and leader info.
		resp.File = dirFileInfoResp

		leader := &types.GetLeaderReq {
			ZoneId: file.ZoneId,
			Region: file.Region,
			BucketName: file.BucketName,
			Ino: dirFileInfoResp.Ino,
		}

		// get leader
		getUpFileLeaderResp, err = GetUpFileLeader(ctx, leader, yigFs)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("CreateFile: Failed to get leader for existed file, zone_id: %s, region: %s, bucket: %s, ino: %d, err: %v",
				file.ZoneId, file.Region, file.BucketName, dirFileInfoResp.Ino, err))
			return
		}

		resp.LeaderInfo = getUpFileLeaderResp.LeaderInfo
		return resp, ErrYigFsFileAlreadyExist
	default:
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get file attr, region: %s, bucket: %s, parent_ino: %d, filename: %s, err: %v", 
			getFileReq.Region, getFileReq.BucketName, getFileReq.ParentIno, getFileReq.FileName, err))
		return
	}
}

func(yigFs *YigFsStorage) GetDirFileAttr(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error) {
	resp, err = yigFs.MetaStorage.Client.GetDirFileInfo(ctx, file)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get file attr, region: %s, bucket: %s, parent_ino: %d, filename: %s, err: %v", 
			file.Region, file.BucketName, file.ParentIno, file.FileName, err))
		return
	}
	return
}

func(yigFs *YigFsStorage) GetFileAttr(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error) {
	resp, err = yigFs.MetaStorage.Client.GetFileInfo(ctx, file)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get file attr, region: %s, bucket: %s, ino: %d, err: %v", file.Region, file.BucketName, file.Ino, err))
		return
	}
	return
}

func(yigFs *YigFsStorage) InitDirAndZone(ctx context.Context, rootDir *types.InitDirReq) (err error) {
	// init dir
	err = yigFs.MetaStorage.Client.InitRootDirs(ctx, rootDir)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to init dirs, region: %s, bucket: %s, zoneId: %s, machine: %s, err: %v", 
			rootDir.Region, rootDir.BucketName, rootDir.ZoneId, rootDir.Machine, err))
		return
	}
	return
}

func(yigFs *YigFsStorage) SetFileAttr(ctx context.Context, file *types.SetFileAttrReq) (resp *types.SetFileAttrResp, err error) {
	resp = &types.SetFileAttrResp{}

	err = yigFs.MetaStorage.Client.SetFileAttr(ctx, file)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to set file attr, region: %s, bucket: %s, ino: %d", file.Region, file.BucketName, file.File.Ino))
		return resp, err
	}

	getFileInfoResp := &types.GetFileInfoReq {
		Region: file.Region,
		BucketName: file.BucketName,
		Ino: file.File.Ino,
	}

	getFileInfoReq, err := yigFs.MetaStorage.Client.GetFileInfo(ctx, getFileInfoResp)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("SetFileAttr: Failed to get file info, region: %s, bucket: %s, ino: %d, err: %v",
			getFileInfoResp.Region, getFileInfoResp.BucketName, getFileInfoResp.Ino, err))
		return resp, err
	}

	resp.File = getFileInfoReq
	return resp, nil
}

func(yigFs *YigFsStorage) DeleteFile(ctx context.Context, file *types.DeleteFileReq) (err error) {
	// put delete file param to kafka.
	start := time.Now().UTC().UnixNano()
	value, err := json.Marshal(file)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to make fileReq to json, err: %v", err))
		return
	}

	err = builder.SendMessage(types.DeleteBlocksTopic, types.DeleteFile, value)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to send delete file msg, err: %v", err))
		return
	}
	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("put delete file to kafka cost: %v", end - start))

	// delete file info.
	err = yigFs.MetaStorage.Client.DeleteFile(ctx, file)
	if err != nil {
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to delete file, region: %s, bucket: %s, ino: %d, generation: %v", 
		file.Region, file.BucketName, file.Ino, file.Generation))
	return
}

func(yigFs *YigFsStorage) UpdateFileSizeAndBlocksNum(ctx context.Context, file *types.GetFileInfoReq, blocksNum uint32, size uint64) (err error) {
	err = yigFs.MetaStorage.Client.UpdateSizeAndBlocksNum(ctx, file, blocksNum, size)
	if err != nil {
		return
	}
	return
}

func(yigFs *YigFsStorage) UpdateFileSizeAndBlocksNumByCheck(ctx context.Context, file *types.GetFileInfoReq) (err error) {
	err = yigFs.MetaStorage.Client.UpdateFileSizeAndBlocksNum(ctx, file)
	if err != nil {
		return
	}
	return
}

func(yigFs *YigFsStorage) RenameFile(ctx context.Context, file *types.RenameFileReq) (err error) {
	err = yigFs.MetaStorage.Client.RenameFile(ctx, file)
	if err != nil {
		return
	}
	return
}
