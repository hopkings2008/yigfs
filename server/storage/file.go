package storage

import (
	"context"
	"fmt"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
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
	segs, err := yigFs.MetaStorage.Client.GetFileSegmentsInfo(ctx, file)
	if err != nil {
		return
	}

	if err == ErrYigFsNoVaildSegments || len(segs) == 0 {
		helper.Logger.Warn(ctx, fmt.Sprintf("The file does not have segs to delete, region: %v, bucket: %v, ino: %v, generation: %v",
			file.Region, file.BucketName, file.Ino, file.Generation))

		// delete the file.
		err = yigFs.MetaStorage.Client.DeleteFile(ctx, file)
		if err != nil {
			return
		}
		return
	}

	// delete blocks in file_blocks table.
	waitgroup.Add(1)
	var deleteFileBlocksErr error
	go func() {
		defer waitgroup.Done()
		deleteFileBlocksErr = yigFs.MetaStorage.Client.DeleteFileBlocks(ctx, file, segs)
		if deleteFileBlocksErr != nil {
			return
		}
	}()

	// delete blocks in segment_blocks table.
	err = yigFs.MetaStorage.Client.DeleteBlocksBySegsId(ctx, segs)
	if err != nil {
		waitgroup.Wait()
		return
	}

	waitgroup.Wait()
	
	if deleteFileBlocksErr != nil {
		return
	}

	// delete file info.
	err = yigFs.MetaStorage.Client.DeleteFile(ctx, file)
	if err != nil {
		return
	}

	return
}
