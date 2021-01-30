package storage

import (
	"context"
	"log"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)


func(yigFs *YigFsStorage) ListDirFiles(ctx context.Context, dir *types.GetDirFilesReq) (listDirFilesResp []*types.GetDirFileInfo, offset uint64, err error) {
	listDirFilesResp, offset, err = yigFs.MetaStorage.Client.ListDirFiles(ctx, dir)
	if err != nil {
		log.Printf("Failed to list dir files, err: %v", err)
		return
	}

	if len(listDirFilesResp) == 0 {
		log.Printf("Not found files in target dir, region: %s, bucket: %s, offset: %d", dir.Region, dir.BucketName, dir.Offset)
		err = ErrYigFsNotFindTargetDirFiles
		return
	}
	return
}

func(yigFs *YigFsStorage) CreateFile(ctx context.Context, file *types.CreateFileReq) (resp *types.CreateFileResp, err error) {
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
			log.Printf("Failed to create file, region: %s, bucket: %s, parent_ino: %d, filename: %s, err: %v", file.Region, file.BucketName, file.ParentIno, file.FileName, err)
			return
		}

		// get file info
		dirFileInfoResp, err = yigFs.MetaStorage.Client.GetDirFileInfo(ctx, getFileReq)
		if err != nil {
			return
		}

		resp = &types.CreateFileResp {
			File: dirFileInfoResp,
		}

		// create or update leader and zone
		leader := &types.GetLeaderReq {
			ZoneId: file.ZoneId,
			Region:file.Region,
			BucketName: file.BucketName,
			Ino: dirFileInfoResp.Ino,
			Machine: file.Machine,
		}

		err = UpdateLeaderAndZone(ctx, leader, yigFs)
		if err != nil {
			return
		}

		resp.LeaderInfo = &types.LeaderInfo {
			ZoneId: leader.ZoneId,
			Leader: leader.Machine,
		}
		return
	case nil:
		// if file exist, get it leader.
                resp = &types.CreateFileResp {
                        File: dirFileInfoResp,
                }

		leader := &types.GetLeaderReq {
			ZoneId: file.ZoneId,
			Region:file.Region,
			BucketName: file.BucketName,
			Ino: dirFileInfoResp.Ino,
			Machine: file.Machine,
		}

		var getLeaderResp = &types.GetLeaderResp{}
		getLeaderResp, err = GetUpLeader(ctx, leader, yigFs)
		if err != nil {
			return
		}

		resp.LeaderInfo = getLeaderResp.LeaderInfo
		return
	default:
		log.Printf("Failed to get file attr, region: %s, bucket: %s, parent_ino: %d, filename: %s, err: %v", 
			getFileReq.Region, getFileReq.BucketName, getFileReq.ParentIno, getFileReq.FileName, err)
		return
	}
}

func(yigFs *YigFsStorage) GetDirFileAttr(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error) {
	resp, err = yigFs.MetaStorage.Client.GetDirFileInfo(ctx, file)
	if err != nil {
		log.Printf("Failed to get file attr, region: %s, bucket: %s, parent_ino: %d, filename: %s, err: %v", file.Region, file.BucketName, file.ParentIno, file.FileName, err)
		return
	}
	return
}

func(yigFs *YigFsStorage) GetFileAttr(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error) {
	resp, err = yigFs.MetaStorage.Client.GetFileInfo(ctx, file)
	if err != nil {
		log.Printf("Failed to get file attr, region: %s, bucket: %s, ino: %d, err: %v", file.Region, file.BucketName, file.Ino, err)
		return
	}
	return
}

func(yigFs *YigFsStorage) InitDirAndZone(ctx context.Context, rootDir *types.InitDirReq) (err error) {
	// init dir
	file := &types.GetFileInfoReq {
		Region: rootDir.Region,
		BucketName: rootDir.BucketName,
		Ino: types.RootDirIno,
	}
	_, err = yigFs.MetaStorage.Client.GetFileInfo(ctx, file)
        if err != nil && err != ErrYigFsNoSuchFile {
                log.Printf("Failed to get file attr, region: %s, bucket: %s, ino: %d, err: %v", file.Region, file.BucketName, file.Ino, err)
                return
        } else if err == ErrYigFsNoSuchFile {
		err = yigFs.MetaStorage.Client.InitRootDir(ctx, rootDir)
		if err != nil {
			log.Printf("Failed to init root dir, region: %s, bucket: %s, err: %v", rootDir.Region, rootDir.BucketName, err)
			return		
		}
	}

	file.Ino = types.RootParentDirIno
	_, err = yigFs.MetaStorage.Client.GetFileInfo(ctx, file)
        if err != nil && err != ErrYigFsNoSuchFile {
		log.Printf("Failed to get file attr, region: %s, bucket: %s, ino: %d, err: %v", file.Region, file.BucketName, file.Ino, err)
                return
        } else if err == ErrYigFsNoSuchFile {
                err = yigFs.MetaStorage.Client.InitParentDir(ctx, rootDir)
                if err != nil {
                        log.Printf("Failed to init parent dir, region: %s, bucket: %s, err: %v", rootDir.Region, rootDir.BucketName, err)
                        return
                }
        }

	// init zone
	err = yigFs.MetaStorage.Client.CreateOrUpdateZone(ctx, rootDir)
	if err != nil {
		log.Printf("Failed to init zone, region: %s, bucket: %s, zone_id: %s, machine: %s, err: %v", rootDir.Region, rootDir.BucketName, rootDir.ZoneId, rootDir.Machine, err)
		return
	}
	return nil
}

