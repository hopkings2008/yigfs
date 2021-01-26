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

func(yigFs *YigFsStorage) CreateFile(ctx context.Context, file *types.FileInfo) (err error) {
	err = yigFs.MetaStorage.Client.CreateFile(ctx, file)
	if err != nil {
		log.Printf("Failed to create file, region: %s, bucket: %s, parent_ino: %d, filename: %s, err: %v", file.Region, file.BucketName, file.ParentIno, file.FileName, err)
		return
	}
	return
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

func(yigFs *YigFsStorage) InitDir(ctx context.Context, rootDir *types.InitDirReq) (err error) {
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

        return
}

