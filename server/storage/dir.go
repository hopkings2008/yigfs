package storage

import (
	"context"
	"log"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)


func(yigFs *YigFsStorage) ListDirFiles(ctx context.Context, dir *types.GetDirFilesReq) (listDirFilesResp []*types.GetDirFileInfo, offset uint64, err error) {
	if dir.BucketName == "" {
                log.Printf("Some ListDirFiles required parameters are missing.")
                err = ErrYigFsMissingRequiredParams
                return
        }

	if dir.Region == "" {
                dir.Region = "cn-bj-1"
        }

	listDirFilesResp, offset, err = yigFs.MetaStorage.Client.ListDirFiles(ctx, dir)
	if err != nil {
		return
	}

	if len(listDirFilesResp) == 0 {
		log.Printf("Not found files in target dir, region: %s, bucket: %s, offset:%s", dir.Region, dir.BucketName, dir.Offset)
		err = ErrYigFsNotFindTargetDirFiles
		return
	}
	return
}

func ChechAndAssignmentFileInfo(file *types.FileInfo) (err error) {
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

func(yigFs *YigFsStorage) CreateFile(ctx context.Context, file *types.FileInfo) (err error) {
	err = ChechAndAssignmentFileInfo(file)
	if err != nil {
		return
	}

	err = yigFs.MetaStorage.Client.CreateFile(ctx, file)
	if err != nil {
		log.Printf("Failed to create file, region: %s, bucket: %s, parent_ino: %d, filename: %s",
			file.Region, file.BucketName, file.ParentIno, file.FileName)
		return
	}
	return
}

func(yigFs *YigFsStorage) GetDirFileAttr(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error) {
	if file.BucketName == "" || file.FileName == "" || file.ParentIno == 0 {
                log.Printf("Some GetDirFileAttr required parameters are missing.")
                err = ErrYigFsMissingRequiredParams
                return
        }

	if file.Region == "" {
                file.Region = "cn-bj-1"
        }

	resp, err = yigFs.MetaStorage.Client.GetDirFileInfo(ctx, file)
	if err != nil {
		log.Printf("Failed to get file attr, region:%s, bucket:%s, parent_ino:%d, filename:%s", file.Region, file.BucketName, file.ParentIno, file.FileName)
		return
	}
	return
}


func(yigFs *YigFsStorage) GetFileAttr(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error) {
	if file.Ino < 2 {
		log.Printf("Ino value is invalid, ino: %d", file.Ino)
		err = ErrYigFsInvalidIno
		return
	}

	resp, err = yigFs.MetaStorage.Client.GetFileInfo(ctx, file)
	if err != nil {
		log.Printf("Failed to get file attr, ino: %d", file.Ino)
		return
	}
	return
}
