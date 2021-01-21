package types

import (
	"context"
	"time"
)


type YigFsMetaError struct {
	ErrCode int `json:"err_code"`
	ErrMsg  string `json:"err_msg"`
}

type GetDirFilesReq struct {
	Ctx context.Context `json:"-"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	Ino uint64 `json:"ino"`
	Offset uint64 `json:"offset"`
}

type GetDirFilesResp struct {
	Files []*GetDirFileInfo `json:"files"`
	Result YigFsMetaError `json:"result"`
	Offset uint64 `json:"offset"`
	SpentTime *SpentTime `json:"spent_time"`
}

type GetDirFileInfo struct {
	Ctx context.Context `json:"-"`
	Ino uint64 `json:"ino"`
	FileName string `json:"file_name"`
	Type uint32 `json:"type"`
}

type FileInfo struct {
        Ctx context.Context `json:"-"`
        Ino uint64 `json:"ino"`
        Generation uint64 `json:"generation"`
        Region string `json:"region"`
        BucketName string `json:"bucket"`
        ParentIno uint64 `json:"parent_ino"`
        FileName string `json:"file_name"`
        Size uint64 `json:"size"`
        Type uint32 `json:"type"`
        Owner string `json:"owner"`
        Ctime time.Time `json:"ctime"`
        Mtime time.Time `json:"mtime"`
        Atime time.Time `json:"atime"`
        Perm uint32 `json:"perm"`
        Nlink uint32 `json:"nlink"`
        Uid uint32 `json:"uid"`
        Gid uint32 `json:"gid"`
}

type SpentTime struct {
        Sec int64 `json:"sec"`
        Nsec int32 `json:"nsec"`
}

type CreateFileResp struct {
        Ctx context.Context `json:"-"`
        Result YigFsMetaError `json:"result"`
	SpentTime *SpentTime `json:"spent_time"`	
}

type GetDirFileInfoReq struct {
        Ctx context.Context `json:"-"`
        Region string `json:"region"`
        BucketName string `json:"bucket"`
        ParentIno uint64 `json:"parent_ino"`
        FileName string `json:"file_name"`
}

type GetFileInfoResp struct {
        Result YigFsMetaError `json:"result"`
        File *FileInfo `json:"file"`
        SpentTime *SpentTime `json:"spent_time"`
}

type GetFileInfoReq struct {
        Ctx context.Context `json:"-"`
        Region string `json:"region"`
        BucketName string `json:"bucket"`
        Ino uint64 `json:"ino"`
}

