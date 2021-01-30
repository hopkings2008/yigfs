package types

import (
	"context"
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
        Ctime int64 `json:"ctime"`
        Mtime int64 `json:"mtime"`
        Atime int64 `json:"atime"`
        Perm uint32 `json:"perm"`
        Nlink uint32 `json:"nlink"`
        Uid uint32 `json:"uid"`
        Gid uint32 `json:"gid"`
	Blocks uint32 `json:"blocks"`
}

type CreateFileReq struct {
	Ctx context.Context `json:"-"`
	ZoneId string `json:"zone"`
	Machine string `json:"machine"`
	Ino uint64 `json:"ino"`
	Generation uint64 `json:"generation"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	ParentIno uint64 `json:"parent_ino"`
	FileName string `json:"file_name"`
	Size uint64 `json:"size"`
	Type uint32 `json:"type"`
	Ctime int64 `json:"ctime"`
	Mtime int64 `json:"mtime"`
	Atime int64 `json:"atime"`
	Perm uint32 `json:"perm"`
	Nlink uint32 `json:"nlink"`
	Uid uint32 `json:"uid"`
	Gid uint32 `json:"gid"`
	Blocks uint32 `json:"blocks"`
}

type CreateFileResp struct {
        Ctx context.Context `json:"-"`
        Result YigFsMetaError `json:"result"`
	LeaderInfo *LeaderInfo `json:"leader_info"`
	File *FileInfo `json:"file"`
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
}

type GetFileInfoReq struct {
	Ctx context.Context `json:"-"`
	Region string `json:"region"`
        BucketName string `json:"bucket"`
        Ino uint64 `json:"ino"`
}

type InitDirReq struct {
	Ctx context.Context `json:"-"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	ZoneId string `json:"zone"`
	Machine string `json:"machine"`
	Uid uint32 `json:"uid"`
	Gid uint32 `json:"gid"`
}

type InitDirResp struct {
	Result YigFsMetaError `json:"result"`
}

type SetFileAttrInfo struct {
        Ino uint64 `json:"ino"`
        Size uint64 `json:"size"`
        Ctime int64 `json:"ctime"`
        Mtime int64 `json:"mtime"`
        Atime int64 `json:"atime"`
        Perm uint32 `json:"perm"`
        Uid uint32 `json:"uid"`
        Gid uint32 `json:"gid"`
        Blocks uint32 `json:"blocks"`
}

type SetFileAttrReq struct {
	Ctx context.Context `json:"-"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	File *SetFileAttrInfo `json:"file"`
}

type SetFileAttrResp struct {
	Result YigFsMetaError `json:"result"`
	File *FileInfo `json:"file"`	
}
