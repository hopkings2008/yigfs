package types

import (
	"context"
	"time"
)


type YigFsMetaResp struct {
	Data []byte
	Err error
}

type GetDirFilesReq struct {
	Ctx context.Context `json:"-"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	Ino uint64 `json:"ino"`
	Offset uint64 `json:"offset"`
}

type GetDirFilesResp struct {
	Ctx context.Context `json:"-"`
	Ino uint64 `json:"ino"`
	FileName string `json:"filename"`
	Type uint32 `json:"type"`
}

type CreateDirFileReq struct {
	Ctx context.Context `json:"-"`
	Ino uint64 `json:"ino"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	ParentIno uint64 `json:"parentino"`
	FileName string `json:"filename"`
	Size uint64 `json:"size"`
	Type uint32 `json:"type"`
	Owner string `json:"owner"`
	Atime time.Time `json:"atime"`
	Perm uint32 `json:"perm"`
	Nlink uint32 `json:"nlink"`
	Uid uint32 `json:"uid"`
	Gid uint32 `json:"gid"`
}

type CreateDirFileResp struct {
	Ctx context.Context `json:"-"`
	Ino uint64 `json:"ino"`
}
