package api

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


type YigFsLayer interface {
	ListDirFiles(ctx context.Context, dir *types.GetDirFilesReq) (listDirFilesResp []*types.GetDirFileInfo, offset uint64, err error)
	GetDirFileAttr(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error)
	GetFileAttr(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error)
	InitDirAndZone(ctx context.Context, rootDir *types.InitDirReq) (err error)
	GetFileLeader(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error)
	CreateFile(ctx context.Context, file *types.CreateFileReq) (resp *types.CreateFileResp, err error)
	SetFileAttr(ctx context.Context, file *types.SetFileAttrReq) (resp *types.SetFileAttrResp, err error)
	CheckSegmentLeader(ctx context.Context, segment *types.CreateSegmentReq) (err error) 
	CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq) (err error)
	UpdateFileSizeAndBlocksNum(ctx context.Context, file *types.GetFileInfoReq, blocksNum uint32, size uint64) (err error)
	GetFileSegmentsInfo(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error)
	UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error)
	GetIncompleteUploadSegs(ctx context.Context, seg *types.GetIncompleteUploadSegsReq) (segs *types.GetIncompleteUploadSegsResp, err error)
	GetTheSlowestGrowingSeg(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error)
	IsFileHasSegments(ctx context.Context, seg *types.GetSegmentReq) (isExisted bool, err error)
	DeleteFile(ctx context.Context, file *types.DeleteFileReq) (err error)
	CheckFileLeader(ctx context.Context, file *types.DeleteFileReq) (err error)
	UpdateFileSegments(ctx context.Context, segs *types.UpdateSegmentsReq) (allBlocksNum uint32, maxSize uint64, err error)
	CheckSegmentsLeader(ctx context.Context, segments *types.UpdateSegmentsReq) (err error)
	UpdateFileSizeAndBlocksNumByCheck(ctx context.Context, file *types.GetFileInfoReq) (err error)
}
