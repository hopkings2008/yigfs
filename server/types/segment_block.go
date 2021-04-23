package types

import (
	"context"
)

type UpdateSegBlockInfoReq struct {
	Ctx context.Context `json:"-"`
	ZoneId string `json:"zone"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	SegBlockInfo *UpdateSegBlockInfo `json:"segment"`
}

type UpdateSegBlockInfo struct {
	SegmentId0 uint64 `json:"seg_id0"`
	SegmentId1 uint64 `json:"seg_id1"`
	LatestOffset int `json:"latest_offset"`
	MaxEndAddr int `json:"max_end_addr,omitempty"`
}

type GetIncompleteUploadSegsReq struct {
	Ctx context.Context `json:"-"`
	ZoneId string `json:"zone"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	Machine string `json:"machine"`
}

type GetIncompleteUploadSegsResp struct {
	Result YigFsMetaError `json:"result"`
	Segments []*IncompleteUploadSegInfo `json:"segments"`
}

type IncompleteUploadSegInfo struct {
	SegmentId0   uint64 `json:"seg_id0"`
    SegmentId1   uint64 `json:"seg_id1"`
    NextOffset   int  `json:"next_offset"`
}