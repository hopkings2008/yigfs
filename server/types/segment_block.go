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
	BackendSize int `json:"backend_size"`
	Size int `json:"size,omitempty"`
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
	UploadSegments []*IncompleteUploadSegInfo `json:"upload_segments"`
	RemoveSegments []*RemoveSegInfo `json:"remove_segments"`
}

type IncompleteUploadSegInfo struct {
	SegmentId0   uint64 `json:"seg_id0"`
	SegmentId1   uint64 `json:"seg_id1"`
	NextOffset   int  `json:"next_offset"`
}

type RemoveSegInfo struct {
	SegmentId0   uint64 `json:"seg_id0"`
	SegmentId1   uint64 `json:"seg_id1"`
}
