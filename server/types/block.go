package types

import (
	"context"
)


type GetSegmentReq struct {
	Ctx        context.Context `json:"-"`
	Region     string          `json:"region"`
	BucketName string          `json:"bucket"`
	Ino        uint64          `json:"ino"`
	Generation uint64          `json:"generation"`
	Offset     int64          `json:"offset"`
	Size       int          `json:"size"`
	ZoneId     string 	`json:"zone"`
}

type GetSegmentResp struct {
	Result YigFsMetaError `json:"result"`
	Segments []*SegmentInfo `json:"segments"`
}

type SegmentInfo struct {
	SegmentId0 uint64 `json:"seg_id0"`
	SegmentId1 uint64 `json:"seg_id1"`
	Leader string `json:"leader"`
	MaxSize int `json:"max_size,omitempty"`
	Blocks []BlockInfo `json:"blocks"`
}

type BlockInfo struct {
	Offset int64 `json:"offset"`
	SegStartAddr int64 `json:"seg_start_addr"`
	SegEndAddr int64 `json:"seg_end_addr"`
	Size int `json:"size"`
}

type CreateSegmentReq struct {
	Ctx context.Context `json:"-"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	ZoneId string `json:"zone"`
	Machine string `json:"machine"`
	Ino uint64 `json:"ino"`
	Generation uint64 `json:"generation"`
	CoveredBlockOffset int64 `json:"covered_block_offset,omitempty"`
	Segment CreateBlocksInfo `json:"segment"`
}

type CreateBlocksInfo struct {
	SegmentId0 uint64 `json:"seg_id0"`
	SegmentId1 uint64 `json:"seg_id1"`
	MaxSize int `json:"max_size"`
	Blocks []BlockInfo `json:"blocks"`
}
