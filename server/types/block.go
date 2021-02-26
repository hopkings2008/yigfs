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
	SegmentId0 int64 `json:"seg_id0"`
	SegmentId1 int64 `json:"seg_id1"`
	Leader string `json:"leader"`
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
	Segment *OneSegmentInfo `json:"segment"`
}

type OneSegmentInfo struct {
	SegmentId0 int64 `json:"seg_id0"`
	SegmentId1 int64 `json:"seg_id1"`
	Block BlockInfo `json:"block"`
}
