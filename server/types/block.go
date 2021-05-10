package types

import (
	"context"
	"time"
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
	Machine string `json:"machine"`
}

type GetSegmentResp struct {
	Result YigFsMetaError `json:"result"`
	Segments []*SegmentInfo `json:"segments"`
}

type SegmentInfo struct {
	SegmentId0 uint64 `json:"seg_id0"`
	SegmentId1 uint64 `json:"seg_id1"`
	Leader string `json:"leader"`
	Capacity int `json:"capacity,omitempty"`
	BackendSize int `json:"backend_size"`
	Size int `json:"size"`
	Blocks []*BlockInfo `json:"blocks"`
}

type BlockInfo struct {
	Offset int64 `json:"offset"`
	SegStartAddr int `json:"seg_start_addr"`
	SegEndAddr int `json:"seg_end_addr"`
	Size int `json:"size"`
	BlockId int64 `json:"block_id,omitempty"`
	FileBlockEndAddr int64 `json:"file_block_end_addr,omitempty"`
}

type CreateSegmentReq struct {
	Ctx context.Context `json:"-"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	ZoneId string `json:"zone"`
	Machine string `json:"machine"`
	Ino uint64 `json:"ino"`
	Generation uint64 `json:"generation"`
	Segment CreateBlocksInfo `json:"segment"`
}

type CreateBlocksInfo struct {
	SegmentId0 uint64 `json:"seg_id0"`
	SegmentId1 uint64 `json:"seg_id1"`
	Leader string `json:"leader,omitempty"`
	Capacity int `json:"capacity"`
	Blocks []*BlockInfo `json:"blocks"`
}

type UpdateSegResp struct {
	IncreasedSize uint64 `json:"increased_size"`
	DecreasedSize uint64 `json:"decreased_size"`
	IncreasedNumber uint32 `json:"increased_number"`
	DecreasedNumber uint32 `json:"decreased_number"`
}

type UpdateSegmentsReq struct {
	Ctx context.Context `json:"-"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	ZoneId string `json:"zone"`
	Ino uint64 `json:"ino"`
	Generation uint64 `json:"generation"`
	Segments []*CreateBlocksInfo `json:"segments"`
}

type FileBlockInfo struct {
	Region string `json:"region,omitempty"`
	BucketName string `json:"bucket,omitempty"`
	Ino uint64 `json:"ino,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	SegmentId0 uint64 `json:"seg_id0"`
	SegmentId1 uint64 `json:"seg_id1"`
	BlockId int64 `json:"block_id"`
	SegStartAddr int `json:"seg_start_addr"`
	SegEndAddr int `json:"seg_end_addr"`
	FileBlockEndAddr int64 `json:"file_block_end_addr,omitempty"`
	Size int `json:"size"`
	Offset int64 `json:"offset,omitempty"`
	Ctime time.Time `json:"ctime,omitempty"`
}

type UpdateBlocks struct {
	Region string `json:"region,omitempty"`
	BucketName string `json:"bucket,omitempty"`
	Ino uint64 `json:"ino,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	SegmentId0 uint64 `json:"seg_id0,omitempty"`
	SegmentId1 uint64 `json:"seg_id1,omitempty"`
	Blocks []*BlockInfo `json:"blocks,omitempty"`
}

type DescriptBlockInfo struct {
	Region string `json:"region,omitempty"`
	BucketName string `json:"bucket,omitempty"`
	Ino uint64 `json:"ino,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	SegmentId0 uint64 `json:"seg_id0,omitempty"`
	SegmentId1 uint64 `json:"seg_id1,omitempty"`
}

type GetTheSlowestGrowingSeg struct {
	SegmentId0 uint64 `json:"seg_id0"`
	SegmentId1 uint64 `json:"seg_id1"`
	Leader string `json:"leader"`
	Capacity int `json:"capacity,omitempty"`
	BackendSize int `json:"backend_size"`
	Size int `json:"size"`
}
