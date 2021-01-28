package types

import (
	"context"
)

type GetLeaderReq struct {
	Ctx context.Context `json:"-"`
	ZoneId string `json:"zone"`
	Region string `json:"region"`
	BucketName string `json:"bucket"`
	Ino uint64 `json:"ino"`
	Machine string `json:"machine"`
	Flag int `json:"flag"`
}

type GetLeaderResp struct {
	Result YigFsMetaError `json:"result"`
	LeaderInfo *LeaderInfo `json:"leader_info"`
}

type LeaderInfo struct {
	ZoneId string `json:"zone"`
	Leader string `json:"leader"`
}
