package tidbclient

import (
	"context"
	"database/sql"
	"fmt"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
)

func GetSegmentLeaderSql() (sqltext string) {
	sqltext = "select leader from segment_zone where zone_id=? and region=? and bucket_name=? and seg_id0=? and seg_id1=?"
	return sqltext
}

func CreateSegmentZoneSql() (sqltext string) {
	sqltext = "insert into segment_zone(zone_id, region, bucket_name, seg_id0, seg_id1, leader) values(?,?,?,?,?,?)"
	return sqltext
}

func GetSegmentsByLeaderSql() (sqltext string) {
	sqltext = "select seg_id0, seg_id1 from segment_zone where zone_id=? and region=? and bucket_name=? and leader=?"
	return sqltext
}

func (t *TidbClient) GetSegmentLeader(ctx context.Context, segment *types.GetSegLeaderReq) (resp *types.LeaderInfo, err error) {
	resp = &types.LeaderInfo {}

	sqltext := GetSegmentLeaderSql()
	row := t.Client.QueryRow(sqltext, segment.ZoneId, segment.Region, segment.BucketName, segment.SegmentId0, segment.SegmentId1)
	err = row.Scan (
		&resp.Leader,
	)

	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchLeader
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the segment leader, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	resp.ZoneId = segment.ZoneId
	helper.Logger.Info(ctx, fmt.Sprintf("succeed to get the segment leader from tidb, sqltext: %v", sqltext))
	return
}

func(t *TidbClient) GetSegsByLeader(ctx context.Context, seg *types.GetIncompleteUploadSegsReq) (segsResp []*types.IncompleteUploadSegInfo, err error) {
	sqltext := GetSegmentsByLeaderSql()
	rows, err := t.Client.Query(sqltext, seg.ZoneId, seg.Region, seg.BucketName, seg.Machine)
	if err == sql.ErrNoRows {
		err = ErrYigFsNoTargetSegment
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get segments by leader, err: %v", err))
		return
	}
	defer rows.Close()

	for rows.Next() {
		segInfo := &types.IncompleteUploadSegInfo{}
		err = rows.Scan(
			&segInfo.SegmentId0,
			&segInfo.SegmentId1,
		)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to scan query segments getting by leader, err: %v", err))
			return
		}

		segsResp = append(segsResp, segInfo)
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to iterator rows for segments getting by leader, err: %v", err))
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("succeed to get segments by leader, number: %v", len(segsResp)))
	return
}


