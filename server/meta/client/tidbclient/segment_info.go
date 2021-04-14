package tidbclient

import (
	"context"
	"database/sql"
	"time"
	"fmt"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
)

func GetSegmentLeaderSql() (sqltext string) {
	sqltext = "select leader, max_size from segment_info where zone_id=? and region=? and bucket_name=? and seg_id0=? and seg_id1=?"
	return sqltext
}

func CreateSegmentInfoSql() (sqltext string) {
	sqltext = "insert into segment_info values(?,?,?,?,?,?,?,?,?,?,?)"
	return sqltext
}

func (t *TidbClient) GetSegmentInfo(ctx context.Context, segment *types.GetSegLeaderReq) (resp *types.LeaderInfo, err error) {
	resp = &types.LeaderInfo {}

	sqltext := GetSegmentLeaderSql()
	row := t.Client.QueryRow(sqltext, segment.ZoneId, segment.Region, segment.BucketName, segment.SegmentId0, segment.SegmentId1)
	err = row.Scan (
		&resp.Leader,
		&resp.MaxSize,
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

func (t *TidbClient) CreateSegmentInfo(ctx context.Context, segment *types.CreateSegmentReq) (err error) {
	now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)
	latestOffset := 0
	sqltext := CreateSegmentInfoSql()
	args := []interface{}{segment.ZoneId, segment.Region, segment.BucketName, segment.Segment.SegmentId0,
		segment.Segment.SegmentId1, segment.Machine, segment.Segment.MaxSize, latestOffset, now, now, types.NotDeleted}
		
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment leader to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create segment leader to tidb, sqltext: %v", sqltext))
	return
}

func (t *TidbClient) UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)
	sqltext := "update segment_info set latest_offset=?, mtime=? where zone_id=? and region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
	_, err = t.Client.Exec(sqltext, seg.SegBlockInfo.LatestOffset, now, seg.ZoneId, seg.Region, seg.BucketName, 
		seg.SegBlockInfo.SegmentId0, seg.SegBlockInfo.SegmentId1, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to update seg block info to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update seg block info to tidb, latest offset: %v", seg.SegBlockInfo.LatestOffset))
	return
}


