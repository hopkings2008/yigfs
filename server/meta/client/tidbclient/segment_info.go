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
	sqltext = "select leader, max_size from segment_info where zone_id=? and region=? and bucket_name=? and seg_id0=? and seg_id1=?"
	return sqltext
}

func CreateSegmentInfoSql() (sqltext string) {
	sqltext = "insert into segment_info(zone_id, region, bucket_name, seg_id0, seg_id1, leader, max_size) values(?,?,?,?,?,?,?)"
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
	sqltext := CreateSegmentInfoSql()
	args := []interface{}{segment.ZoneId, segment.Region, segment.BucketName, segment.Segment.SegmentId0,
		segment.Segment.SegmentId1, segment.Machine, segment.Segment.MaxSize}
		
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
	sqltext := "update segment_info set latest_offset=? where zone_id=? and region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
	_, err = t.Client.Exec(sqltext, seg.SegBlockInfo.LatestOffset, seg.ZoneId, seg.Region, seg.BucketName, 
		seg.SegBlockInfo.SegmentId0, seg.SegBlockInfo.SegmentId1, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to update seg block info to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update seg block info to tidb, latest offset: %v", seg.SegBlockInfo.LatestOffset))
	return
}

func(t *TidbClient) GetIncompleteUploadSegs(ctx context.Context, seg *types.GetIncompleteUploadSegsReq) (segsResp *types.GetIncompleteUploadSegsResp, err error) {
	segsResp = &types.GetIncompleteUploadSegsResp{}
	sqltext := "select seg_id0, seg_id1, latest_offset, max_end_addr from segment_info where zone_id=? and region=? and bucket_name=? and leader=? and is_deleted=?"
	rows, err := t.Client.Query(sqltext, seg.ZoneId, seg.Region, seg.BucketName, seg.Machine, types.NotDeleted)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get incomplete segs by leader, err: %v", err))
		return
	}
	defer rows.Close()

	var segId0, segId1 uint64
	var latestOffset, maxEndAddr int

	for rows.Next() {
		err = rows.Scan(
			&segId0,
			&segId1,
			&latestOffset,
			&maxEndAddr)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to scan query incomplete segs getting by leader, err: %v", err))
			return
		}

		if latestOffset < maxEndAddr {
			segInfo := &types.IncompleteUploadSegInfo{
				SegmentId0: segId0,
				SegmentId1: segId1,
				NextOffset: latestOffset,
			}

			segsResp.UploadSegments = append(segsResp.UploadSegments, segInfo)
		}
	}

	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to iterator rows for incomplete segs getting by leader, err: %v", err))
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get incomplete segs by leader, segs number: %v", len(segsResp.UploadSegments)))
	return
}

func (t *TidbClient) UpdateSegLatestEndAddr(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	sqltext := "select max_end_addr from segment_info where zone_id=? and region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
	var maxEndAddr int
	row := t.Client.QueryRow(sqltext, seg.ZoneId, seg.Region, seg.BucketName, seg.SegBlockInfo.SegmentId0, seg.SegBlockInfo.SegmentId1, types.NotDeleted)
	err = row.Scan (
		&maxEndAddr,
	)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("UpdateSegLatestEndAddr: Failed to get the latest_end_add, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	if seg.SegBlockInfo.MaxEndAddr > maxEndAddr {
		sqltext = "update segment_info set max_end_addr=? where zone_id=? and region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
		_, err = t.Client.Exec(sqltext, seg.SegBlockInfo.MaxEndAddr, seg.ZoneId, seg.Region, seg.BucketName, 
			seg.SegBlockInfo.SegmentId0, seg.SegBlockInfo.SegmentId1, types.NotDeleted)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to update seg latest end addr, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}
		helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update seg latest end addr, latest end addr: %v", seg.SegBlockInfo.MaxEndAddr))
	}

	helper.Logger.Info(ctx, "Succeed to update seg latest end addr")
	return
}

