package tidbclient

import (
	"context"
	"database/sql"
	"fmt"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
)

func GetSegmentInfoSql() (sqltext string) {
	sqltext = "select capacity, backend_size, size from segment_info where region=? and bucket_name=? and seg_id0=? and seg_id1=?"
	return sqltext
}

func CreateSegmentInfoSql() (sqltext string) {
	sqltext = "insert into segment_info(region, bucket_name, seg_id0, seg_id1, capacity) values(?,?,?,?,?)"
	return sqltext
}

func (t *TidbClient) CreateSegmentInfo(ctx context.Context, segment *types.CreateSegmentReq) (err error) {
	sqltext := CreateSegmentZoneSql()
	args := []interface{}{segment.ZoneId, segment.Region, segment.BucketName, segment.Segment.SegmentId0,
		segment.Segment.SegmentId1, segment.Machine}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment zone to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	sqltext = CreateSegmentInfoSql()
	args = []interface{}{segment.Region, segment.BucketName, segment.Segment.SegmentId0, segment.Segment.SegmentId1, segment.Segment.Capacity}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment info to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create segment info and zone to tidb, sqltext: %v", sqltext))
	return
}

func (t *TidbClient) UpdateSegBlockInfo(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	sqltext := "update segment_info set backend_size=? where region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
	_, err = t.Client.Exec(sqltext, seg.SegBlockInfo.BackendSize, seg.Region, seg.BucketName, seg.SegBlockInfo.SegmentId0, 
		seg.SegBlockInfo.SegmentId1, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to update segment block info to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update segment block info to tidb, backend_size: %v", seg.SegBlockInfo.BackendSize))
	return
}

func(t *TidbClient) GetIncompleteUploadSegs(ctx context.Context, segInfo *types.GetIncompleteUploadSegsReq, 
	segs []*types.IncompleteUploadSegInfo) (segsResp *types.GetIncompleteUploadSegsResp, err error) {
	segsResp = &types.GetIncompleteUploadSegsResp{}
	sqltext := "select backend_size, size from segment_info where region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
	var stmt *sql.Stmt
	stmt, err = t.Client.Prepare(sqltext)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to prepare get incomplete upload segments, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to close get incomplete upload segments stmt, err: %v", err))
			err = ErrYIgFsInternalErr
		}
	}()

	var backendSize, size int
	for _, seg := range segs {
		row := stmt.QueryRow(segInfo.Region, segInfo.BucketName, seg.SegmentId0, seg.SegmentId1, types.NotDeleted)
		err = row.Scan (
			&backendSize,
			&size,
		)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get incomplete segs by leader, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		if backendSize < size {
			segInfo := &types.IncompleteUploadSegInfo{
				SegmentId0: seg.SegmentId0,
				SegmentId1: seg.SegmentId1,
				NextOffset: backendSize,
			}
			segsResp.UploadSegments = append(segsResp.UploadSegments, segInfo)
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get incomplete segs by leader, segs number: %v", len(segsResp.UploadSegments)))
	return
}

func (t *TidbClient) UpdateSegSize(ctx context.Context, seg *types.UpdateSegBlockInfoReq) (err error) {
	sqltext := "select size from segment_info where region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
	var size int
	row := t.Client.QueryRow(sqltext, seg.Region, seg.BucketName, seg.SegBlockInfo.SegmentId0, seg.SegBlockInfo.SegmentId1, types.NotDeleted)
	err = row.Scan (
		&size,
	)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("UpdateSegSize: Failed to get the segment size, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	if seg.SegBlockInfo.Size > size {
		sqltext = "update segment_info set size=? where region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
		_, err = t.Client.Exec(sqltext, seg.SegBlockInfo.Size, seg.Region, seg.BucketName, 
			seg.SegBlockInfo.SegmentId0, seg.SegBlockInfo.SegmentId1, types.NotDeleted)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to update segment size, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}
		helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update the segment size, size: %v", seg.SegBlockInfo.Size))
	}

	helper.Logger.Info(ctx, "Succeed to update segment size")
	return
}

func(t *TidbClient) GetTheSlowestGrowingSeg(ctx context.Context, segReq *types.GetSegmentReq, 
	segIds []*types.IncompleteUploadSegInfo) (isExisted bool, resp *types.GetTheSlowestGrowingSeg, err error) {
	resp = &types.GetTheSlowestGrowingSeg{}
	sqltext := "select capacity, backend_size, size from segment_info where region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
	var stmt *sql.Stmt
	stmt, err = t.Client.Prepare(sqltext)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to prepare get the segment info, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to close get the segment info stmt, err: %v", err))
			err = ErrYIgFsInternalErr
		}
	}()

	var capacity, size, backendSize int
	var maxRemainingCapacity, slowestGrowingSegCapacity int 
	var slowestGrowingSegIndex int = -1
	for i, seg := range segIds {
		row := stmt.QueryRow(segReq.Region, segReq.BucketName, seg.SegmentId0, seg.SegmentId1, types.NotDeleted)
		err = row.Scan (
			&capacity,
			&backendSize,
			&size,
		)
		if err == sql.ErrNoRows {
			continue
		} else if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the segment info, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		if i == 0 {
			maxRemainingCapacity = capacity - size
			slowestGrowingSegCapacity = capacity
			slowestGrowingSegIndex = 0
		} else {
			remainingCapacity := capacity - size
			if remainingCapacity > maxRemainingCapacity {
				maxRemainingCapacity = remainingCapacity
				slowestGrowingSegCapacity = capacity
				slowestGrowingSegIndex = i
			}
		}
	}

	if slowestGrowingSegIndex == -1 {
		return
	} else {
		isExisted = true
		resp = &types.GetTheSlowestGrowingSeg {
			SegmentId0: segIds[slowestGrowingSegIndex].SegmentId0,
			SegmentId1: segIds[slowestGrowingSegIndex].SegmentId1,
			Capacity: slowestGrowingSegCapacity,
			BackendSize: backendSize,
			Size: size,
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get slowest growing seg, seg_id0: %v, seg_id1: %v", resp.SegmentId0, resp.SegmentId1))
	return
}


