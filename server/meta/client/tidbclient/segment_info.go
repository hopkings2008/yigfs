package tidbclient

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
)

func GetSegmentInfoSql() (sqltext string) {
	sqltext = "select capacity, backend_size, size from segment_info where region=? and bucket_name=? and seg_id0=? and seg_id1=?"
	return sqltext
}

func CreateSegmentInfoSql() (sqltext string) {
	sqltext = "insert into segment_info(region, bucket_name, seg_id0, seg_id1, capacity, size) values(?,?,?,?,?,?) on duplicate key update size=values(size);"
	return sqltext
}

func DeleteSegmentInfoSql() (sqltext string) {
	sqltext = "update segment_info set is_deleted=? where region=? and bucket_name=? and seg_id0=? and seg_id1=? and is_deleted=?"
	return sqltext
}

func getInsertOrUpdateSegInfoSql(ctx context.Context, maxNum int) (sqltext string) {
	for i := 0; i < maxNum; i ++ {
		if i == 0 {
			sqltext = "insert into segment_info(region, bucket_name, seg_id0, seg_id1, capacity, size) values(?,?,?,?,?,?)"
		} else {
			sqltext += ",(?,?,?,?,?,?)"
		}
	}
	sqltext += " on duplicate key update size=values(size);"
	return
}

func (t *TidbClient) CreateSegmentInfoAndZoneInfo(ctx context.Context, segment *types.CreateSegmentReq, maxSize int) (err error) {
	sqltext := CreateSegmentZoneSql()
	args := []interface{}{segment.ZoneId, segment.Region, segment.BucketName, segment.Segment.SegmentId0,
		segment.Segment.SegmentId1, segment.Machine, types.NotDeleted}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment zone to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	sqltext = CreateSegmentInfoSql()
	args = []interface{}{segment.Region, segment.BucketName, segment.Segment.SegmentId0, segment.Segment.SegmentId1, segment.Segment.Capacity, maxSize}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment info to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create segment info and zone to tidb, seg_id0: %v, seg_id1: %v", segment.Segment.SegmentId0,
		segment.Segment.SegmentId1))
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
	var maxRemainingCapacity, segCapacity, segSize, segBackendSize int 
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
			segCapacity = capacity
			slowestGrowingSegIndex = 0
			segSize = size
			segBackendSize = backendSize
		} else {
			remainingCapacity := capacity - size
			if remainingCapacity > maxRemainingCapacity {
				maxRemainingCapacity = remainingCapacity
				segCapacity = capacity
				slowestGrowingSegIndex = i
				segSize = size
				segBackendSize = backendSize
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
			Capacity: segCapacity,
			BackendSize: segBackendSize,
			Size: segSize,
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get slowest growing seg, seg_id0: %v, seg_id1: %v", resp.SegmentId0, resp.SegmentId1))
	return
}

func(t *TidbClient) DeleteSegInfo(ctx context.Context, file *types.DeleteFileReq, segs map[interface{}]struct{}) (err error) {
	start := time.Now().UTC().UnixNano()
	var stmt *sql.Stmt
	sqltext := DeleteSegmentInfoSql()
	checkSql := CheckSegHasBlocksSql()
	stmt, err = t.Client.Prepare(sqltext)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to prepare delete segment info, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to close delete segment info stmt, err: %v", err))
			err = ErrYIgFsInternalErr
		}
	}()

	var r int
	for segmentIds, _ := range segs {
		segmentId := segmentIds.([2]uint64)
		row := t.Client.QueryRow(checkSql, segmentId[0], segmentId[1], types.NotDeleted)
		err = row.Scan (
			&r,
		)
		if err == sql.ErrNoRows {
			_, err = stmt.Exec(types.Deleted, file.Region, file.BucketName, segmentId[0], segmentId[1], types.NotDeleted)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete the segment info, seg_id0: %v, seg_id1: %v, err: %v", segmentId[0], segmentId[1], err))
				err = ErrYIgFsInternalErr
				return
			}
		} else if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get incomplete segs by leader, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}
	}

	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to deleted segment blocks, blocksNum: %v, cost: %v", len(segs), end - start))
	return
}