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


func GetSegExistedSql() (sqltext string) {
	sqltext = "select 1 from file_blocks where region=? and bucket_name=? and ino=? and generation=? and is_deleted=? limit 1;"
	return sqltext
}

func(t *TidbClient) IsFileHasSegments(ctx context.Context, seg *types.GetSegmentReq) (isExisted bool, err error) {
	sqltext := GetSegExistedSql()
	var f int
	row := t.Client.QueryRow(sqltext, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted)
	err = row.Scan(
		&f,
	)
	if err == sql.ErrNoRows {
		isExisted = false
		helper.Logger.Info(ctx, fmt.Sprintf("The file does not have segments, region: %v, bucket: %v, ino: %v, generation: %v", 
			seg.Region, seg.BucketName, seg.Ino, seg.Generation))
		return isExisted, nil
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to check whether the file has segments or not, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	isExisted = true
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to check the file has segments, region: %v, bucket: %v, ino: %v, generation: %v", 
		seg.Region, seg.BucketName, seg.Ino, seg.Generation))
	return
}

func (t *TidbClient) GetIncludeOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, 
	checkOffset int64) (getSegs map[interface{}][]*types.BlockInfo, err error) {
	var segmentId0, segmentId1 uint64
	var offset int64
	var segStartAddr, size int
	getSegs = make(map[interface{}][]*types.BlockInfo)

	args := make([]interface{}, 0)
	sqltext := "select seg_id0, seg_id1, size, offset, seg_start_addr from file_blocks where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=? and offset <= ? and size + offset > ? order by offset;"
	args = append(args, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted, checkOffset, checkOffset)

	rows, err := t.Client.Query(sqltext, args...)
	if err == sql.ErrNoRows {
		err = ErrYigFsNoTargetSegment
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetIncludeOffsetIndexInfo, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(
			&segmentId0,
			&segmentId1,
			&size,
			&offset,
			&segStartAddr,)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetIncludeOffsetIndexInfo in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		segmentId := [2]uint64{segmentId0, segmentId1}
		block := types.BlockInfo {
			Size: size,
			Offset: offset,
			SegStartAddr: segStartAddr,
		}
		getSegs[segmentId] = append(getSegs[segmentId], &block)
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetIncludeOffsetIndexInfo in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("GetIncludeOffsetIndexInfo: getSegments length is %v", len(getSegs)))
	return
}

func (t *TidbClient) GetGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (getSegs map[interface{}][]*types.BlockInfo, err error) {
	var segmentId0, segmentId1 uint64
	var offset int64
	var segStartAddr, size int
	getSegs = make(map[interface{}][]*types.BlockInfo)

	args := make([]interface{}, 0)
	var sqltext string

	if checkOffset > 0 {
		sqltext = "select seg_id0, seg_id1, size, offset, seg_start_addr from file_blocks where region=? and bucket_name=? and ino=?" + 
			" and generation=? and is_deleted=? and offset > ? order by offset;"
		args = append(args, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted, checkOffset)
	} else {
		sqltext = "select seg_id0, seg_id1, size, offset, seg_start_addr from file_blocks where region=? and bucket_name=? and ino=?" + 
			" and generation=? and is_deleted=? order by offset;"
		args = append(args, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted)
	}

	rows, err := t.Client.Query(sqltext, args...)
	if err == sql.ErrNoRows {
		err = ErrYigFsNoTargetSegment
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetGreaterOffsetIndexInfo, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(
			&segmentId0,
			&segmentId1,
			&size,
			&offset,
			&segStartAddr,)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetGreaterOffsetIndexInfo in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		segmentId := [2]uint64{segmentId0, segmentId1}
		block := types.BlockInfo {
			Size: size,
			Offset: offset,
			SegStartAddr: segStartAddr,
		}
		getSegs[segmentId] = append(getSegs[segmentId], &block)
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetGreaterOffsetIndexInfo in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("GetGreaterOffsetIndexInfo: getSegments length is %v", len(getSegs)))
	return
}

func (t *TidbClient) GetBlocksBySegId(ctx context.Context, seg *types.GetTheSlowestGrowingSeg) (resp *types.GetSegmentResp, err error) {
	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}

	sqltext := "select size, offset, seg_start_addr from file_blocks where seg_id0=? and seg_id1=? and is_deleted=? order by offset;"
	rows, err := t.Client.Query(sqltext, seg.SegmentId0, seg.SegmentId1, types.NotDeleted)
	if err == sql.ErrNoRows {
		err = ErrYigFsNoTargetSegment
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetBlocksBySegId, segId0: %v, segId1: %v, err: %v", seg.SegmentId0, seg.SegmentId1, err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()

	var offset int64
	var size, segStartAddr int
	segment := &types.SegmentInfo {
		SegmentId0: seg.SegmentId0,
		SegmentId1: seg.SegmentId1,
		Leader: seg.Leader,
		Capacity: seg.Capacity,
		BackendSize: seg.BackendSize,
		Size: seg.Size,
		Blocks: []*types.BlockInfo{},
	}

	for rows.Next() {
		err = rows.Scan(
			&size,
			&offset,
			&segStartAddr)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to scan query blocks getting by segId, segId0: %v, segId1: %v, err: %v", 
				seg.SegmentId0, seg.SegmentId1, err))
			err = ErrYIgFsInternalErr
			return
		}

		block := types.BlockInfo {
			Size: size,
			Offset: offset,
			SegStartAddr: segStartAddr,
		}

		segment.Blocks = append(segment.Blocks, &block)
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to iterator rows for blocks getting by segId, segId0: %v, segId1: %v, err: %v", 
			seg.SegmentId0, seg.SegmentId1, err))
		err = ErrYIgFsInternalErr
		return
	}

	resp.Segments = append(resp.Segments, segment)
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to GetBlocksBySegId, segId0: %v, segId1: %v", seg.SegmentId0, seg.SegmentId1))
	return
}

func(t *TidbClient) GetAllExistedFileSegs(ctx context.Context, file *types.DeleteFileReq) (segs map[interface{}]struct{}, err error) {
	start := time.Now().UTC().UnixNano()
	segs = make(map[interface{}]struct{})
	sqltext := "select seg_id0, seg_id1 from file_blocks where region=? and bucket_name=? and ino=? and generation=? and is_deleted=?;"
	rows, err := t.Client.Query(sqltext, file.Region, file.BucketName, file.Ino, file.Generation, types.NotDeleted)
	if err == sql.ErrNoRows {
		err = ErrYigFsNoVaildSegments
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get segs for the file, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()
	
	var segId0, segId1 uint64
	for rows.Next() {
		err = rows.Scan(
			&segId0,
			&segId1,
		)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to scan query file segs, region: %v, bucket: %v, ino: %v, generation: %v, err: %v", 
				file.Region, file.BucketName, file.Ino, file.Generation, err))
			err = ErrYIgFsInternalErr
			return
		}

		segmentId := [2]uint64{segId0, segId1}
		segs[segmentId] = struct{}{}
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to check the segment info, seg_id0: %v, seg_id1: %v, err: %v", segId0, segId1, err))
		err = ErrYIgFsInternalErr
		return
	}

	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get file segs info, region: %v, bucket: %v, ino: %v, generation: %v, cost: %v", 
		file.Region, file.BucketName, file.Ino, file.Generation, end-start))
	return
}

func(t *TidbClient) DeleteFileBlocks(ctx context.Context, file *types.DeleteFileReq) (err error) {
	start := time.Now().UTC().UnixNano()
	sqltext := "update file_blocks set is_deleted=? where region=? and bucket_name=? and ino=? and generation=? and is_deleted=?;"
	_, err = t.Client.Exec(sqltext, types.Deleted, file.Region, file.BucketName, file.Ino, file.Generation, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete file blocks, region: %v, bucket: %v, ino: %v, generation: %v, err: %v", 
			file.Region, file.BucketName, file.Ino, file.Generation, err))
		err = ErrYIgFsInternalErr
		return
	}

	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to delete file blocks, region: %v, bucket: %v, ino: %v, generation: %v, cost: %v", 
		file.Region, file.BucketName, file.Ino, file.Generation, end-start))
	return
}

func getInsertOrUpdateFileBlocksSql(ctx context.Context, maxNum int) (sqltext string) {
	for i := 0; i < maxNum; i ++ {
		if i == 0 {
			sqltext = "insert into file_blocks(region, bucket_name, ino, generation, seg_id0, seg_id1, size, offset, seg_start_addr, is_deleted)" + 
				" values(?,?,?,?,?,?,?,?,?,?)"
		} else {
			sqltext += ",(?,?,?,?,?,?,?,?,?,?)"
		}
	}
	sqltext += " on duplicate key update seg_id0=values(seg_id0), seg_id1=values(seg_id1), size=values(size), offset=values(offset)," +
		" seg_start_addr=values(seg_start_addr), is_deleted=values(is_deleted);"
	return
}

func getInsertFileAndSegBlocksArgs(ctx context.Context, segInfo *types.DescriptBlockInfo, segs []*types.CreateBlocksInfo) (fileArgs []interface{}, segArgs []interface{}) {
	for _, seg := range segs {
		for _, block := range seg.Blocks {
			fileArgs = append(fileArgs, segInfo.Region, segInfo.BucketName, segInfo.Ino, segInfo.Generation, seg.SegmentId0, seg.SegmentId1,
				block.Size, block.Offset, block.SegStartAddr, types.NotDeleted)
			segArgs = append(segArgs, seg.SegmentId0, seg.SegmentId1, block.SegStartAddr, block.Size, types.NotDeleted)
		}
	}
	return
}

func getInsertSegZoneAndInfoArgs(ctx context.Context, segInfo *types.DescriptBlockInfo, segs []*types.CreateBlocksInfo) (zoneArgs []interface{}, infoArgs []interface{}) {
	for _, seg := range segs {
		zoneArgs = append(zoneArgs, segInfo.ZoneId, segInfo.Region, segInfo.BucketName, seg.SegmentId0, seg.SegmentId1, seg.Leader, types.NotDeleted)
		infoArgs = append(infoArgs, segInfo.Region, segInfo.BucketName, seg.SegmentId0, seg.SegmentId1, seg.Capacity, seg.MaxSize, types.NotDeleted)
	}
	return
}

func (t *TidbClient) InsertOrUpdateFileAndSegBlocks(ctx context.Context, segInfo *types.DescriptBlockInfo, segs []*types.CreateBlocksInfo, 
	isUpdateInfo bool, blocksNum int) (err error) {
	start := time.Now().UTC().UnixNano()
	var tx interface{}
	var sqlTx *sql.Tx
	tx, err = t.Client.Begin()
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		} else {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to InsertOrUpdateFileAndSegBlocks, err: %v", err))
			sqlTx.Rollback()
		}
	}()

	sqlTx, _ = tx.(*sql.Tx)

	insertSegBlocksSql := getInsertOrUpdateSegBlocksSql(ctx, blocksNum)
	insertFileBlocksArgs, insertSegBlocksArgs := getInsertFileAndSegBlocksArgs(ctx, segInfo, segs)
	_, err = sqlTx.Exec(insertSegBlocksSql, insertSegBlocksArgs...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to insert or update seg blocks, region: %v, bucket: %v, ino: %v, generation: %v, err: %v",
			segInfo.Region, segInfo.BucketName, segInfo.Ino, segInfo.Generation, err))
		err = ErrYIgFsInternalErr
		return
	}
	end1 := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("insert seg blocks cost: %v, segsNum: %v", end1 - start, blocksNum))

	insertFileBlocksSql := getInsertOrUpdateFileBlocksSql(ctx, blocksNum)
	_, err = sqlTx.Exec(insertFileBlocksSql, insertFileBlocksArgs...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to insert or update file blocks, region: %v, bucket: %v, ino: %v, generation: %v, err: %v",
			segInfo.Region, segInfo.BucketName, segInfo.Ino, segInfo.Generation, err))
		err = ErrYIgFsInternalErr
		return
	}

	end2 := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("insert file blocks cost: %v, segsNum: %v", end2 - end1, blocksNum))

	if isUpdateInfo {
		start1 := time.Now().UTC().UnixNano()
		segsNum := len(segs)
		insertSegZoneSql := getInsertOrUpdateSegZoneSql(ctx, segsNum)
		insertSegZoneArgs, insertSegInfoArgs := getInsertSegZoneAndInfoArgs(ctx, segInfo, segs)
		_, err = sqlTx.Exec(insertSegZoneSql, insertSegZoneArgs...)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to insert or update seg zone, region: %v, bucket: %v, ino: %v, generation: %v, err: %v",
				segInfo.Region, segInfo.BucketName, segInfo.Ino, segInfo.Generation, err))
			err = ErrYIgFsInternalErr
			return
		}

		end3 := time.Now().UTC().UnixNano()
		helper.Logger.Info(ctx, fmt.Sprintf("insert seg zone, cost: %v", end3 - start1))
	
		insertSegInfoSql := getInsertOrUpdateSegInfoSql(ctx, segsNum)
		_, err = sqlTx.Exec(insertSegInfoSql, insertSegInfoArgs...)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to insert or update seg info, region: %v, bucket: %v, ino: %v, generation: %v, err: %v",
				segInfo.Region, segInfo.BucketName, segInfo.Ino, segInfo.Generation, err))
			err = ErrYIgFsInternalErr
			return
		}
		end4 := time.Now().UTC().UnixNano()
		helper.Logger.Info(ctx, fmt.Sprintf("insert seg info cost: %v", end4 - end3))
	}

	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to insert or update file blocks, region: %v, bucket: %v, ino: %v, generation: %v, cost: %v, segsNum: %v",
		segInfo.Region, segInfo.BucketName, segInfo.Ino, segInfo.Generation, end - start, len(segs)))
	return
}