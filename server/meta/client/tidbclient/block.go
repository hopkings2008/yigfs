package tidbclient

import (
	"context"
	"database/sql"
	"time"
	"fmt"

	"github.com/bwmarrin/snowflake"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
)


func GetBlockInfoSql() (sqltext string) {
	sqltext = "select size, offset, seg_start_addr, seg_end_addr from block where region=? and bucket_name=?" + 
		" and ino=? and generation=? and seg_id0=? and seg_id1=? and block_id=?;"
	return sqltext
}

func GetCoveredBlocksInfoSql() (sqltext string) {
	sqltext = "select size, block_id, offset from block where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=? and offset >= ? and offset + size <= ? and block_id < ?;"
	return sqltext
}

func GetCoverBlocksInfoSql() (sqltext string) {
	sqltext = "select size, block_id, offset from block where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=? and offset <= ? and offset + size > ? and block_id < ?;"
	return sqltext
}

func GetTargetBlockSql() (sqltext string) {
	sqltext = "select block_id, size from block where region=? and bucket_name=? and ino=? and generation=?" + 
		" and seg_id0=? and seg_id1=? and is_deleted=? and seg_end_addr = ?;"
	return sqltext
}

func DeleteBlockSql() (sqltext string) {
	sqltext = "update block set is_deleted=? where region=? and bucket_name=? and ino=? and generation=? and offset=? and block_id=?;"
	return sqltext
}

func MergeBlockSql() (sqltext string) {
	sqltext = "update block set seg_end_addr=?, size=?, mtime=? where region=? and bucket_name=? and ino=?" +
	" and generation=? and seg_id0=? and seg_id1=? and block_id=?"
	return sqltext
}

func(t *TidbClient) DeleteBlock(ctx context.Context, seg *types.CreateSegmentReq, blockId int64) (err error) {
	sqltext := DeleteBlockSql()
	_, err = t.Client.Exec(sqltext, types.Deleted, seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.CoveredBlockOffset, blockId)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete block from tidb, blockId: %d, err: %v", blockId, err))
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to delete the block, offset: %d, block_id: %d", seg.CoveredBlockOffset, blockId))
	return
}

func(t *TidbClient) GetCoveredExistedBlocks(ctx context.Context, seg *types.CreateSegmentReq, startAddr, endAddr, tag int64) (blocks map[int64][]int64, err error) {
	blocks = make(map[int64][]int64)

	sqltext := GetCoveredBlocksInfoSql()
	rows, err := t.Client.Query(sqltext, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted, startAddr, endAddr, tag)
	if err != nil && err != sql.ErrNoRows {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get blocks, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()
	
	var size int
	var block_id int64
	var offset int64

	for rows.Next() {
		err = rows.Scan (
			&size,
			&block_id,
			&offset,
		)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get block in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		blocks[block_id] = append(blocks[block_id], offset)
		blocks[block_id] = append(blocks[block_id], int64(size))
	}
	
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get blocks in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	return blocks, nil
}

func(t *TidbClient) GetCoveredUploadingBlocks(ctx context.Context, seg *types.CreateSegmentReq, startAddr, endAddr, tag int64) (blocks map[int64][]int64, err error) {
	blocks = make(map[int64][]int64)

	sqltext := GetCoverBlocksInfoSql()
	rows, err := t.Client.Query(sqltext, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted, startAddr, endAddr, tag)
	if err != nil && err != sql.ErrNoRows {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get contain blocks, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()
	
	var size int
	var block_id int64
	var offset int64

	for rows.Next() {
		err = rows.Scan (
			&size,
			&block_id,
			&offset,
		)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get contain block in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		blocks[block_id] = append(blocks[block_id], offset)
		blocks[block_id] = append(blocks[block_id], int64(size))
	}
	
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get blocks in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	return blocks, nil
}

func (t *TidbClient) GetFileSegmentInfo(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error) {
	var segmentId0, segmentId1 uint64
	var blockId int64
	var segmentMap = make(map[interface{}][]int64)
	block := types.BlockInfo{}
	var stmt *sql.Stmt

	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}

	args := make([]interface{}, 0)
	sqltext := "select seg_id0, seg_id1, block_id from block where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=? order by offset;"
	args = append(args, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted)

	rows, err := t.Client.Query(sqltext, args...)
	if err == sql.ErrNoRows {
		err = ErrYigFsNoTargetSegment
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get segment info, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(
			&segmentId0,
			&segmentId1,
			&blockId)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get segment info in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		segmentId := [2]uint64{segmentId0, segmentId1}
		segmentMap[segmentId] = append(segmentMap[segmentId], blockId)
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get segment info in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("GetFileSegmentInfo: segmentMap is %v", segmentMap))

	for segmentId, blockIds := range segmentMap {
		segment := &types.SegmentInfo {
			Blocks: []types.BlockInfo{},
		}

		segmentIds := segmentId.([2]uint64)
		segment.SegmentId0 = segmentIds[0]
		segment.SegmentId1 = segmentIds[1]

		// get block info
		sqltext = GetBlockInfoSql()
		stmt, err = t.Client.Prepare(sqltext)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to prepare get block info, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		defer func() {
			closeErr := stmt.Close()
			if closeErr != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to close get block info stmt, err: %v", err))
				err = ErrYIgFsInternalErr
			}
		}()

		for _, blockId := range blockIds {
			row := stmt.QueryRow(seg.Region, seg.BucketName, seg.Ino, seg.Generation, segment.SegmentId0, segment.SegmentId1, blockId)
			err = row.Scan(
				&block.Size,
				&block.Offset,
				&block.SegStartAddr,
				&block.SegEndAddr)

			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the block info, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			}

			segment.Blocks = append(segment.Blocks, block)
		}
		
		// get segment leader and max_size
		sqltext = GetSegmentLeaderSql()
		row := t.Client.QueryRow(sqltext, seg.ZoneId, seg.Region, seg.BucketName, segment.SegmentId0, segment.SegmentId1)
		err = row.Scan (
			&segment.Leader,
			&segment.MaxSize,
		)

		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("GetFileSegmentInfo: Failed to get the segment leader, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		resp.Segments = append(resp.Segments, segment)
	}
	
	return
}

func (t *TidbClient) CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq) (mergeNumber int, err error) {
	var blockId int64
	var blockSize int
	
	var tx interface{}
	var sqlTx *sql.Tx
	var stmt *sql.Stmt
	tx, err = t.Client.Begin()
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		} else {
			sqlTx.Rollback()
		}
	}()

	sqlTx, _ = tx.(*sql.Tx)

	sqltext := "insert into block values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	stmt, err = sqlTx.Prepare(sqltext)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to prepare insert block, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to close insert block stmt, err: %v", err))
			err = ErrYIgFsInternalErr
		}
	}()

	now := time.Now().UTC()

	for i, block := range seg.Segment.Blocks {
		// if the block start addr == the existed block end addr, merge it.
		sqltext = GetTargetBlockSql()
		row := sqlTx.QueryRow(sqltext, seg.Region, seg.BucketName, seg.Ino, seg.Generation, 
			seg.Segment.SegmentId0, seg.Segment.SegmentId1, types.NotDeleted, block.SegStartAddr)
		err = row.Scan(
			&blockId,
			&blockSize,
		)

		switch err {
		case sql.ErrNoRows:
			// if not find it, upload it.
			node, createBlockErr := snowflake.NewNode(int64(i%10))
			if createBlockErr != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to create blockId, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			}
			blockId := node.Generate()
	
			_, err = stmt.Exec(seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1, 
				blockId, block.Size, block.Offset, block.SegStartAddr, block.SegEndAddr, now, now, types.NotDeleted)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment to tidb, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			}
			
			helper.Logger.Info(ctx, fmt.Sprintf("Successed to upload block, blockId: %v", blockId))

		case nil:
			// if found it , merge it.
			newEndAddr := block.SegStartAddr + int64(block.Size)
			blockSize += block.Size
			sqltext = MergeBlockSql()
			_, err = sqlTx.Exec(sqltext, newEndAddr, blockSize, now, seg.Region, seg.BucketName, seg.Ino, seg.Generation, 
				seg.Segment.SegmentId0, seg.Segment.SegmentId1, blockId)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to merge segment to tidb, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			}
			mergeNumber ++

			helper.Logger.Info(ctx, fmt.Sprintf("Successed to merge block, blockId: %v", blockId))

		default:
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the file size and blocks number, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}
	}

	return
}