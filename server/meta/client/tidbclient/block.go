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

func GetBlocksInfoSql() (sqltext string) {
	sqltext = "select size, offset, block_id from block where region=? and bucket_name=?" +
		" and ino=? and generation=? and seg_id0=? and seg_id1=? and is_deleted=?;"
	return sqltext
}

func DeleteBlockSql() (sqltext string) {
	sqltext = "update block set is_deleted=? where region=? and bucket_name=? and ino=?" +
		" and generation=? and seg_id0=? and seg_id1=? and block_id=?;"
	return sqltext
}

func GetBlocksSizeSql() (sqltext string) {
	sqltext = "select size from block where region=? and bucket_name=? and ino=? and generation=? and is_deleted=?"
	return sqltext
}

func GetBlocks(seg *types.CreateSegmentReq, t *TidbClient) (blockMap map[int64][]int64, err error){
	blockMap = make(map[int64][]int64)

	sqltext := GetBlocksInfoSql()
	rows, err := t.Client.Query(sqltext, seg.Region, seg.BucketName, seg.Ino, seg.Generation, 
		seg.Segment.SegmentId0, seg.Segment.SegmentId1, types.NotDeleted)
	if err != nil && err != sql.ErrNoRows {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to get blocks, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()
	
	var size int
	var offset int64
	var block_id int64

	for rows.Next() {
		err = rows.Scan (
			&size,
			&offset,
			&block_id,
		)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("Failed to get block in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		blockMap[block_id] = append(blockMap[block_id], int64(offset))
		blockMap[block_id] = append(blockMap[block_id], int64(size))
	}
	
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to get blocks in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	return blockMap, nil
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
		
		// get segment leader
		sqltext = GetSegmentLeaderSql()
		row := t.Client.QueryRow(sqltext, seg.ZoneId, seg.Region, seg.BucketName, segment.SegmentId0, segment.SegmentId1)
		err = row.Scan (
			&segment.Leader,
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

func (t *TidbClient) CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq) (err error) {
	now := time.Now().UTC()
	
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
	
	// get existed blocks
	blockMap, err := GetBlocks(seg, t)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to get the segment blocks, err: %v", err))
		return ErrYIgFsInternalErr
	}

	// deleted covered existed blocks
	var lastInsertOffset int64 = 0
	var lastInsertSize int64 = 0
	var lastInsertBlockId int64 = 0

	for i, block := range seg.Segment.Blocks {
		for blockId, blockInfo := range blockMap {
			
			if (block.Offset <= blockInfo[0] && block.Offset + int64(block.Size) - int64(1) >= blockInfo[0] + int64(block.Size) - int64(1)) {
				// if covered existed block, delete existed block.
				sqltext = DeleteBlockSql()
				_, err = sqlTx.Exec(sqltext, types.Deleted, seg.Region, seg.BucketName, seg.Ino, seg.Generation, 
					seg.Segment.SegmentId0, seg.Segment.SegmentId1, blockId)
				if err != nil {
					helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to delete segment to tidb, err: %v", err))
					return ErrYIgFsInternalErr
				}
	
				helper.Logger.Info(ctx, fmt.Sprintf("Deleted covered block, seg_id0: %d, seg_id1: %d, block_id: %d", 
					seg.Segment.SegmentId0, seg.Segment.SegmentId1, blockId))

				// deleted map keys
				delete(blockMap, blockId)
			} 
		}

		// Determine if the last uploaded block has been overwritten.
		deleteLastBlock := false
		if i != 0 {
			if (block.Offset <= lastInsertOffset && block.Offset + int64(block.Size) - int64(1) >= lastInsertOffset + lastInsertSize - int64(1)) {
				deleteLastBlock = true
			}
		}

		// if deleteLastBlock is true, deleted last insert block.
		if deleteLastBlock {
			sqltext = DeleteBlockSql()
			_, err = sqlTx.Exec(sqltext, types.Deleted, seg.Region, seg.BucketName, seg.Ino, seg.Generation, 
				seg.Segment.SegmentId0, seg.Segment.SegmentId1, lastInsertBlockId)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to delete lastInsertBlockId to tidb, err: %v", err))
				return ErrYIgFsInternalErr
			}

			helper.Logger.Info(ctx, fmt.Sprintf("Deleted last insert block, seg_id0: %d, seg_id1: %d, block_id: %d", 
				seg.Segment.SegmentId0, seg.Segment.SegmentId1, lastInsertBlockId))
		}

		// upload block
		node, err := snowflake.NewNode(int64(i%10))
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to create blockId, err: %v", err))
			return ErrYIgFsInternalErr
		}
		blockId := node.Generate()

		_, err = stmt.Exec(seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1, 
			blockId, block.Size, block.Offset, block.SegStartAddr, block.SegEndAddr, now, now, types.NotDeleted)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to create segment to tidb, err: %v", err))
			return ErrYIgFsInternalErr
		}

		lastInsertOffset = block.Offset
		lastInsertSize = int64(block.Size)
		lastInsertBlockId = int64(blockId)
	}

	// get the target file's all blocks
	sqltext = GetBlocksSizeSql()
	rows, err := sqlTx.Query(sqltext, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted)
	if err != nil && err != sql.ErrNoRows {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get blocks size, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()
	
	var size int
	var allFileSize uint64 = 0
	var allBlocksNumber uint32 = 0

	for rows.Next() {
		err = rows.Scan (
			&size,
		)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get block size in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		allFileSize += uint64(size)
		allBlocksNumber ++
	}
	
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get blocks size in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}


	// update file size and blocks
	sqltext = UpdateFileSizeAndBlocksSql()
	_, err = sqlTx.Exec(sqltext, allFileSize, now, allBlocksNumber, seg.Region, seg.BucketName, seg.Ino, seg.Generation)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to update the file size and blocks number, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	// if segment leader not exist, create it.
	var leader string
	sqltext = GetSegmentLeaderSql()
	row := sqlTx.QueryRow(sqltext, seg.ZoneId, seg.Region, seg.BucketName, seg.Segment.SegmentId0, seg.Segment.SegmentId1)
	err = row.Scan (
		&leader,
	)

	if err == sql.ErrNoRows {
		sqltext = CreateSegmentLeaderSql()
		_, err = sqlTx.Exec(sqltext, seg.ZoneId, seg.Region, seg.BucketName, seg.Segment.SegmentId0,
			seg.Segment.SegmentId1, seg.Machine, now, now, types.NotDeleted)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to create segment leader, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to get the segment leader, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, "Succeed to create segment to tidb")
	return
}
