package tidbclient

import (
	"context"
	"database/sql"
	"time"
	"log"

	"github.com/bwmarrin/snowflake"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
)


func GetBlockInfoSql() (sqltext string) {
	sqltext = "select size, offset, seg_start_addr, seg_end_addr from block where region=? and bucket_name=?" + 
		" and ino=? and generation=? and seg_id0=? and seg_id1=? and block_id=?;"
	return sqltext
}

func (t *TidbClient) GetFileSegmentInfo(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error) {
	var segmentId0, segmentId1 int64
	var blockId int64
	var segmentMap = make(map[interface{}][]int64)
	block := types.BlockInfo{}
	var stmt *sql.Stmt

	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}

	args := make([]interface{}, 0)
	sqltext := "select seg_id0, seg_id1, block_id from block where region=? and bucket_name=? and ino=? and generation=? order by offset;"
	args = append(args, seg.Region, seg.BucketName, seg.Ino, seg.Generation)

	rows, err := t.Client.Query(sqltext, args...)
	if err == sql.ErrNoRows {
		err = ErrYigFsNoTargetSegment
		return
	} else if err != nil {
		log.Printf("Failed to get segment info, err: %v", err)
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
			log.Printf("Failed to get segment info in row, err: %v", err)
			err = ErrYIgFsInternalErr
			return
		}

		segmentId := [2]int64{segmentId0, segmentId1}
		segmentMap[segmentId] = append(segmentMap[segmentId], blockId)
	}
	err = rows.Err()
	if err != nil {
		log.Printf("Failed to get segment info in rows, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("segmentMap is %v", segmentMap)

	for segmentId, blockIds := range segmentMap {
		segment := &types.SegmentInfo {
			Blocks: []types.BlockInfo{},
		}

		segmentIds := segmentId.([2]int64)
		segment.SegmentId0 = segmentIds[0]
		segment.SegmentId1 = segmentIds[1]

		// get block info
		sqltext = GetBlockInfoSql()
		stmt, err = t.Client.Prepare(sqltext)
		if err != nil {
			log.Printf("Failed to prepare get block info, err: %v", err)
			err = ErrYIgFsInternalErr
			return
		}

		defer func() {
			closeErr := stmt.Close()
			if closeErr != nil {
				log.Printf("Failed to close get block info stmt, err: %v", err)
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
				log.Printf("Failed to get the block info, err: %v", err)
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
			log.Printf("GetFileSegmentInfo: Failed to get the segment leader, err: %v", err)
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
		log.Printf("Failed to prepare insert block, err: %v", err)
			err = ErrYIgFsInternalErr
			return
	}

	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			log.Printf("Failed to close insert block stmt, err: %v", err)
			err = ErrYIgFsInternalErr
		}
	}()

	for i, block := range seg.Segment.Blocks{
		node, err := snowflake.NewNode(int64(i%10))
		if err != nil {
			log.Printf("Failed to create blockId, err: %v", err)
			return ErrYIgFsInternalErr
		}
		blockId := node.Generate()

		_, err = stmt.Exec(seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId0, seg.Segment.SegmentId1, 
			blockId, block.Size, block.Offset, block.SegStartAddr, block.SegEndAddr, now, now, types.NotDeleted)
		if err != nil {
			log.Printf("Failed to create segment to tidb, err: %v", err)
			return ErrYIgFsInternalErr
		}
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
			log.Printf("CreateFileSegment: Failed to create segment leader, err: %v", err)
			err = ErrYIgFsInternalErr
			return
		}
	} else if err != nil {
		log.Printf("CreateFileSegment: Failed to get the segment leader, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("Succeed to create segment to tidb")
	return
}
