package tidbclient

import (
	"context"
	"database/sql"
	//"database/sql"
	"fmt"
	"math/rand"
	"sync"

	"github.com/bwmarrin/snowflake"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/types"
)


var (
	waitgroup sync.WaitGroup
)

func GetBlockInfoSql() (sqltext string) {
	sqltext = "select seg_start_addr, seg_end_addr, size from segment_blocks where seg_id0=? and seg_id1=? and block_id=?;"
	return sqltext
}

func GetBlocksBySegId() (sqltext string) {
	sqltext = "select seg_start_addr from segment_blocks where seg_id0=? and seg_id1=? and is_deleted=?;"
	return sqltext
}

func(t *TidbClient) InsertSegmentBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (blockId int64, err error) {
	sqltext := "insert into segment_blocks(seg_id0, seg_id1, block_id, seg_start_addr, seg_end_addr, size) values(?,?,?,?,?,?)"
	node, err := snowflake.NewNode(rand.Int63n(10))
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create blockId, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	newBlockId := node.Generate()

	_, err = t.Client.Exec(sqltext, blockInfo.SegmentId0, blockInfo.SegmentId1, newBlockId, block.SegStartAddr, block.SegEndAddr, block.Size)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create the segment block, blockId: %d, err: %v", newBlockId, err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create the segment block, block_id: %d", newBlockId))
	return int64(newBlockId), nil
}

func(t *TidbClient) MergeSegmentBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (err error) {
	sqltext := "update segment_blocks set seg_end_addr=?, size=? where seg_id0=? and seg_id1=? and block_id=? and is_deleted=?"
	_, err = t.Client.Exec(sqltext, block.SegEndAddr, block.Size, blockInfo.SegmentId0, blockInfo.SegmentId1, block.BlockId, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to merge the segment block, blockId: %d, err: %v", block.BlockId, err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to merge the segment block, block_id: %d", block.BlockId))
	return
}

func(t *TidbClient) IsBlockCanMerge(ctx context.Context, blockInfo *types.DescriptBlockInfo, 
	block *types.BlockInfo) (isCanMerge bool, resp *types.BlockInfo, err error) {
	resp = &types.BlockInfo{}
	isCanMerge = false
	sqltext := "select block_id, size from segment_blocks where seg_id0=? and seg_id1=? and seg_end_addr=? and is_deleted=?"

	row := t.Client.QueryRow(sqltext, blockInfo.SegmentId0, blockInfo.SegmentId1, block.SegStartAddr, types.NotDeleted)
	err = row.Scan(
		&resp.BlockId,
		&resp.Size,
	)
	
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		helper.Logger.Info(ctx, fmt.Sprintf("Failed to check whether the segment block can be merge or not, block_id: %d, seg_end_addr: %v", block.BlockId, block.SegStartAddr))
		err = ErrYIgFsInternalErr
		return
	} else {
		isCanMerge = true
		resp.SegEndAddr = block.SegStartAddr
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to check whether the segment block can be merge or not, isCanMerge: %v", isCanMerge))
	return
}

func (t *TidbClient) GetSegsBlockInfo(ctx context.Context, seg *types.GetSegmentReq, segmentMap map[interface{}][]int64, 
	offsetMap map[int64]int64) (resp *types.GetSegmentResp, err error) {
	var stmt *sql.Stmt
	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}

	for segmentId, blockIds := range segmentMap {
		segment := &types.SegmentInfo {
			Blocks: []*types.BlockInfo{},
		}
		segmentIds := segmentId.([2]uint64)
		segment.SegmentId0 = segmentIds[0]
		segment.SegmentId1 = segmentIds[1]

		waitgroup.Add(1)
		go func() {
			defer waitgroup.Done()
			// get segment leader
			sqltext := GetSegmentLeaderSql()
			row := t.Client.QueryRow(sqltext, seg.ZoneId, seg.Region, seg.BucketName, segment.SegmentId0, segment.SegmentId1)
			err = row.Scan (
				&segment.Leader,
			)
			if err == sql.ErrNoRows {
				segment.Leader = seg.Machine
			} else if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("GetFileSegmentInfo: Failed to get the segment leader, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			}

			// get segment info
			sqltext = GetSegmentInfoSql()
			row = t.Client.QueryRow(sqltext, seg.Region, seg.BucketName, segment.SegmentId0, segment.SegmentId1)
			err = row.Scan (
				&segment.Capacity,
				&segment.BackendSize,
				&segment.Size,
			)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("GetFileSegmentInfo: Failed to get the segment capacity, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			}
		}()

		// get block info
		sqltext := GetBlockInfoSql()
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
			row := stmt.QueryRow(segment.SegmentId0, segment.SegmentId1, blockId)
			block := &types.BlockInfo{}
			err = row.Scan(
				&block.SegStartAddr,
				&block.SegEndAddr,
				&block.Size)

			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the block info, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			}

			block.Offset = offsetMap[blockId]
			segment.Blocks = append(segment.Blocks, block)
		}

		waitgroup.Wait()
		resp.Segments = append(resp.Segments, segment)
	}
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get segments blocks info, number: %v", len(resp.Segments)))
	return
}

func(t *TidbClient) DeleteBlocksBySegsId(ctx context.Context, segs map[interface{}][]int64) (err error) {
	var stmt *sql.Stmt
	sqltext := "update segment_blocks set is_deleted=? where seg_id0=? and seg_id1=? and seg_start_addr=? and is_deleted=?"
	stmt, err = t.Client.Prepare(sqltext)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to prepare delete segment blocks, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to close delete segment blocks stmt, err: %v", err))
			err = ErrYIgFsInternalErr
		}
	}()

	for segmentId, _:= range segs {
		segIds := segmentId.([2]uint64)
		sqltext := GetBlocksBySegId()
		rows, err := t.Client.Query(sqltext, segIds[0], segIds[1], types.NotDeleted)
		if err == sql.ErrNoRows {
			return nil
		} else if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get blocks for the seg, err: %v", err))
			return ErrYIgFsInternalErr
		}
		defer rows.Close()

		var startAddr int
		for rows.Next() {
			err = rows.Scan(
				&startAddr,
			)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to scan query segment blocks, segId0: %v, segId1: %v, err: %v", segIds[0], segIds[1], err))
				return ErrYIgFsInternalErr
			}
	
			_, err = stmt.Exec(types.Deleted, segIds[0], segIds[1], startAddr, types.NotDeleted)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete the segment block, segId0: %v, segId1: %v, startAddr: %v, err: %v",
					segIds[0], segIds[1], startAddr, err))
				return ErrYIgFsInternalErr
			}
		}
		err = rows.Err()
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to iterator rows for segment blocks, segId0: %v, segId1: %v, err: %v", segIds[0], segIds[1], err))
			return ErrYIgFsInternalErr
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to deleted segment blocks, segsNum: %v", len(segs)))
	return
}