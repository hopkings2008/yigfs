package tidbclient

import (
	"context"
	"database/sql"
	//"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/bwmarrin/snowflake"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/types"
)

func GetBlockInfoSql() (sqltext string) {
	sqltext = "select seg_start_addr, seg_end_addr, size from segment_blocks where seg_id0=? and seg_id1=? and block_id=?;"
	return sqltext
}

func(t *TidbClient) InsertSegmentBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, 
	block *types.BlockInfo) (blockId int64, isCanMerge bool, err error) {
	isCanMerge = false
	sqltext := "insert into segment_blocks values(?,?,?,?,?,?,?,?,?)"

	node, err := snowflake.NewNode(rand.Int63n(10))
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create blockId, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	block_id := node.Generate()

	now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)
	_, err = t.Client.Exec(sqltext, blockInfo.SegmentId0, blockInfo.SegmentId1, block_id, block.SegStartAddr, block.SegEndAddr, block.Size, now, now, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create the segment block, blockId: %d, err: %v", block_id, err))
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create the segment block, block_id: %d", block_id))

	// check whether it can be merge or not.
	sqltext = "select size from segment_blocks where seg_id0=? and seg_id1=? and seg_end_addr=?"
	var size int

	row := t.Client.QueryRow(sqltext, blockInfo.SegmentId0, blockInfo.SegmentId1, block.SegStartAddr)
	err = row.Scan(
		&size,)
	
	if err == sql.ErrNoRows {
		err = nil
	} else if err != nil && err != sql.ErrNoRows{
		helper.Logger.Info(ctx, fmt.Sprintf("Failed to get the merge block, block_id: %d, seg_end_addr: %v", block.BlockId, block.SegStartAddr))
		err = ErrYIgFsInternalErr
		return
	} else {
		isCanMerge = true
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to check segment block whether can be merge, isCanMerge: %v", isCanMerge))
	return int64(block_id), isCanMerge, nil
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