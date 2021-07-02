package tidbclient

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/types"
)


func CheckSegHasBlocksSql() (sqltext string) {
	sqltext = "select 1 from segment_blocks where seg_id0=? and seg_id1=? and is_deleted=?;"
	return sqltext
}

func deleteSegBlocksSql(blocksNum int) (sqltext string) {
	for i := 0; i < blocksNum; i++ {
		if i == 0 {
			sqltext = "update segment_blocks set is_deleted=? where seg_id0=? and seg_id1=? and seg_start_addr in (?"
		} else {
			sqltext += ",?"
		}
	}
	sqltext += ");"

	return sqltext
}

func getDeleteAllSegBlocksArgs(segId0, segId1 uint64, startAddrs []int) (args []interface{}) {
	args = []interface{}{types.Deleted, segId0, segId1}
	for _, startAddr := range startAddrs {
		args = append(args, startAddr)
	}
	
	return args
}

func (t *TidbClient) GetSegsBlockInfo(ctx context.Context, seg *types.GetSegmentReq, segs map[interface{}][]*types.BlockInfo) (resp *types.GetSegmentResp, err error) {
	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}

	for segmentId, blocksInfo := range segs {
		segment := &types.SegmentInfo {
			Blocks: []*types.BlockInfo{},
		}
		segmentIds := segmentId.([2]uint64)
		segment.SegmentId0 = segmentIds[0]
		segment.SegmentId1 = segmentIds[1]

		// get segment leader
		sqltext := GetSegmentLeaderSql()
		row := t.Client.QueryRow(sqltext, seg.ZoneId, seg.Region, seg.BucketName, segment.SegmentId0, segment.SegmentId1, types.NotDeleted)
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
		row = t.Client.QueryRow(sqltext, seg.Region, seg.BucketName, segment.SegmentId0, segment.SegmentId1, types.NotDeleted)
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

		segment.Blocks = append(segment.Blocks, blocksInfo...)
		resp.Segments = append(resp.Segments, segment)
	}
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get segments blocks info, number: %v", len(resp.Segments)))
	return
}

func(t *TidbClient) DeleteSegBlocks(ctx context.Context, segs map[interface{}][]int) (err error) {
	start := time.Now().UTC().UnixNano()
	var sqltext string
	var num int
	for seg, startAddrs := range segs {
		num = 0
		segId := seg.([2]uint64)
		blocksNum := len(startAddrs)
		args := []interface{}{types.Deleted, segId[0], segId[1]}
		if blocksNum > types.MaxDeleteBlocksNum {
			for _, startAddr := range startAddrs {
				num ++
				args = append(args, startAddr)
				if num == types.MaxDeleteBlocksNum {
					sqltext = deleteSegBlocksSql(types.MaxDeleteBlocksNum)
					_, err = t.Client.Exec(sqltext, args...)
					if err != nil {
						helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete seg blocks, segId0: %v, segId1: %v, err: %v", segId[0], segId[1], err))
						return
					} else {
						num = 0
						args = args[:3]
					}
				}
			}

			remainBlocksNum := len(args) - 3
			if remainBlocksNum > 0 {
				sqltext = deleteSegBlocksSql(remainBlocksNum)
				_, err = t.Client.Exec(sqltext, args...)
				if err != nil {
					helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete remain seg blocks, segId0: %v, segId1: %v, err: %v", segId[0], segId[1], err))
					return
				}
			}
		} else {
			sqltext = deleteSegBlocksSql(blocksNum)
			getArgs := getDeleteAllSegBlocksArgs(segId[0], segId[1], startAddrs)
			_, err = t.Client.Exec(sqltext, getArgs...)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete seg blocks, segId0: %v, segId1: %v, err: %v", segId[0], segId[1], err))
				return
			}
		}
	}
	
	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to delete seg blocks, segsNum: %v, cost: %v", len(segs), end - start))
	return
}

func getInsertOrUpdateSegBlocksSql(ctx context.Context, maxNum int) (sqltext string) {
	for i := 0; i < maxNum; i ++ {
		if i == 0 {
			sqltext = "insert into segment_blocks(seg_id0, seg_id1, seg_start_addr, size, is_deleted) values(?,?,?,?,?)"
		} else {
			sqltext += ",(?,?,?,?,?)"
		}
	}
	sqltext += " on duplicate key update is_deleted=values(is_deleted);"
	return
}

func getDeleteSegBlocksArgs(ctx context.Context, segs []*types.CreateBlocksInfo) (args []interface{}) {
	for _, seg := range segs {
		for _, block := range seg.Blocks {
			args = append(args, seg.SegmentId0, seg.SegmentId1, block.SegStartAddr, block.Size, types.Deleted)
		}
	}
	return
}

func(t *TidbClient) RemoveSegBlocks(ctx context.Context, segs []*types.CreateBlocksInfo, blocksNum int) (err error) {
	start := time.Now().UTC().UnixNano()
	sqltext := getInsertOrUpdateSegBlocksSql(ctx, blocksNum)
	args := getDeleteSegBlocksArgs(ctx, segs)
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Succeed to remove seg blocks for the file, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	
	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to remove seg blocks, blocksNum: %v, cost: %v", blocksNum, end-start))
	return
}