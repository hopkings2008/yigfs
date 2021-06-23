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

func DeleteSegBlocksSql() (sqltext string) {
	sqltext = "update segment_blocks join (select seg_id0 as segId0, seg_id1 as segId1, seg_start_addr as startAddr from file_blocks where region=? and bucket_name=?" + 
		" and ino=? and generation=? and is_deleted=?) b set is_deleted=? where seg_id0=b.segId0 and seg_id1=b.segId1 and seg_start_addr=b.startAddr;"
	return sqltext
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

func(t *TidbClient) DeleteSegBlocks(ctx context.Context, file *types.DeleteFileReq) (err error) {
	start := time.Now().UTC().UnixNano()
	sqltext := DeleteSegBlocksSql()
	_, err = t.Client.Exec(sqltext, file.Region, file.BucketName, file.Ino, file.Generation, types.NotDeleted, types.Deleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete seg blocks for the file, region: %v, bucket: %v, ino: %v, generation: %v, err: %v", 
			file.Region, file.BucketName, file.Ino, file.Generation, err))
		err = ErrYIgFsInternalErr
		return
	}
	
	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to delete seg blocks for the file, region: %v, bucket: %v, ino: %v, generation: %v, cost: %v", 
		file.Region, file.BucketName, file.Ino, file.Generation, end-start))
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
	sqltext += " on duplicate key update size=values(size), is_deleted=values(is_deleted);"
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