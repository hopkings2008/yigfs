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


func (t *TidbClient) GetFileSegmentInfo(ctx context.Context, seg *types.GetSegmentReq) (resp *types.GetSegmentResp, err error) {
	var segmentId int64
	var blockId int64
	var segmentMap = make(map[int64][]int64)
	block := types.BlockInfo{}

	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}

	args := make([]interface{}, 0)
	sqltext := "select seg_id, block_id from block where region=? and bucket_name=? and ino=? and generation=? order by offset;"
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
			&segmentId,
			&blockId)
		if err != nil {
			log.Printf("Failed to get segment info in row, err: %v", err)
			err = ErrYIgFsInternalErr
			return
		}

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

		segment.SegmentId = segmentId
		for _, blockId := range blockIds {
			// get block info
			sqltext := "select size, offset, seg_start_addr, seg_end_addr from block where region=? and bucket_name=? and ino=? and generation=? and seg_id=? and block_id=?;"
			row := t.Client.QueryRow(sqltext, seg.Region, seg.BucketName, seg.Ino, seg.Generation, segmentId, blockId)
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

			log.Printf("Succeed to get segment info, sqltext: %v", sqltext)
			segment.Blocks = append(segment.Blocks, block)
		}
		resp.Segments = append(resp.Segments, segment)
	}
	
	return
}

func (t *TidbClient) CreateFileSegment(ctx context.Context, seg *types.CreateSegmentReq) (err error) {
	now := time.Now().UTC()
	node, err := snowflake.NewNode(seg.Segment.SegmentId%10)
	if err != nil {
		log.Printf("Failed to create blockId, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}
	blockId := node.Generate()

	sqltext := "insert into block values(?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update size=values(size), offset=values(offset), " +
		"seg_start_addr=values(seg_start_addr), seg_end_addr=values(seg_end_addr), mtime=values(mtime)"
	args := []interface{}{seg.Region, seg.BucketName, seg.Ino, seg.Generation, seg.Segment.SegmentId, blockId, seg.Segment.Block.Size,
		seg.Segment.Block.Offset, seg.Segment.Block.SegStartAddr, seg.Segment.Block.SegEndAddr, now, now, types.NotDeleted}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		log.Printf("Failed to create segment to tidb, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("Succeed to create segment to tidb, sqltext: %v", sqltext)
	return
}
