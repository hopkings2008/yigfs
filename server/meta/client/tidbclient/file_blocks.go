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


func GetCoveredBlocksInfoSql() (sqltext string) {
	sqltext = "select offset from file_blocks where region=? and bucket_name=? and ino=? and generation=? and is_deleted=? and offset >= ? and end_addr <= ?;"
	return sqltext
}

func GetTargetFileBlockSql() (sqltext string) {
	sqltext = "select end_addr from file_blocks where region=? and bucket_name=? and ino=? and generation=? and offset=? and is_deleted=?;"
	return sqltext
}

func DeleteBlockSql() (sqltext string) {
	sqltext = "update file_blocks set is_deleted=? where region=? and bucket_name=? and ino=? and generation=? and offset=?;"
	return sqltext
}

func UpdateBlockSql() (sqltext string) {
	sqltext = "update file_blocks set block_id=?, size=?, end_addr=? where region=?" + 
		" and bucket_name=? and ino=? and generation=? and offset=? and is_deleted=?;"
	return sqltext
}

func GetInsertBlockSql() (sqltext string) {
	sqltext = "insert into file_blocks(region, bucket_name, ino, generation, seg_id0, seg_id1, block_id, size, offset, end_addr, ctime)" +
		" values(?,?,?,?,?,?,?,?,?,?,?);"
	return sqltext
}

func DeletePartialCoverBlockSql() (sqltext string) {
	sqltext = "insert into file_blocks values(?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key" +
		" update size=values(size), end_addr=values(end_addr), is_deleted=values(is_deleted);"
	return sqltext
}

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

func(t *TidbClient) GetCoveredExistedBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, 
	block *types.BlockInfo) (blocks []*types.BlockInfo, err error) {
	sqltext := GetCoveredBlocksInfoSql()
	rows, err := t.Client.Query(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, 
		types.NotDeleted, block.Offset, block.FileBlockEndAddr)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get covered existed blocks, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()
	
	var offset int64

	for rows.Next() {
		err = rows.Scan (
			&offset,
		)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get block in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		block := &types.BlockInfo{
			Offset: offset,
		}

		blocks = append(blocks, block)
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to iterator rows for covered existed blocks, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get covered existed blocks, offset: %v, blocksNum: %v", block.Offset, len(blocks)))
	return blocks, nil
}

func(t *TidbClient) GetOffsetInUploadingBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, 
	block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error) {
	blockResp = &types.FileBlockInfo{}
	isExisted = false
	var createTime string

	sqltext := "select seg_id0, seg_id1, block_id, size, offset, end_addr, ctime from file_blocks where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=? and offset > ? and offset < ? and end_addr > ?;"
	row := t.Client.QueryRow(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, 
			types.NotDeleted, block.Offset, block.FileBlockEndAddr, block.FileBlockEndAddr)
	err = row.Scan(
		&blockResp.SegmentId0,
		&blockResp.SegmentId1,
		&blockResp.BlockId,
		&blockResp.Size,
		&blockResp.Offset,
		&blockResp.FileBlockEndAddr,
		&createTime)
	
	if err == sql.ErrNoRows{
		err = nil
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetOffsetInUploadingBlock, offset: %d, err: %v", block.Offset, err))
		err = ErrYIgFsInternalErr
		return
	}

	blockResp.Ctime, err = time.Parse(types.TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		return
	}

	isExisted = true
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to GetOffsetInUploadingBlock, offset: %d, ctime: %v", blockResp.Offset, blockResp.Ctime))
	return
}

func(t *TidbClient) GetOffsetInExistedBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, 
	block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error) {
	blockResp = &types.FileBlockInfo{}
	isExisted = false
	var createTime string

	sqltext := "select seg_id0, seg_id1, block_id, size, offset, end_addr, ctime from file_blocks where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=? and offset < ? and end_addr > ? and end_addr < ?;"
	row := t.Client.QueryRow(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, 
			types.NotDeleted, block.Offset, block.Offset, block.FileBlockEndAddr)
	err = row.Scan(
		&blockResp.SegmentId0,
		&blockResp.SegmentId1,
		&blockResp.BlockId,
		&blockResp.Size,
		&blockResp.Offset,
		&blockResp.FileBlockEndAddr,
		&createTime)
	
	if err == sql.ErrNoRows{
		err = nil
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetOffsetInUploadingBlocks, offset: %d, err: %v", block.Offset, err))
		err = ErrYIgFsInternalErr
		return
	}

	blockResp.Ctime, err = time.Parse(types.TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		return
	}

	isExisted = true
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to GetOffsetInExistedBlock, offset: %d, ctime: %v", blockResp.Offset, blockResp.Ctime))
	return
}

func(t *TidbClient) GetCoveredUploadingBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, 
	block *types.BlockInfo) (isExisted bool, blockResp *types.FileBlockInfo, err error) {
	blockResp = &types.FileBlockInfo{}
	isExisted = false
	var createTime string

	sqltext := "select seg_id0, seg_id1, block_id, size, offset, end_addr, ctime from file_blocks where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=? and offset <= ? and end_addr >= ?;"
	row := t.Client.QueryRow(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, 
			types.NotDeleted, block.Offset, block.FileBlockEndAddr)
	err = row.Scan(
		&blockResp.SegmentId0,
		&blockResp.SegmentId1,
		&blockResp.BlockId,
		&blockResp.Size,
		&blockResp.Offset,
		&blockResp.FileBlockEndAddr,
		&createTime)
	
	if err == sql.ErrNoRows{
		err = nil
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetCoveredUploadingBlock, offset: %d, err: %v", block.Offset, err))
		err = ErrYIgFsInternalErr
		return
	}

	if blockResp.Offset == block.Offset && blockResp.FileBlockEndAddr == block.FileBlockEndAddr {
		return false, &types.FileBlockInfo{}, nil
	}

	blockResp.Ctime, err = time.Parse(types.TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		return
	}

	isExisted = true
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to GetCoveredUploadingBlock, offset: %d, ctime: %v", blockResp.Offset, blockResp.Ctime))
	return
}

func(t *TidbClient) DealOverlappingBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, updateBlocks []*types.FileBlockInfo, 
	deleteBlocks []*types.FileBlockInfo, insertBlocks []*types.FileBlockInfo) (err error) {
	if len(updateBlocks) != 0 {
		sqltext := UpdateBlockSql()
		for _, block := range updateBlocks {
			_, err = t.Client.Exec(sqltext, block.BlockId, block.Size, block.FileBlockEndAddr, blockInfo.Region, blockInfo.BucketName, 
				blockInfo.Ino, blockInfo.Generation, block.Offset, types.NotDeleted)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("DealOverlappingBlocks:Failed to update file block info, seg_id0: %v, seg_id1: %v, offset: %v, err: %v", 
					blockInfo.SegmentId0, blockInfo.SegmentId1, block.Offset, err))
				err = ErrYIgFsInternalErr
				return
			}
			helper.Logger.Info(ctx, fmt.Sprintf("DealOverlappingBlocks:Succeed to update file block info, seg_id0: %v, seg_id1: %v, offset: %v", 
				block.SegmentId0, block.SegmentId1, block.Offset))
		}
	}

	if len(deleteBlocks) != 0 {
		sqltext := DeletePartialCoverBlockSql()
		for _, block := range deleteBlocks {
			now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)
			ctime := block.Ctime.Format(types.TIME_LAYOUT_TIDB)
			_, err = t.Client.Exec(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, block.SegmentId0, block.SegmentId1,
				block.BlockId, block.Size, block.Offset, block.FileBlockEndAddr, ctime, now, types.Deleted)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("DealOverlappingBlocks:Failed to delete the file block from tidb, offset: %d, err: %v", block.Offset, err))
				err = ErrYIgFsInternalErr
				return
			}
			helper.Logger.Info(ctx, fmt.Sprintf("DealOverlappingBlocks:Succeed to delete the file block info, seg_id0: %v, seg_id1: %v, offset: %v", block.SegmentId0, block.SegmentId1, block.Offset))
		}
	}

	if len(insertBlocks) != 0 {
		sqltext := GetInsertBlockSql()
		for _, block := range insertBlocks {
			ctime := block.Ctime.Format(types.TIME_LAYOUT_TIDB)
			_, err = t.Client.Exec(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, block.SegmentId0, block.SegmentId1,
				block.BlockId, block.Size, block.Offset, block.FileBlockEndAddr, ctime)
			if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("DealOverlappingBlocks:Failed to create the file segment to tidb, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			}
			helper.Logger.Info(ctx, fmt.Sprintf("DealOverlappingBlocks:Succeed to insert file block, seg_id0: %v, seg_id1: %v, offset: %v", block.SegmentId0, block.SegmentId1, block.Offset))
		}
	}

	return
}

func(t *TidbClient) DeleteBlocks(ctx context.Context, blockInfo *types.DescriptBlockInfo, blocks []*types.BlockInfo) (err error) {
	for _, block := range blocks {
		sqltext := DeleteBlockSql()
		_, err = t.Client.Exec(sqltext, types.Deleted, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, block.Offset)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete the file block from tidb, offset: %d, err: %v", block.Offset, err))
			return ErrYIgFsInternalErr
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to delete the the file blocks: %d", len(blocks)))
	return
}

func(t *TidbClient) GetMergeBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, 
	block *types.BlockInfo) (isExisted bool, fileBlockResp *types.FileBlockInfo, err error) {
	fileBlockResp = &types.FileBlockInfo{}
	isExisted = false

	sqltext := "select size, offset, end_addr from file_blocks where region=? and bucket_name=?" + 
		" and ino=? and generation=? and seg_id0=? and seg_id1=? and block_id=? and end_addr=? and is_deleted=?"
	row := t.Client.QueryRow(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, 
		blockInfo.Generation, blockInfo.SegmentId0, blockInfo.SegmentId1, block.BlockId, block.Offset, types.NotDeleted)
	err = row.Scan(
		&fileBlockResp.Size,
		&fileBlockResp.Offset,
		&fileBlockResp.FileBlockEndAddr)

	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the merge file block info, seg_id0: %d, seg_id1: %v, offset: %v, err: %v", 
			blockInfo.SegmentId0, blockInfo.SegmentId1, block.Offset, err))
		err = ErrYIgFsInternalErr
		return
	}

	isExisted = true
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get the merge file block info, size: %d, offset: %v", fileBlockResp.Size, fileBlockResp.Offset))
	return
}

func(t *TidbClient) UpdateBlock(ctx context.Context, block *types.FileBlockInfo) (err error) {
	sqltext := UpdateBlockSql()
	_, err = t.Client.Exec(sqltext, block.BlockId, block.Size, block.FileBlockEndAddr, block.Region, block.BucketName, 
		block.Ino, block.Generation, block.Offset, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to update file block info, seg_id0: %v, seg_id1: %v, offset: %v, err: %v", 
			block.SegmentId0, block.SegmentId1, block.Offset, err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update file block info, offset: %v, blockId: %v, size: %v, end_addr: %v",
		block.Offset, block.BlockId, block.Size, block.FileBlockEndAddr))
	return
}

func (t *TidbClient) CreateFileBlock(ctx context.Context, block *types.FileBlockInfo) (err error) {
	sqltext := GetInsertBlockSql()
	now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)
	_, err = t.Client.Exec(sqltext, block.Region, block.BucketName, block.Ino, block.Generation, block.SegmentId0, block.SegmentId1,
		block.BlockId, block.Size, block.Offset, block.FileBlockEndAddr, now)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create the file segment to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
			
	helper.Logger.Info(ctx, fmt.Sprintf("Successed to create the file segment, blockId: %v", block.BlockId))
	return
}

func(t *TidbClient) GetFileBlockSize(ctx context.Context, file *types.GetFileInfoReq) (blocksSize uint64, blocksNum uint32, err error) {
	sqltext := "select sum(size), count(*) from file_blocks where region=? and bucket_name=? and ino=? and generation=? and is_deleted=?"
	row := t.Client.QueryRow(sqltext, file.Region, file.BucketName, file.Ino, file.Generation, types.NotDeleted)

	err = row.Scan(
		&blocksSize,
		&blocksNum,
	)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get all blocks size and number for the target file, ino: %d, err: %v", file.Ino, err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get all blocks file and number for the target file, size: %v, number: %v", blocksSize, blocksNum))
	return
}

func (t *TidbClient) GetIncludeOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, 
	checkOffset int64) (segmentMap map[interface{}][]int64, offsetMap map[int64]int64, err error) {
	var segmentId0, segmentId1 uint64
	var blockId, offset int64
	segmentMap = make(map[interface{}][]int64)
	offsetMap = make(map[int64]int64)

	args := make([]interface{}, 0)
	sqltext := "select seg_id0, seg_id1, block_id, offset from file_blocks where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=? and offset <= ? and end_addr > ? order by offset;"
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
			&blockId,
			&offset)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetIncludeOffsetIndexInfo in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		segmentId := [2]uint64{segmentId0, segmentId1}
		segmentMap[segmentId] = append(segmentMap[segmentId], blockId)
		offsetMap[blockId] = offset
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetIncludeOffsetIndexInfo in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("GetIncludeOffsetIndexInfo: segmentMap is %v", segmentMap))
	return
}

func (t *TidbClient) GetGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, 
	checkOffset int64) (segmentMap map[interface{}][]int64, offsetMap map[int64]int64, err error) {
	var segmentId0, segmentId1 uint64
	var blockId, offset int64
	segmentMap = make(map[interface{}][]int64)
	offsetMap = make(map[int64]int64)

	args := make([]interface{}, 0)
	var sqltext string

	if checkOffset > 0 {
		sqltext = "select seg_id0, seg_id1, block_id, offset from file_blocks where region=? and bucket_name=? and ino=?" + 
			" and generation=? and is_deleted=? and offset > ? order by offset;"
		args = append(args, seg.Region, seg.BucketName, seg.Ino, seg.Generation, types.NotDeleted, checkOffset)
	} else {
		sqltext = "select seg_id0, seg_id1, block_id, offset from file_blocks where region=? and bucket_name=? and ino=?" + 
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
			&blockId,
			&offset)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetGreaterOffsetIndexInfo in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		segmentId := [2]uint64{segmentId0, segmentId1}
		segmentMap[segmentId] = append(segmentMap[segmentId], blockId)
		offsetMap[blockId] = offset
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to GetGreaterOffsetIndexInfo in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("GetGreaterOffsetIndexInfo: segmentMap is %v", segmentMap))
	return
}

func (t *TidbClient) GetBlocksBySegId(ctx context.Context, seg *types.GetTheSlowestGrowingSeg) (resp *types.GetSegmentResp, err error) {
	resp = &types.GetSegmentResp {
		Segments: []*types.SegmentInfo{},
	}

	sqltext := "select block_id, offset from file_blocks where seg_id0=? and seg_id1=? and is_deleted=? order by offset;"
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

	var blockId, offset int64
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
			&blockId,
			&offset)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to scan query blocks getting by segId, segId0: %v, segId1: %v, err: %v", 
				seg.SegmentId0, seg.SegmentId1, err))
			err = ErrYIgFsInternalErr
			return
		}

		sqltext = GetBlockInfoSql()
		row := t.Client.QueryRow(sqltext, seg.SegmentId0, seg.SegmentId1, blockId)
		block := &types.BlockInfo{}
		err = row.Scan(
			&block.SegStartAddr,
			&block.SegEndAddr,
			&block.Size)

		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get block info, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

		block.Offset = offset
		segment.Blocks = append(segment.Blocks, block)
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