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
	sqltext = "update file_blocks set block_id=?, size=?, end_addr=?, mtime=? where region=?" + 
		" and bucket_name=? and ino=? and generation=? and offset=? and is_deleted=?;"
	return sqltext
}

func UpdateBlockIdSql() (sqltext string) {
	sqltext = "update file_blocks set block_id=? where region=? and bucket_name=? and ino=? and generation=? and offset=?;"
	return sqltext
}

func GetInsertBlockSql() (sqltext string) {
	sqltext = "insert into file_blocks values(?,?,?,?,?,?,?,?,?,?,?,?,?);"
	return sqltext
}

func DeletePartialCoverBlockSql() (sqltext string) {
	sqltext = "insert into file_blocks values(?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key" +
		" update size=values(size), end_addr=values(end_addr), is_deleted=values(is_deleted), mtime=values(mtime);"
	return sqltext
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
			now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)
			_, err = t.Client.Exec(sqltext, block.BlockId, block.Size, block.FileBlockEndAddr, now, blockInfo.Region, blockInfo.BucketName, 
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
			now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)
			ctime := block.Ctime.Format(types.TIME_LAYOUT_TIDB)
			helper.Logger.Error(ctx, fmt.Sprintf("DealOverlappingBlocks insert ctime: %v, block ctime: %v", ctime, block.Ctime))
			_, err = t.Client.Exec(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, block.SegmentId0, block.SegmentId1,
				block.BlockId, block.Size, block.Offset, block.FileBlockEndAddr, ctime, now, types.NotDeleted)
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

func(t *TidbClient) GetMergeBlock(ctx context.Context, blockInfo *types.DescriptBlockInfo, block *types.BlockInfo) (isExisted bool, fileBlockResp *types.FileBlockInfo, err error) {
	fileBlockResp = &types.FileBlockInfo{}
	isExisted = false

	sqltext := "select block_id, size, offset, end_addr from file_blocks where region=? and bucket_name=? and ino=? and generation=? and seg_id0=? and seg_id1=? and end_addr=? and is_deleted=?"
	row := t.Client.QueryRow(sqltext, blockInfo.Region, blockInfo.BucketName, blockInfo.Ino, blockInfo.Generation, blockInfo.SegmentId0, blockInfo.SegmentId1, block.Offset, types.NotDeleted)
	err = row.Scan(
		&fileBlockResp.BlockId,
		&fileBlockResp.Size,
		&fileBlockResp.Offset,
		&fileBlockResp.FileBlockEndAddr)

	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the merge block info, seg_id0: %d, seg_id1: %v, offset: %v, err: %v", 
			blockInfo.SegmentId0, blockInfo.SegmentId1, block.Offset, err))
		err = ErrYIgFsInternalErr
		return
	}

	isExisted = true
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get the merge block info, size: %d, offset: %v", fileBlockResp.Size, fileBlockResp.Offset))
	return
}

func(t *TidbClient) UpdateBlock(ctx context.Context, block *types.FileBlockInfo) (err error) {
	sqltext := UpdateBlockSql()
	now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)
	_, err = t.Client.Exec(sqltext, block.BlockId, block.Size, block.FileBlockEndAddr, now, block.Region, block.BucketName, 
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
		block.BlockId, block.Size, block.Offset, block.FileBlockEndAddr, now, now, types.NotDeleted)
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

func (t *TidbClient) GetGreaterOffsetIndexSegs(ctx context.Context, seg *types.GetSegmentReq, checkOffset int64) (segmentMap map[interface{}][]int64, offsetMap map[int64]int64, err error) {
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