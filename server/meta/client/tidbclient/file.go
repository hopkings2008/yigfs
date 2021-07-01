package tidbclient

import (
	"context"
	"database/sql"
	"time"
	"strings"
	"fmt"
	
	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
)


func GetDirFileInfoSql() (sqltext string) {
	sqltext = "select ino, generation, size, type, ctime, mtime, atime, perm, nlink, uid, gid," + 
		" blocks from file where region=? and bucket_name=? and parent_ino=? and file_name=?;"
	return sqltext
}

func GetDirFileInoSql() (sqltext string) {
	sqltext = "select ino from file where region=? and bucket_name=? and parent_ino=? and file_name=?;"
	return sqltext
}

func GetFileInfoSql() (sqltext string) {
	sqltext = "select generation, parent_ino, file_name, size, type, ctime, mtime, atime, perm, nlink," + 
		" uid, gid, blocks from file where region=? and bucket_name=? and ino=?;"
	return sqltext
}

func GetFileExistedSql() (sqltext string) {
	sqltext = "select 1 from file where region=? and bucket_name=? and ino=? limit 1;"
	return sqltext
}

func GetFileSizeAndBlocksSql() (sqltext string) {
	sqltext = "select size, blocks from file where region=? and bucket_name=? and ino=? and generation=?;"
	return sqltext
}

func UpdateFileSizeAndBlocksSql() (sqltext string) {
	sqltext = "update file set size=?, blocks=? where region=? and bucket_name=? and ino=? and generation=?;"
	return sqltext
}

func DeleteFileSql() (sqltext string) {
	sqltext = "delete from file where region=? and bucket_name=? and ino=? and generation=?;"
	return sqltext
}

func (t *TidbClient) ListDirFiles(ctx context.Context, dir *types.GetDirFilesReq) (dirFilesResp []*types.GetDirFileInfo, offset uint64, err error) {
	var maxNum = 1000
	args := make([]interface{}, 0)
	sqltext := "select ino, file_name, type from file where region=? and bucket_name=? and parent_ino=? and ino > ? order by ino limit ?;"
	args = append(args, dir.Region, dir.BucketName, dir.ParentIno, dir.Offset, maxNum)

	rows, err := t.Client.Query(sqltext, args...)
	if err == sql.ErrNoRows {
		err = ErrYigFsNotFindTargetDirFiles
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to query dir files, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tmp types.GetDirFileInfo
		err = rows.Scan(
			&tmp.Ino,
			&tmp.FileName,
			&tmp.Type)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to list dir files in row, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}
		dirFilesResp = append(dirFilesResp, &tmp)
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to list dir files in rows, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	dirLength := len(dirFilesResp)
	if dirLength > 0 {
		offset = dirFilesResp[dirLength - 1].Ino + 1   //nextStartIno
	}

	helper.Logger.Info(ctx, fmt.Sprintf("succeed to list dir files, sqltext: %v, req offset: %v, resp offset: %v", sqltext, dir.Offset, offset))
	return
}

func (t *TidbClient) GetDirFileInfo(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error) {
	resp = &types.FileInfo{}
	var ctime, mtime, atime string
	sqltext := GetDirFileInfoSql()
	row := t.Client.QueryRow(sqltext, file.Region, file.BucketName, file.ParentIno, file.FileName)
	err = row.Scan(
		&resp.Ino,
		&resp.Generation,
		&resp.Size,
		&resp.Type,
		&ctime,
		&mtime,
		&atime,
		&resp.Perm,
		&resp.Nlink,
		&resp.Uid,
		&resp.Gid,
		&resp.Blocks,)

	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchFile
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the dir file info, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	cTime, err := time.Parse(types.TIME_LAYOUT_TIDB, ctime)
	if err != nil {
		return
	}
	mTime, err := time.Parse(types.TIME_LAYOUT_TIDB, mtime)
	if err != nil {
		return
	}
	aTime, err := time.Parse(types.TIME_LAYOUT_TIDB, atime)
	if err != nil {
		return
	}

	resp.Ctime = cTime.UnixNano()
	resp.Mtime = mTime.UnixNano()
	resp.Atime = aTime.UnixNano()
	resp.Region = file.Region
	resp.BucketName = file.BucketName
	resp.ParentIno = file.ParentIno
	resp.FileName = file.FileName

	helper.Logger.Info(ctx, fmt.Sprintf("succeed to get dir file info, sqltext: %v", sqltext))
	return
}

func (t *TidbClient) GetFileInfo(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error) {
	resp = &types.FileInfo{}
	var ctime, mtime, atime string

	sqltext:= GetFileInfoSql()
	row := t.Client.QueryRow(sqltext, file.Region, file.BucketName, file.Ino)
	err = row.Scan(
		&resp.Generation,
		&resp.ParentIno,
		&resp.FileName,
		&resp.Size,
		&resp.Type,
		&ctime,
		&mtime,
		&atime,
		&resp.Perm,
		&resp.Nlink,
		&resp.Uid,
		&resp.Gid,
		&resp.Blocks,)
		
	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchFile
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the file info, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	cTime, err := time.Parse(types.TIME_LAYOUT_TIDB, ctime)
	if err != nil {
		return
	}
	mTime, err := time.Parse(types.TIME_LAYOUT_TIDB, mtime)
	if err != nil {
		return
	}
	aTime, err := time.Parse(types.TIME_LAYOUT_TIDB, atime)
	if err != nil {
		return
	}
	
	resp.Ctime = cTime.UnixNano()
	resp.Mtime = mTime.UnixNano()
	resp.Atime = aTime.UnixNano()
	resp.Ino = file.Ino
	resp.Region = file.Region
	resp.BucketName = file.BucketName

	helper.Logger.Info(ctx, fmt.Sprintf("succeed to get file info, sqltext: %v", sqltext))
	return
}

func (t *TidbClient) InitRootDirs(ctx context.Context, rootDir *types.InitDirReq) (err error) {
	var tx interface{}
	var sqlTx *sql.Tx
	tx, err = t.Client.Begin()
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		} else {
			sqlTx.Rollback()
		}
	}()

	sqlTx, _ = tx.(*sql.Tx)

	now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)

	sqltext := "insert into file (ino, region, bucket_name, file_name, type, atime, perm, uid, gid) values(?,?,?,?,?,?,?,?,?);"
	isFileExistedSql := GetFileExistedSql()
	var f int
	row := sqlTx.QueryRow(isFileExistedSql, rootDir.Region, rootDir.BucketName, types.RootDirIno)
	err = row.Scan(
		&f,
	)
	if err == sql.ErrNoRows {
		// create root dir
		_, err = sqlTx.Exec(sqltext, types.RootDirIno, rootDir.Region, rootDir.BucketName, ".", types.DIR_FILE, now, 
			types.DIR_PERM, rootDir.Uid, rootDir.Gid)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to init root dir ., err: %v", err))
			return ErrYIgFsInternalErr
		}
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("InitRootDirs: Failed to get the root dir info, err: %v", err))
		return ErrYIgFsInternalErr
	}

	row = sqlTx.QueryRow(isFileExistedSql, rootDir.Region, rootDir.BucketName, types.RootParentDirIno)
	err = row.Scan(
		&f,
	)
	if err == sql.ErrNoRows {
		// create root parent dir
		_, err = sqlTx.Exec(sqltext, types.RootParentDirIno, rootDir.Region, rootDir.BucketName, "..", types.DIR_FILE, now, 
			types.DIR_PERM, rootDir.Uid, rootDir.Gid)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to init root parent dir .., err: %v", err))
			return ErrYIgFsInternalErr
		}
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("InitRootDirs: Failed to get the root parent dir info, err: %v", err))
		return ErrYIgFsInternalErr
	}	

	// create zone
	sqltext = CreateOrUpdateZoneSql()
	_, err = sqlTx.Exec(sqltext, rootDir.ZoneId, rootDir.Region, rootDir.BucketName, rootDir.Machine, types.MachineUp)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("InitRootDirs: Failed to create zone, err: %v", err))
		return ErrYIgFsInternalErr
	}

	helper.Logger.Info(ctx, "Succeed to init root dirs to tidb.")
	return
}

func (t *TidbClient) CreateFile(ctx context.Context, file *types.CreateFileReq) (err error) {
	var tx interface{}
	var sqlTx *sql.Tx
	tx, err = t.Client.Begin()
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		} else {
			sqlTx.Rollback()
		}
	}()

	sqlTx, _ = tx.(*sql.Tx)

	now := time.Now().UTC().Format(types.TIME_LAYOUT_TIDB)

	sqltext := "insert into file(region, bucket_name, parent_ino, file_name, size, type, atime, perm, uid, gid) values(?,?,?,?,?,?,?,?,?,?);"
	args := []interface{}{file.Region, file.BucketName, file.ParentIno, file.FileName, file.Size,
		file.Type, now, file.Perm, file.Uid, file.Gid}
	_, err = sqlTx.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFile: Failed to create file to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	// get file ino
	var ino uint64
	sqltext = GetDirFileInoSql()
	row := sqlTx.QueryRow(sqltext, file.Region, file.BucketName, file.ParentIno, file.FileName)
	err = row.Scan(
		&ino)

	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFile: Failed to get the dir file info, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	// create file leader
	sqltext = CreateOrUpdateFileLeaderSql()
	_, err = sqlTx.Exec(sqltext, file.ZoneId, file.Region, file.BucketName, ino, file.Generation, file.Machine, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFile: Failed to create file leader to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	// create or update zone
	sqltext = CreateOrUpdateZoneSql()
	_, err = sqlTx.Exec(sqltext, file.ZoneId, file.Region, file.BucketName, file.Machine, types.MachineUp)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFile: Failed to create or update zone to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, "Succeed to create file to tidb.")
	return
}

func (t *TidbClient) SetFileAttr(ctx context.Context, file *types.SetFileAttrReq) (err error) {
	now := time.Now().UTC()

	mtime := now
	if file.File.Mtime != nil {
		mtime = time.Unix(0, *file.File.Mtime).UTC()
	}

	sqltext := "update file set mtime=?,"
	args := []interface{}{mtime}

	if file.File.Atime != nil {
		atime := time.Unix(0, *file.File.Atime).UTC()
		sqltext += " atime=?,"
		args = append(args, atime)
	}

	if file.File.Ctime != nil {
		ctime := time.Unix(0, *file.File.Ctime).UTC()
		sqltext += " ctime=?,"
		args = append(args, ctime)
	}
	
	if file.File.Size != nil {
		sqltext += " size=?,"
		args = append(args, *file.File.Size)
	}

	if file.File.Blocks != nil {
		sqltext += " blocks=?,"
		args = append(args, *file.File.Blocks)
	}

	if file.File.Gid != nil {
		sqltext += " gid=?,"
		args = append(args, *file.File.Gid)
	}

	if file.File.Perm != nil {
		sqltext += " perm=?,"
		args = append(args, *file.File.Perm)
	}

	if file.File.Uid != nil {
		sqltext += " uid=?,"
		args = append(args, *file.File.Uid)
	}

	sqltext = strings.TrimRight(sqltext, ",")

	sqltext += " where region=? and bucket_name=? and ino=? and generation=?"
	args = append(args, file.Region, file.BucketName, file.File.Ino, file.File.Generation)
	
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to set file attr to tidb, err: %v, sqltext: %v", err, sqltext))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to set file attr to tidb, sqltext: %v", sqltext))
	return
}

func(t *TidbClient) GetFileSizeAndBlocksNum(ctx context.Context, file *types.CreateSegmentReq) (size uint64, blocksNum uint32, err error) {
	sqltext := GetFileSizeAndBlocksSql()
	row := t.Client.QueryRow(sqltext, file.Region, file.BucketName, file.Ino, file.Generation)
	err = row.Scan(
		&size,
		&blocksNum)
		
	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchFile
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the file size and blocks number, err: %v", err))
		err = ErrYIgFsInternalErr
	}
	
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to get file size and blocks number from tidb, size: %v, blocks number: %v", size, blocksNum))
	return
}

func(t *TidbClient) DeleteFile(ctx context.Context, file *types.DeleteFileReq) (err error) {
	sqltext := DeleteFileSql()
	_, err = t.Client.Exec(sqltext, file.Region, file.BucketName, file.Ino, file.Generation)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete the file, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	sqltext = DeleteFileLeaderSql()
	_, err = t.Client.Exec(sqltext, file.ZoneId, file.Region, file.BucketName, file.Ino, file.Generation)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete the file leader, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to delete the file, region: %v, bucket: %v, ino: %v, generation: %v", 
		file.Region, file.BucketName, file.Ino, file.Generation))
	return
}

func(t *TidbClient) UpdateSizeAndBlocksNum(ctx context.Context, file *types.GetFileInfoReq, blocksNum uint32, size uint64) (err error) {
	start := time.Now().UTC().UnixNano()
	sqltext := "update file set size=?, blocks=? where region=? and bucket_name=? and ino=? and generation=?;"
	_, err = t.Client.Exec(sqltext, size, blocksNum, file.Region, file.BucketName, file.Ino, file.Generation)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to update the file size and blocks number, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update file size and blocks number, ino: %v, size: %v, blocksNum: %v, cost: %v", 
		file.Ino, size, blocksNum, end - start))
	return
}

func(t *TidbClient) UpdateFileSizeAndBlocksNum(ctx context.Context, file *types.GetFileInfoReq) (err error) {
	start := time.Now().UTC().UnixNano()
	sqltext := "update file join (select sum(size) as totalSize, count(*) as number from file_blocks where region=? and bucket_name=? and ino=?" + 
		" and generation=? and is_deleted=0) b set file.size=b.totalSize, file.blocks=b.number where region=? and bucket_name=? and ino=? and generation=?;"
	_, err = t.Client.Exec(sqltext, file.Region, file.BucketName, file.Ino, file.Generation, file.Region, file.BucketName, file.Ino, file.Generation)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("CreateFileSegment: Failed to update the file size and blocks number, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to update file size and blocks number, ino: %v, cost: %v", file.Ino, end - start))
	return
}
