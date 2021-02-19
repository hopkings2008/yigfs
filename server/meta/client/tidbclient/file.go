package tidbclient

import (
        "context"
        "database/sql"
        "log"
        "time"

        "github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)


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
		log.Printf("Failed to query dir files, err: %v", err)
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
			log.Printf("Failed to list dir files in row, err: %v", err)
			err = ErrYIgFsInternalErr
                        return
                }
                dirFilesResp = append(dirFilesResp, &tmp)
        }
        err = rows.Err()
        if err != nil {
		log.Printf("Failed to list dir files in rows, err: %v", err)
		err = ErrYIgFsInternalErr
                return
        }

	dirLength := len(dirFilesResp)
        if dirLength > 0 {
		offset = dirFilesResp[dirLength - 1].Ino + 1   //nextStartIno
	}
        log.Printf("succeed to list dir files, sqltext: %v, req offset: %v, resp offset: %v", sqltext, dir.Offset, offset)
        return
}

func (t *TidbClient) GetDirFileInfo(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error) {
        resp = &types.FileInfo{}
        var ctime, mtime, atime string
        sqltext := "select ino, generation, size, type, ctime, mtime, atime, perm, nlink, uid, gid," + 
		" blocks from file where region=? and bucket_name=? and parent_ino=? and file_name=?"
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
		&resp.Blocks,
        )

        if err == sql.ErrNoRows {
                err = ErrYigFsNoSuchFile
                return
        } else if err != nil {
                log.Printf("Failed to get the dir file info, err: %v", err)
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

	log.Printf("succeed to get dir file info, sqltext: %v", sqltext)
        return
}

func (t *TidbClient) GetFileInfo(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error) {
        resp = &types.FileInfo{}
        var ctime, mtime, atime string
        sqltext := "select generation, parent_ino, file_name, size, type, ctime, mtime, atime, perm, nlink," + 
		" uid, gid, blocks from file where region=? and bucket_name=? and ino=?"
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
		&resp.Blocks,
        )

        if err == sql.ErrNoRows {
                err = ErrYigFsNoSuchFile
                return
        } else if err != nil {
                log.Printf("Failed to get the file info, err: %v", err)
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

	log.Printf("succeed to get file info, sqltext: %v", sqltext)
        return
}

func (t *TidbClient) InitRootDir(ctx context.Context, rootDir *types.InitDirReq) (err error) {
        now := time.Now().UTC()

        sqltext := "insert into file (ino, region, bucket_name, file_name, type, ctime, mtime, atime, perm, uid, gid) values(?,?,?,?,?,?,?,?,?,?,?)"
        args := []interface{}{types.RootDirIno, rootDir.Region, rootDir.BucketName, ".", types.DIR_FILE, 
		now, now, now, types.DIR_PERM, rootDir.Uid, rootDir.Gid}
        _, err = t.Client.Exec(sqltext, args...)
        if err != nil {
		log.Printf("Failed to init root dir ., err: %v", err)
               	err = ErrYIgFsInternalErr
                return
        }

	log.Printf("succeed to init root dir")
	return
}

func (t *TidbClient) InitParentDir(ctx context.Context, rootDir *types.InitDirReq) (err error) {
        now := time.Now().UTC()

	sqltext := "insert into file (ino, region, bucket_name, file_name, type, ctime, mtime, atime, perm, uid, gid) values(?,?,?,?,?,?,?,?,?,?,?)"
        args := []interface{}{types.RootParentDirIno, rootDir.Region, rootDir.BucketName, "..", types.DIR_FILE, 
		now, now, now, types.DIR_PERM, rootDir.Uid, rootDir.Gid}
        _, err = t.Client.Exec(sqltext, args...)
        if err != nil {
                log.Printf("Failed to init root parent dir .., err: %v", err)
                err = ErrYIgFsInternalErr
                return
        }

        log.Printf("succeed to init root parent dir")
        return
}

func (t *TidbClient) CreateFile(ctx context.Context, file *types.CreateFileReq) (err error) {
	now := time.Now().UTC()

	ctime := now
	if file.Ctime != 0 {
		ctime = time.Unix(0, file.Ctime).UTC()
	}
	mtime := now
	if file.Mtime != 0 {
		mtime = time.Unix(0, file.Mtime).UTC()
	}
	atime := now
	if file.Atime != 0 {
		atime = time.Unix(0, file.Atime).UTC()
	}

	sqltext := "insert into file(region, bucket_name, parent_ino, file_name, size, type, ctime, mtime, atime, perm," +
		" nlink, uid, gid, blocks) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
	args := []interface{}{file.Region, file.BucketName, file.ParentIno, file.FileName, file.Size,
		file.Type, ctime, mtime, atime, file.Perm, file.Nlink, file.Uid, file.Gid, file.Blocks}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		log.Printf("Failed to create file to tidb, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}
	log.Printf("Succeed to create file, sqltext: %v", sqltext)
	return
}

func (t *TidbClient) SetFileAttr(ctx context.Context, file *types.SetFileAttrReq) (err error) {
	now := time.Now().UTC()
	generation := 0

	ctime := now
        if file.File.Ctime != 0 {
                ctime = time.Unix(0, file.File.Ctime).UTC()
        }
	mtime := now
	if file.File.Mtime != 0 {
		mtime = time.Unix(0, file.File.Mtime).UTC()
	}
	atime := now
	if file.File.Atime != 0 {
		atime = time.Unix(0, file.File.Atime).UTC()
	}
	
	sqltext := "update file set size=?, ctime=?, mtime=?, atime=?, perm=?, uid=?, gid=?, blocks=? where" + 
		" region=? and bucket_name=? and ino=? and generation=?"
	args := []interface{}{file.File.Size, ctime, mtime, atime, file.File.Perm, file.File.Uid, file.File.Gid, file.File.Blocks, 
		file.Region, file.BucketName, file.File.Ino, generation}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		log.Printf("Failed to set file attr to tidb, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("Succeed to set file attr to tidb, sqltext: %v", sqltext)
	return
}