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
        sqltext := "select ino, file_name, type from dir where region=? and bucket_name=? and ino >= ? order by ino limit ?;"
        args = append(args, dir.Region, dir.BucketName, dir.Offset, maxNum)

        rows, err := t.Client.Query(sqltext, args...)
        if err == sql.ErrNoRows {
                err = ErrYigFsNotFindTargetDirFiles
                return
        } else if err != nil {
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

        offset = uint64(len(dirFilesResp)) + dir.Offset //nextStartIno
        log.Println("succeed to list dir files, sqltext:", sqltext)
        return
}

func (t *TidbClient) CreateAndUpdateRootDir(ctx context.Context, rootDir *types.FileInfo) (err error) {
        sql := "insert into dir (ino, file_name, type) values(?,?,?) on duplicate key update file_name=values(file_name)"
        args := []interface{}{rootDir.Ino, rootDir.FileName, rootDir.Type}
        _, err = t.Client.Exec(sql, args...)
        if err != nil {
		log.Printf("Failed to create and update root dir, err: %v", err)
               	err = ErrYIgFsInternalErr
                return
        }
        return
}

func (t *TidbClient) CreateFile(ctx context.Context, file *types.FileInfo) (err error) {
        now := time.Now().UTC()

        sqltext := "insert into dir(region, bucket_name, parent_ino, file_name, size, type, owner, ctime, mtime, atime, perm," +
            " nlink, uid, gid) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        args := []interface{}{file.Region, file.BucketName, file.ParentIno, file.FileName, file.Size,
                file.Type, file.Owner, now, now, now, file.Perm, file.Nlink, file.Uid, file.Gid}
        _, err = t.Client.Exec(sqltext, args...)
        if err != nil {
                log.Printf("Failed to create file to tidb, err: %v", err)
                err = ErrYIgFsInternalErr
                return
        }
        log.Printf("Succeed to create file, sqltext: %v", sqltext)
        return
}


func (t *TidbClient) GetDirFileInfo(ctx context.Context, file *types.GetDirFileInfoReq) (resp *types.FileInfo, err error) {
        resp = &types.FileInfo{}
        var ctime, mtime, atime string
        sqltext := "select ino, generation, size, type, owner, ctime, mtime, atime, perm, nlink, uid, gid from dir where region=? and bucket_name=? and parent_ino=? and file_name=?"
        row := t.Client.QueryRow(sqltext, file.Region, file.BucketName, file.ParentIno, file.FileName)
        err = row.Scan(
                &resp.Ino,
                &resp.Generation,
                &resp.Size,
                &resp.Type,
                &resp.Owner,
                &ctime,
                &mtime,
                &atime,
                &resp.Perm,
                &resp.Nlink,
                &resp.Uid,
                &resp.Gid,
        )

        if err == sql.ErrNoRows {
                err = ErrYigFsNoSuchFile
                return
        } else if err != nil {
                log.Printf("Failed to get the dir file info, err: %v", err)
                err = ErrYIgFsInternalErr
                return
        }

        resp.Ctime, err = time.Parse(types.TIME_LAYOUT_TIDB, ctime)
        if err != nil {
                return
        }
        resp.Mtime, err = time.Parse(types.TIME_LAYOUT_TIDB, mtime)
        if err != nil {
                return
        }
        resp.Atime, err = time.Parse(types.TIME_LAYOUT_TIDB, atime)
        if err != nil {
                return
        }
        resp.Region = file.Region
        resp.BucketName = file.BucketName
        resp.ParentIno = file.ParentIno
        resp.FileName = file.FileName
        return
}

func (t *TidbClient) GetFileInfo(ctx context.Context, file *types.GetFileInfoReq) (resp *types.FileInfo, err error) {
        resp = &types.FileInfo{}
        var ctime, mtime, atime string
        sqltext := "select generation, region, bucket_name, parent_ino, file_name, size, type, owner, ctime, mtime, atime, perm, nlink, uid, gid from dir where ino = ?"
        row := t.Client.QueryRow(sqltext, file.Ino)
        err = row.Scan(
                &resp.Generation,
                &resp.Region,
                &resp.BucketName,
                &resp.ParentIno,
                &resp.FileName,
                &resp.Size,
                &resp.Type,
                &resp.Owner,
                &ctime,
                &mtime,
                &atime,
                &resp.Perm,
                &resp.Nlink,
                &resp.Uid,
                &resp.Gid,
        )

        if err == sql.ErrNoRows {
                err = ErrYigFsNoSuchFile
                return
        } else if err != nil {
                log.Printf("Failed to get the file info, err: %v", err)
                err = ErrYIgFsInternalErr
                return
        }

        resp.Ctime, err = time.Parse(types.TIME_LAYOUT_TIDB, ctime)
        if err != nil {
                return
        }
        resp.Mtime, err = time.Parse(types.TIME_LAYOUT_TIDB, mtime)
        if err != nil {
                return
        }
        resp.Atime, err = time.Parse(types.TIME_LAYOUT_TIDB, atime)
        if err != nil {
                return
        }
        resp.Ino = file.Ino

        return
}
