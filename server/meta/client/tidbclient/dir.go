package tidbclient

import (
        "context"
        "database/sql"
        "log"
        "time"

        "github.com/hopkings2008/yigfs/server/types"
)


func (t *TidbClient) ListDirFiles(ctx context.Context, files *types.GetDirFilesReq) (dirFilesResp []*types.GetDirFilesResp, err error) {
        var ino, generation uint64
        var maxNum = 1000

        args := make([]interface{}, 0)
        sqltext := "select ino, generation, file_name, type from dir where region=? and bucket_name=? and (ino + generation) > ? order by (ino + generation) limit ?;"
        args = append(args, files.Region, files.BucketName, files.Offset, maxNum)

        rows, err := t.Client.Query(sqltext, args...)
        if err == sql.ErrNoRows {
                err = nil
                return
        } else if err != nil {
                log.Fatal("Failed to list dir files, err:", err)
                return
        }
        defer rows.Close()

        for rows.Next() {
                var tmp types.GetDirFilesResp
                err = rows.Scan(
                        &ino,
                        &generation,
                        &tmp.FileName,
                        &tmp.Type)
                if err != nil {
			log.Fatal("Form dirFilesResp from tidb failed, err:", err)
                        return
                }

                if generation > 0 {
                        tmp.Ino = ino + generation
                } else {
                        tmp.Ino = ino
                }

                dirFilesResp = append(dirFilesResp, &tmp)

        }
        err = rows.Err()
        if err != nil {
                return
        }
        log.Println("succeed to list dir files, sqltext:", sqltext)
        return
}

func (t *TidbClient) CreateFile(ctx context.Context, file *types.CreateDirFileReq) (resp *types.CreateDirFileResp, err error) {
        createDirFileResp := &types.CreateDirFileResp{}
        now := time.Now().UTC()

        ino, generation, err := getMaxInoAndGeneration(t)
        if err != nil {
                return
        }

        if ino == types.MAXMUM_INO_VALUE {
                generation += 1
                sqltext := "insert into dir values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
                args := []interface{}{ino, generation, file.Region, file.BucketName, file.ParentIno, file.FileName, file.Size,
                        file.Type, file.Owner, now, now, file.Atime, file.Perm, file.Nlink, file.Uid, file.Gid}
                _, err = t.Client.Exec(sqltext, args...)
                if err != nil {
                        log.Fatal("Failed to create file into tidb where ino is maximum, err:", err)
                        return
                }
                createDirFileResp.Ino = ino + generation
                return
        }

        sqltext := "insert into dir values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        args := []interface{}{generation, file.Region, file.BucketName, file.ParentIno, file.FileName, file.Size,
                file.Type, file.Owner, now, now, file.Atime, file.Perm, file.Nlink, file.Uid, file.Gid}
        _, err = t.Client.Exec(sqltext, args...)
        if err != nil {
                log.Fatal("Failed to create file to tidb, err:", err)
                return
        }
        createDirFileResp.Ino = ino + 1
        log.Fatal("succeed to create or update image styles, sqltext:", sqltext)
        return createDirFileResp, nil
}

func getMaxInoAndGeneration(t *TidbClient) (ino uint64, generation uint64, err error) {
        sqltext := "select ino, generation from dir order by (ino + generation) desc limit 1"
        row := t.Client.QueryRow(sqltext)
        err = row.Scan(
                &ino,
                &generation,
        )
        if err == sql.ErrNoRows {
                err = nil
                return
        } else if err != nil {
                log.Fatal("Failed to get the maximum generation, err:", err)
                return
        }
        return ino, generation, nil
}

func (t *TidbClient) CreateAndUpdateRootDir(ctx context.Context, rootDir *types.CreateDirFileReq) (err error) {
        sql := "insert into dir (ino, file_name) values(?,?) on duplicate key update file_name=values(file_name)"
        args := []interface{}{rootDir.Ino, rootDir.FileName}
        _, err = t.Client.Exec(sql, args...)
        if err != nil {
                log.Fatal("Failed to CreateAndUpdateRootDir, err:", err)
                return err
        }
        return nil
}
