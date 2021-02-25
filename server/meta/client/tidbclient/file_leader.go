package tidbclient

import (
	"context"
	"database/sql"
	"log"
	"time"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
)


func CreateOrUpdateFileLeaderSql(leader *types.GetLeaderReq) (sqltext string, args []interface{}) {
	now := time.Now().UTC()
	sqltext = "insert into file_leader values(?,?,?,?,?,?,?,?,?) on duplicate key update leader=values(leader), mtime=values(mtime), is_deleted=values(is_deleted)"
	args = []interface{}{leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, leader.Generation, leader.Machine, now, now, types.NotDeleted}
	return sqltext, args
}

func (t *TidbClient) GetFileLeaderInfo(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error) {
	resp = &types.GetLeaderResp {
		LeaderInfo: &types.LeaderInfo{},
	}

	sqltext := "select leader from file_leader where zone_id=? and region=? and bucket_name=? and ino=?"
	row := t.Client.QueryRow(sqltext, leader.ZoneId, leader.Region, leader.BucketName, leader.Ino)
	err = row.Scan (
		&resp.LeaderInfo.Leader,
	)

	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchLeader
		return
	} else if err != nil {
		log.Printf("Failed to get the file leader, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	resp.LeaderInfo.ZoneId = leader.ZoneId
	log.Printf("succeed to get the file leader from tidb, sqltext: %v", sqltext)
	return
}

func (t *TidbClient) CreateOrUpdateFileLeader(ctx context.Context, leader *types.GetLeaderReq) (err error) {
	sqltext, args := CreateOrUpdateFileLeaderSql(leader)
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		log.Printf("Failed to create file leader to tidb, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("Succeed to create file leader to tidb, sqltext: %v", sqltext)
	return
}
