package tidbclient

import (
	"context"
	"database/sql"
	"log"
	"time"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
)

func (t *TidbClient) GetLeaderInfo(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error) {
	resp = &types.GetLeaderResp {
		LeaderInfo: &types.LeaderInfo{},
	}

	sqltext := "select leader from leader where zone_id=? and region=? and bucket_name=? and ino=?"
	row := t.Client.QueryRow(sqltext, leader.ZoneId, leader.Region, leader.BucketName, leader.Ino)
	err = row.Scan (
		&resp.LeaderInfo.Leader,
	)

	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchLeader
		return
	} else if err != nil {
		log.Printf("Failed to get the leader, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	resp.LeaderInfo.ZoneId = leader.ZoneId
	log.Printf("succeed to get the leader from tidb, sqltext: %v", sqltext)
	return
}

func (t *TidbClient) CreateOrUpdateLeader(ctx context.Context, leader *types.GetLeaderReq) (err error) {
	now := time.Now().UTC()
	generation := 0

	sqltext := "insert into leader values(?,?,?,?,?,?,?,?,?) on duplicate key update leader=values(leader), mtime=values(mtime), is_deleted=values(is_deleted)"
	args := []interface{}{leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, generation, leader.Machine, now, now, types.NotDeleted}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		log.Printf("Failed to create leader to tidb, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("Succeed to create leader to tidb, sqltext: %v", sqltext)
	return
}
