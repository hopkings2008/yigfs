package tidbclient

import (
	"context"
	"database/sql"
	"fmt"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
)


func CreateOrUpdateFileLeaderSql() (sqltext string) {
	sqltext = "insert into file_leader(zone_id, region, bucket_name, ino, generation, leader, is_deleted) values(?,?,?,?,?,?,?)" +
		" on duplicate key update leader=values(leader), is_deleted=values(is_deleted)"
	return sqltext
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
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the file leader, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	resp.LeaderInfo.ZoneId = leader.ZoneId
	helper.Logger.Info(ctx, fmt.Sprintf("succeed to get the file leader from tidb, sqltext: %v", sqltext))
	return
}

func (t *TidbClient) CreateOrUpdateFileLeader(ctx context.Context, leader *types.GetLeaderReq) (err error) {
	sqltext := CreateOrUpdateFileLeaderSql()
	_, err = t.Client.Exec(sqltext, leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, leader.Generation, leader.Machine, types.NotDeleted)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create file leader to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create file leader to tidb, sqltext: %v", sqltext))
	return
}
