package tidbclient

import (
	"context"
	"database/sql"
	"log"
	"time"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
)

func GetSegmentLeaderSql() (sqltext string) {
	sqltext = "select leader from segment_leader where zone_id=? and region=? and bucket_name=? and seg_id0=? and seg_id1=?"
	return sqltext
}

func (t *TidbClient) GetSegmentLeaderInfo(ctx context.Context, segment *types.GetSegLeaderReq) (resp *types.LeaderInfo, err error) {
	resp = &types.LeaderInfo {}

	sqltext := GetSegmentLeaderSql()
	row := t.Client.QueryRow(sqltext, segment.ZoneId, segment.Region, segment.BucketName, segment.SegmentId0, segment.SegmentId1)
	err = row.Scan (
		&resp.Leader,
	)

	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchLeader
		return
	} else if err != nil {
		log.Printf("Failed to get the segment leader, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	resp.ZoneId = segment.ZoneId
	log.Printf("succeed to get the segment leader from tidb, sqltext: %v", sqltext)
	return
}

func (t *TidbClient) CreateSegmentLeader(ctx context.Context, segment *types.CreateSegmentReq) (err error) {
	now := time.Now().UTC()

	sqltext := "insert into segment_leader values(?,?,?,?,?,?,?,?,?)"
	args := []interface{}{segment.ZoneId, segment.Region, segment.BucketName, segment.Segment.SegmentId0,
		segment.Segment.SegmentId1, segment.Machine, now, now, types.NotDeleted}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		log.Printf("Failed to create segment leader to tidb, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("Succeed to create segment leader to tidb, sqltext: %v", sqltext)
	return
}

