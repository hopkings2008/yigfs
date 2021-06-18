package tidbclient

import (
	"context"
	"database/sql"
	"fmt"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
)


func CreateOrUpdateZoneSql() (sqltext string) {
	sqltext = "insert into zone(id, region, bucket_name, machine, status) values(?,?,?,?,?) on duplicate key update status=values(status)"
	return sqltext
}

func (t *TidbClient) CreateOrUpdateZone(ctx context.Context, zone *types.InitDirReq) (err error) {
	sqltext := CreateOrUpdateZoneSql()
	_, err = t.Client.Exec(sqltext, zone.ZoneId, zone.Region, zone.BucketName, zone.Machine, types.MachineUp)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create or update zone to tidb, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create or update zone to tidb, sqltext: %v", sqltext))
	return
}

func (t *TidbClient) GetOneUpMachine(ctx context.Context, zone *types.GetLeaderReq) (leader string, err error) {
	sqltext := "select machine from zone where id=? and region=? and bucket_name=? and status=? order by weight desc limit 1"
	row := t.Client.QueryRow(sqltext, zone.ZoneId, zone.Region, zone.BucketName, types.MachineUp)
	err = row.Scan(
		&leader,
	)

	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchMachine
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get one up machine, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}

	helper.Logger.Info(ctx, fmt.Sprintf("succeed to get one up machine, sqltext: %v", sqltext))
	return
}

func (t *TidbClient) GetMachineInfo(ctx context.Context, zone *types.GetLeaderReq) (resp *types.GetMachineInfoResp, err error) {
	resp = &types.GetMachineInfoResp{}
	sqltext := "select status, weight from zone where id=? and region=? and bucket_name=? and machine=?"
	row := t.Client.QueryRow(sqltext, zone.ZoneId, zone.Region, zone.BucketName, zone.Machine)
	err = row.Scan(
		&resp.Status,
		&resp.Weight,
	)

	if err == sql.ErrNoRows {
		err = ErrYigFsNoSuchMachine
		return
	} else if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get machine info, err: %v", err))
		err = ErrYIgFsInternalErr
		return
	}
	
	helper.Logger.Info(ctx, fmt.Sprintf("succeed to get machine info, machine: %v", zone.Machine))
	return
}

func (t *TidbClient) CheckSegsmachine(ctx context.Context, zone *types.GetSegLeaderReq, segs []*types.CreateBlocksInfo) (isValid bool, err error) {
	getLeaderSql := GetSegmentLeaderSql()
	sqltext := "select status from zone where id=? and region=? and bucket_name=? and machine=?"
	var stmt *sql.Stmt
	var status int
	stmt, err = t.Client.Prepare(sqltext)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to prepare get machines info, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}

	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to close get machine info stmt, err: %v", err))
			err = ErrYIgFsInternalErr
		}
	}()

	var leader string
	for _, seg := range segs {
		row := t.Client.QueryRow(getLeaderSql, zone.ZoneId, zone.Region, zone.BucketName, seg.SegmentId0, seg.SegmentId1, types.NotDeleted)
		err = row.Scan (
			&leader,
		)
	
		if err == sql.ErrNoRows {
			row := stmt.QueryRow(zone.ZoneId, zone.Region, zone.BucketName, seg.Leader)
			err = row.Scan(
				&status,
			)
	
			if err == sql.ErrNoRows {
				err = ErrYigFsNoSuchMachine
				return
			} else if err != nil {
				helper.Logger.Error(ctx, fmt.Sprintf("Failed to get machine info, err: %v", err))
				err = ErrYIgFsInternalErr
				return
			} else if status != types.MachineUp {
				helper.Logger.Error(ctx, fmt.Sprintf("The machine is not up, machine: %v", seg.Leader))
				err = ErrYigFsLeaderStatusIsInvalid
				return
			}
		} else if err == nil {
			if leader != seg.Leader {
				helper.Logger.Error(ctx, fmt.Sprintf("The segment machine is not match leader, seg_id0: %v, seg_id1: %v, leader: %v, err: %v", 
					seg.SegmentId0, seg.SegmentId1, leader, err))
				err = ErrYigFsMachineNotMatchSegLeader
				return
			}
		} else {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get the segment leader, err: %v", err))
			err = ErrYIgFsInternalErr
			return
		}
	}

	isValid = true
	helper.Logger.Info(ctx, fmt.Sprintf("succeed to check machines, machines number: %v", len(segs)))
	return
}
