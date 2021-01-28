package tidbclient

import (
	"context"
	"database/sql"
	"log"
	"time"

	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/types"
)


func (t *TidbClient) CreateOrUpdateZone(ctx context.Context, zone *types.InitDirReq) (err error) {
	now := time.Now().UTC()

	sqltext := "insert into zone values(?,?,?,?,?,?,?,?) on duplicate key update status=values(status), mtime=values(mtime)"
	args := []interface{}{zone.ZoneId, zone.Region, zone.BucketName, zone.Machine, types.MachineUp, 0, now, now}
	_, err = t.Client.Exec(sqltext, args...)
	if err != nil {
		log.Printf("Failed to create or update zone to tidb, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("Succeed to create or update zone to tidb, sqltext: %v", sqltext)
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
		log.Printf("Failed to get one up machine, err: %v", err)
		err = ErrYIgFsInternalErr
		return
	}

	log.Printf("succeed to get one up machine, sqltext: %v", sqltext)
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
                log.Printf("Failed to get machine info, err: %v", err)
                err = ErrYIgFsInternalErr
                return
        }

        log.Printf("succeed to get machine info, sqltext: %v", sqltext)
        return
}

