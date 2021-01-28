package storage

import (
	"context"
	"log"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)


func GetMachineAndUpdateLeader(ctx context.Context, leader *types.GetLeaderReq, yigFs *YigFsStorage) (resp *types.GetLeaderResp, err error) {
	// get a up machine from zone
	machine, err := yigFs.MetaStorage.Client.GetOneUpMachine(ctx, leader)
	if err != nil {
		log.Printf("Failed to get one up machine, zone_id: %s, region: %s, bucket: %s, err: %v", leader.ZoneId, leader.Region, leader.BucketName, err)
		return
	}

	// update leader
	leader.Machine = machine
	err = yigFs.MetaStorage.Client.CreateOrUpdateLeader(ctx, leader)
	if err != nil {
		log.Printf("Failed to create leader, zone_id: %s, region: %s, bucket: %s, ino: %d, leader: %s, err: %v",
			leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, leader.Machine, err)
		return
	}

	resp = &types.GetLeaderResp {
		LeaderInfo: &types.LeaderInfo {
			ZoneId: leader.ZoneId,
			Leader: machine,
		},
	}

	log.Printf("Get one up machine is: %s, zone_id is: %s", machine, leader.ZoneId)
	return
}

func GetUpLeader(ctx context.Context, leader *types.GetLeaderReq, yigFs *YigFsStorage) (resp *types.GetLeaderResp, err error) {
	resp, err = yigFs.MetaStorage.Client.GetLeaderInfo(ctx, leader)
	switch err {
	case ErrYigFsNoSuchLeader:
		// if leader is non, get a up machine from zone and update leader info
		getMachineResp, err := GetMachineAndUpdateLeader(ctx, leader, yigFs)
		if err != nil {
			return resp, err
		}
		return getMachineResp, nil
	case nil:
		// if leader exist, determine where leader status is up
		getMachineInfoResp, err:= yigFs.MetaStorage.Client.GetMachineInfo(ctx, leader)
		if err != nil && err != ErrYigFsNoSuchMachine {
			log.Printf("Failed to get machine info, zone_id: %s, region: %s, bucket: %s, machine: %s, err: %v", leader.ZoneId, leader.Region, leader.BucketName, leader.Machine, err)
			return resp, err
		}

		// if status does not up or the target leader is not existed in zone, get a up machine from zone and update leader info.
		if err == ErrYigFsNoSuchMachine || getMachineInfoResp.Status != types.MachineUp {
			getMachineResp, err := GetMachineAndUpdateLeader(ctx, leader, yigFs)
			if err != nil {
				return resp, err
			}
			return getMachineResp, nil
		}

		return resp, nil
	default:
		log.Printf("Failed to get leader, zone_id: %s, region: %s, bucket: %s, ino: %d, err: %v", leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, err)
		return
	}
}

func(yigFs *YigFsStorage) GetLeader(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error) {
	resp, err = GetUpLeader(ctx, leader, yigFs)
	if err != nil {
		return
	}
	return
}

func UpdateLeaderAndZone(ctx context.Context, leader *types.GetLeaderReq, yigFs *YigFsStorage) (err error) {
	// create or update leader
	err = yigFs.MetaStorage.Client.CreateOrUpdateLeader(ctx, leader)
	if err != nil {
		log.Printf("Failed to create leader, zone_id: %s, region: %s, bucket: %s, ino: %d, leader: %s, err: %v",
			leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, leader.Machine, err)
		return
	}

	// create or update zone
	zone := &types.InitDirReq {
		Region: leader.Region,
		BucketName: leader.BucketName,
		ZoneId: leader.ZoneId,
		Machine: leader.Machine,
	}
	err = yigFs.MetaStorage.Client.CreateOrUpdateZone(ctx, zone)
	if err != nil {
		log.Printf("Failed to create or update zone, zone_id: %s, region: %s, bucket: %s, machine: %s, err: %v",
			zone.ZoneId, zone.Region, zone.BucketName, zone.Machine, err)
		return
	}
	return
}

func(yigFs *YigFsStorage) CreateOrUpdateLeader(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error) {
	err = UpdateLeaderAndZone(ctx, leader, yigFs)
	if err != nil {
		return
	}

	resp = &types.GetLeaderResp {
		LeaderInfo: &types.LeaderInfo {
			ZoneId: leader.ZoneId,
			Leader: leader.Machine,
		},
	}
	return
}
