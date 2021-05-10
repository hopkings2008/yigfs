package storage

import (
	"context"
	"fmt"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
	"github.com/hopkings2008/yigfs/server/helper"
)


func GetMachineAndUpdateFileLeader(ctx context.Context, leader *types.GetLeaderReq, yigFs *YigFsStorage) (resp *types.GetLeaderResp, err error) {
	// get a up machine from zone
	machine, err := yigFs.MetaStorage.Client.GetOneUpMachine(ctx, leader)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get one up machine, zone_id: %s, region: %s, bucket: %s, err: %v", 
			leader.ZoneId, leader.Region, leader.BucketName, err))
		return
	}

	// update leader
	leader.Machine = machine
	err = yigFs.MetaStorage.Client.CreateOrUpdateFileLeader(ctx, leader)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create leader, zone_id: %s, region: %s, bucket: %s, ino: %d, leader: %s, err: %v",
			leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, leader.Machine, err))
		return
	}

	resp = &types.GetLeaderResp {
		LeaderInfo: &types.LeaderInfo {
			ZoneId: leader.ZoneId,
			Leader: machine,
		},
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Get one up machine is: %s, zone_id is: %s", machine, leader.ZoneId))
	return
}

func CreateFileLeaderAndUpdateZone(ctx context.Context, leader *types.GetLeaderReq, yigFs *YigFsStorage) (err error) {
	// create file leader
	err = yigFs.MetaStorage.Client.CreateOrUpdateFileLeader(ctx, leader)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create leader, zone_id: %s, region: %s, bucket: %s, ino: %d, leader: %s, err: %v",
			leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, leader.Machine, err))
		return
	}

	// create or update zone
	zone := &types.InitDirReq{
		ZoneId: leader.ZoneId,
		Region: leader.Region,
		BucketName: leader.BucketName,
		Machine: leader.Machine,
	}
	
	err = yigFs.MetaStorage.Client.CreateOrUpdateZone(ctx, zone)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create or update zone, zone_id: %s, region: %s, bucket: %s, machine: %s, err: %v",
			leader.ZoneId, leader.Region, leader.BucketName, leader.Machine, err))
		return
	}
	
	return
}

func GetUpFileLeader(ctx context.Context, leader *types.GetLeaderReq, yigFs *YigFsStorage) (resp *types.GetLeaderResp, err error) {
	resp, err = yigFs.MetaStorage.Client.GetFileLeaderInfo(ctx, leader)
	switch err {
	case ErrYigFsNoSuchLeader:
		helper.Logger.Warn(ctx, fmt.Sprintf("The file does not have leader, zone_id: %s, region: %s, bucket: %s, ino: %d, starting to create it", 
			leader.ZoneId, leader.Region, leader.BucketName, leader.Ino))
		// if leader does not exist, get a up machine from zone and update leader info
		getMachineResp, err := GetMachineAndUpdateFileLeader(ctx, leader, yigFs)
		if err != nil {
			return resp, err
		}
		return getMachineResp, nil
	case nil:
		// if leader exist, determine where leader status is up
		leader.Machine = resp.LeaderInfo.Leader
		var getMachineInfoResp = &types.GetMachineInfoResp{}
		getMachineInfoResp, err = yigFs.MetaStorage.Client.GetMachineInfo(ctx, leader)
		if err != nil && err != ErrYigFsNoSuchMachine {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to get machine info, zone_id: %s, region: %s, bucket: %s, machine: %s, err: %v",
				leader.ZoneId, leader.Region, leader.BucketName, leader.Machine, err))
			return resp, err
		}

		// if status does not up or the target leader is not existed in zone, get a up machine from zone and update leader info.
		if err == ErrYigFsNoSuchMachine || getMachineInfoResp.Status != types.MachineUp {
			helper.Logger.Error(ctx, fmt.Sprintf("The file leader existed, but the status is not valid, zone_id: %s, region: %s, bucket: %s, machine: %s, err: %v, status: %v", 
				leader.ZoneId, leader.Region, leader.BucketName, leader.Machine, err, getMachineInfoResp.Status))
			getMachineResp, err := GetMachineAndUpdateFileLeader(ctx, leader, yigFs)
			if err != nil {
				return resp, err
			}
			return getMachineResp, nil
		}

		return resp, nil
	default:
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get leader, zone_id: %s, region: %s, bucket: %s, ino: %d, err: %v", 
			leader.ZoneId, leader.Region, leader.BucketName, leader.Ino, err))
		return
	}
}

func(yigFs *YigFsStorage) GetFileLeader(ctx context.Context, leader *types.GetLeaderReq) (resp *types.GetLeaderResp, err error) {
	resp, err = GetUpFileLeader(ctx, leader, yigFs)
	if err != nil {
		return
	}
	return
}

func(yigFs *YigFsStorage) CheckSegmentLeader(ctx context.Context, segment *types.CreateSegmentReq) (isExisted int, err error) {
	// get segment leader
	segLeader := &types.GetSegLeaderReq {
		ZoneId: segment.ZoneId,
		Region: segment.Region,
		BucketName: segment.BucketName,
		SegmentId0: segment.Segment.SegmentId0,
		SegmentId1: segment.Segment.SegmentId1,
	}

	getSegLeaderResp, err := yigFs.MetaStorage.Client.GetSegmentLeader(ctx, segLeader)
	switch err {
	case ErrYigFsNoSuchLeader:
		// if not segment leader, get file leader
		leader := &types.GetLeaderReq {
			ZoneId: segment.ZoneId,
			Region: segment.Region,
			BucketName: segment.BucketName,
			Ino: segment.Ino,
		}

		// get file leader
		var getFileLeaderResp = &types.GetLeaderResp{}
		getFileLeaderResp, err = GetUpFileLeader(ctx, leader, yigFs)
		if err != nil {
			return
		}

		// check request machine match leader or not
		if getFileLeaderResp.LeaderInfo.ZoneId != segment.ZoneId || getFileLeaderResp.LeaderInfo.Leader != segment.Machine {
			err = ErrYigFsMachineNotMatchLeader
		}

		isExisted = types.NotExisted
		return
	case nil:
		// if segment leader exist, check request machine match leader or not
		if getSegLeaderResp.ZoneId != segment.ZoneId || getSegLeaderResp.Leader != segment.Machine {
			err = ErrYigFsMachineNotMatchLeader
		}
		
		isExisted = types.Existed
		return
	default:
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to get segment leader, zone_id: %s, region: %s, bucket: %s, seg_id0: %d, seg_id1: %d, err: %v",
			segment.ZoneId, segment.Region, segment.BucketName, segment.Segment.SegmentId0, segment.Segment.SegmentId1, err))
		return
	}
}
