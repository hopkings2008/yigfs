package api

import (
	"context"
	"log"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)


func CheckAndAssignmentLeaderInfo(ctx context.Context, leader *types.GetLeaderReq) (err error) {
	if leader.Flag == 1 {
		if leader.BucketName == "" || leader.ZoneId == "" || leader.Machine == "" || leader.Ino == 0 {
			log.Printf("Some getLeader required parameters are missing.")
			err = ErrYigFsMissingRequiredParams
			return
		}
	} else if leader.Flag == 0 {
		if leader.BucketName == "" || leader.ZoneId == "" || leader.Ino == 0 {
			log.Printf("Some getLeader required parameters are missing.")
			err = ErrYigFsMissingRequiredParams
			return
		}
	} else {
		log.Printf("GetLeader flag is invalid, flag: %d", leader.Flag)
		err = ErrYigFsInvalidFlag
		return
	}

	if leader.Region == "" {
		leader.Region = "cn-bj-1"
	}

	return nil
}
