package meta

import (
	"context"

	"github.com/hopkings2008/yigfs/server/types"
)


func(m *Meta) CreateAndUpdateZone(ctx context.Context, zone *types.InitDirReq) (err error) {
	return m.Client.CreateOrUpdateZone(ctx, zone)
}

func(m *Meta) GetOneUpMachine(ctx context.Context, zone *types.GetLeaderReq) (leader string, err error) {
	return m.Client.GetOneUpMachine(ctx, zone)
}

func(m *Meta) GetMachineInfo(ctx context.Context, zone *types.GetLeaderReq) (resp *types.GetMachineInfoResp, err error) {
	return m.Client.GetMachineInfo(ctx, zone)
}
