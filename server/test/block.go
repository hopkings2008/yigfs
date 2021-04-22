package test

import (
	"encoding/json"
	"io/ioutil"

	. "github.com/hopkings2008/yigfs/server/test/lib"
	"github.com/hopkings2008/yigfs/server/types"
)

func UpdateSegBlockInfo(updateReq *types.UpdateSegBlockInfoReq) (updateResp *types.NonBodyResp, result string, err error) {
	updateResp = &types.NonBodyResp{}
	sc := NewClient()
	newServer := Endpoint + "/v1/segment/block"

	reqStr, err := json.Marshal(updateReq)
	if err != nil {
		return updateResp, "", err
	}

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	if err != nil {
		return updateResp, "", err
	}
	defer resp.Close()

	updateSegsInfo, err := ioutil.ReadAll(resp)
	if err != nil {
		return updateResp, "", err
	}

	if err = json.Unmarshal(updateSegsInfo, &updateResp); err != nil {
		return updateResp, "", err
	}

	return updateResp, string(updateSegsInfo), nil
}

func HeartBeat(segReq *types.GetIncompleteUploadSegsReq) (heartBeatResp *types.GetIncompleteUploadSegsResp, result string, err error) {
	heartBeatResp = &types.GetIncompleteUploadSegsResp{}
	sc := NewClient()
	newServer := Endpoint + "/v1/machine/heartbeat"

	reqStr, err := json.Marshal(segReq)
	if err != nil {
		return heartBeatResp, "", err
	}

	resp, err := SendHttpToYigFs("GET", newServer, sc, reqStr)
	if err != nil {
		return heartBeatResp, "", err
	}
	defer resp.Close()

	heartBeatInfo, err := ioutil.ReadAll(resp)
	if err != nil {
		return heartBeatResp, "", err
	}

	if err = json.Unmarshal(heartBeatInfo, &heartBeatResp); err != nil {
		return heartBeatResp, "", err
	}

	return heartBeatResp, string(heartBeatInfo), nil
}