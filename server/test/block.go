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

	if err = json.Unmarshal(updateSegsInfo, &updateReq); err != nil {
		return updateResp, "", err
	}

	return updateResp, string(updateSegsInfo), nil
}