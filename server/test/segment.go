package test

import (
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/test/lib"
)


func PutSegmentInfo(createSegmentReq *types.CreateSegmentReq) (createSegResp *types.NonBodyResp, result string, err error) {
	createSegResp = &types.NonBodyResp{}
	sc := NewClient()
	newServer := Endpoint + "/v1/file/block"

	reqStr, err := json.Marshal(createSegmentReq)
	if err != nil {
		return createSegResp, "", err
	}

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	defer resp.Close()

	createSegInfo, err := ioutil.ReadAll(resp)
	if err != nil {
		return createSegResp, "", err
	}

	if err = json.Unmarshal(createSegInfo, &createSegResp); err != nil {
		return createSegResp, "", err
	}

	return createSegResp, string(createSegInfo), nil
}

func GetSegmentInfo(getSegmentReq *types.GetSegmentReq) (getSegmentInfoResp *types.GetSegmentResp, result string, err error) {
	getSegmentInfoResp = &types.GetSegmentResp{}
	sc := NewClient()
	newServer := Endpoint + "/v1/file/segments"

	reqStr, err := json.Marshal(getSegmentReq)
	if err != nil {
		return getSegmentInfoResp, "", err
	}

	var resp io.ReadCloser
	resp, err = SendHttpToYigFs("GET", newServer, sc, reqStr)
	defer resp.Close()

	getSegInfo, err := ioutil.ReadAll(resp)
	if err != nil {
		return getSegmentInfoResp, "", err
	}

	if err = json.Unmarshal(getSegInfo, &getSegmentInfoResp); err != nil {
		return getSegmentInfoResp, "", err
	}

	return getSegmentInfoResp, string(getSegInfo), nil
}