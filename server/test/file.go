package test

import (
        "encoding/json"
        "io/ioutil"

        "github.com/hopkings2008/yigfs/server/types"
        . "github.com/hopkings2008/yigfs/server/test/lib"
)


func GetDirFiles(getDirFilesReq *types.GetDirFilesReq) (getDirFilesResp *types.GetDirFilesResp, result string, err error) {
        getDirFilesResp = &types.GetDirFilesResp{}
        sc := NewClient()
        newServer := Endpoint + "/v1/dir/files"

        reqStr, err := json.Marshal(getDirFilesReq)
        if err != nil {
                return getDirFilesResp, "", err
        }

        resp, err := SendHttpToYigFs("GET", newServer, sc, reqStr)
        defer resp.Close()

        getDirFilesInfo, err := ioutil.ReadAll(resp)
        if err != nil {
                return getDirFilesResp, "", err
        }

        if err = json.Unmarshal(getDirFilesInfo, &getDirFilesResp); err != nil {
                return getDirFilesResp, "", err
        }

        return getDirFilesResp, string(getDirFilesInfo), nil
}

func GetDirFileAttr(getDirFileReq *types.GetDirFileInfoReq) (getDirFileResp *types.GetFileInfoResp, result string, err error) {
        getDirFileResp = &types.GetFileInfoResp{}
        sc := NewClient()
        newServer := Endpoint + "/v1/dir/file/attr"

        reqStr, err := json.Marshal(getDirFileReq)
        if err != nil {
                return getDirFileResp, "", err
        }

        resp, err := SendHttpToYigFs("GET", newServer, sc, reqStr)
        defer resp.Close()

        getDirFileInfo, err := ioutil.ReadAll(resp)
        if err != nil {
                return getDirFileResp, "", err
        }

        if err = json.Unmarshal(getDirFileInfo, &getDirFileResp); err != nil {
                return getDirFileResp, "", err
        }

        return getDirFileResp, string(getDirFileInfo), nil
}

func GetFileAttr(getFileReq *types.GetFileInfoReq) (getFileAttrResp *types.GetFileInfoResp, result string, err error) {
        getFileAttrResp = &types.GetFileInfoResp{}
        sc := NewClient()
        newServer := Endpoint + "/v1/file/attr"

        reqStr, err := json.Marshal(getFileReq)
        if err != nil {
                return getFileAttrResp, "", err
        }

        resp, err := SendHttpToYigFs("GET", newServer, sc, reqStr)
        defer resp.Close()

        var getFileInfoResp types.GetFileInfoResp
        fileAttrRespInfo, err := ioutil.ReadAll(resp)
        if err != nil {
                return getFileAttrResp, "", err
        }

        if err = json.Unmarshal(fileAttrRespInfo, &getFileInfoResp); err != nil {
                return getFileAttrResp, "", err
        }

        return getFileAttrResp, string(fileAttrRespInfo), nil
}

func GetFileLeader(getLeaderReq *types.GetLeaderReq) (getFileLeaderResp *types.GetLeaderResp, result string, err error) {
	getFileLeaderResp = &types.GetLeaderResp{}
	sc := NewClient()
	newServer := Endpoint + "/v1/file/leader"

	reqStr, err := json.Marshal(getLeaderReq)
	if err != nil {
		return getFileLeaderResp, "", err
	}

	resp, err := SendHttpToYigFs("GET", newServer, sc, reqStr)
	defer resp.Close()

	var getLeaderResp types.GetLeaderResp
	getFileLeaderInfo, _ := ioutil.ReadAll(resp)

	if err = json.Unmarshal(getFileLeaderInfo, &getLeaderResp); err != nil {
		return getFileLeaderResp, "", err
	}

	return getFileLeaderResp, string(getFileLeaderInfo), nil
}
