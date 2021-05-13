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
	if err != nil {
		return getDirFilesResp, "", err
	}
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
	if err != nil {
		return getDirFileResp, "", err
	}
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
	if err != nil {
		return getFileAttrResp, "", err
	}
	defer resp.Close()

	fileAttrRespInfo, err := ioutil.ReadAll(resp)
	if err != nil {
		return getFileAttrResp, "", err
	}

	if err = json.Unmarshal(fileAttrRespInfo, &getFileAttrResp); err != nil {
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
	if err != nil {
		return getFileLeaderResp, "", err
	}
	defer resp.Close()

	getFileLeaderInfo, _ := ioutil.ReadAll(resp)

	if err = json.Unmarshal(getFileLeaderInfo, &getFileLeaderResp); err != nil {
		return getFileLeaderResp, "", err
	}

	return getFileLeaderResp, string(getFileLeaderInfo), nil
}

func PutFile(createFileReq *types.CreateFileReq) (createFileResp *types.CreateFileResp, result string, err error) {
	createFileResp = &types.CreateFileResp{}
	sc := NewClient()
	newServer := Endpoint + "/v1/dir/file"

	reqStr, err := json.Marshal(createFileReq)
	if err != nil {
		return createFileResp, "", err
	}

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	if err != nil {
		return createFileResp, "", err
	}
	defer resp.Close()

	createFileRespInfo, _ := ioutil.ReadAll(resp)

	if err = json.Unmarshal(createFileRespInfo, &createFileResp); err != nil {
		return createFileResp, "", err
	}

	return createFileResp, string(createFileRespInfo), nil
}

func DeleteFile(deleteFileReq *types.DeleteFileReq) (deleteFileResp *types.NonBodyResp, result string, err error) {
	deleteFileResp = &types.NonBodyResp{}
	sc := NewClient()
	newServer := Endpoint + "/v1/file"

	reqStr, err := json.Marshal(deleteFileReq)
	if err != nil {
		return deleteFileResp, "", err
	}

	resp, err := SendHttpToYigFs("DELETE", newServer, sc, reqStr)
	if err != nil {
		return deleteFileResp, "", err
	}
	defer resp.Close()

	deleteFileRespInfo, _ := ioutil.ReadAll(resp)

	if err = json.Unmarshal(deleteFileRespInfo, &deleteFileResp); err != nil {
		return deleteFileResp, "", err
	}

	return deleteFileResp, string(deleteFileRespInfo), nil
}
