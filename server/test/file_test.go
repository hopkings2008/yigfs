package test

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/test/lib"
)


type CreateFile struct {
	CreateIno uint64
}

var testFile = CreateFile {
	CreateIno: 1,
}

func IsInitDirFilesValid(getDirFilesResp *types.GetDirFilesResp) bool {
	flag := 0
	for _, file := range getDirFilesResp.Files {
		if file.Ino == types.RootDirIno && file.FileName == "." {
			flag++
		} else if file.Ino == types.RootParentDirIno && file.FileName == ".." {
			flag++
		}
	}

	if flag == 2 {
		return true
	}

	return false
}

func Test_InitDir(t *testing.T) {
	sc := NewClient()
	newServer := Endpoint + "/v1/dir"

	initDirReq := &types.InitDirReq {
		Region: Region,
		BucketName: BucketName,
		ZoneId: ZoneId,
		Machine: Machine,
	}

	reqStr, err := json.Marshal(initDirReq)
	if err != nil {
		t.Fatal("failed to marshal initDirReq, err:", err)
	}

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	if err != nil {
		t.Fatal("failed to send Test_InitDir http/2 request, err:", err)
	}
	defer resp.Close()

	var initDirResp types.NonBodyResp
	result, _ := ioutil.ReadAll(resp)

	if err = json.Unmarshal(result, &initDirResp); err != nil {
		t.Fatal("failed to unmarshal initDirResp, err:", err)
	}

	if initDirResp.Result.ErrCode != 0 {
		t.Fatal("Failed to init dir, resp code:, err:", initDirResp.Result.ErrCode, initDirResp.Result.ErrMsg)
	}

	// get dir files
	getDirFilesReq := &types.GetDirFilesReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: ParentIno,
		Offset: Offset,
	}

	getDirFilesResp, getDirFilesInfo, err := GetDirFiles(getDirFilesReq)
	if err != nil {
		t.Fatal("Failed to get dir files, err:", err)
	} else if getDirFilesResp.Result.ErrCode != 0 {
		t.Fatal("Failed to get dir files, resp code:, err:", getDirFilesResp.Result.ErrCode, getDirFilesResp.Result.ErrMsg)
	} else {
		t.Log("Succeed to get dir files, resp:", getDirFilesInfo)
	}

	if IsInitDirFilesValid(getDirFilesResp) {
		t.Log("Succeed to init dir, resp:", string(result))
	} else {
		t.Fatal("Failed to init dir.")
	}
}

func Test_CreateFile(t *testing.T) {
	sc := NewClient()
	newServer := Endpoint + "/v1/dir/file"
	createFileReq := &types.CreateFileReq{
		ZoneId:     ZoneId,
		Region:     Region,
		BucketName: BucketName,
		ParentIno:  ParentIno,
		FileName:   FileName,
		Size:       Size,
		Type:       types.COMMON_FILE,
		Perm:       types.FILE_PERM,
		Nlink:      Nlink,
	}

	reqStr, err := json.Marshal(createFileReq)
	if err != nil {
		t.Fatal("failed to marshal createFileReq, err:", err)
	}

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	if err != nil {
                t.Fatal("failed to send Test_CreateFile http/2 request, err:", err)
        }
	defer resp.Close()

	var createFileResp types.CreateFileResp
	result, _ := ioutil.ReadAll(resp)

	if err = json.Unmarshal(result, &createFileResp); err != nil {
		t.Fatal("failed to unmarshal createFileResp, err:", err)
	}

	switch createFileResp.Result.ErrCode {
	case 0:
		s := reflect.ValueOf(&testFile.CreateIno)
		s.Elem().SetUint(createFileResp.File.Ino)
		t.Log("Succeed to create new file, createFileResp:", string(result))
	case 40011:
		t.Log("The file already existed, createFileResp:", string(result))

                s := reflect.ValueOf(&testFile.CreateIno)
                s.Elem().SetUint(createFileResp.File.Ino)

		// get segments
		getSegmentReq := &types.GetSegmentReq {
			Region:     Region,
			BucketName: BucketName,
			Ino:        createFileResp.File.Ino,
			Generation: Generation,
			Offset:     0,
			Size:       0,
		}
		
		getSegResp, getSegInfo, err := GetSegmentInfo(getSegmentReq)
		if err != nil {
			t.Fatal("Failed to get segment info, err:", err)
		} else if getSegResp.Result.ErrCode != 0 {
			t.Fatal("Failed to get segment info, resp code:, err:", getSegResp.Result.ErrCode, getSegResp.Result.ErrMsg)
		} else {
			t.Log("Succeed to get segment info, getSegmentsResp:", getSegInfo)
		}
	default:
		t.Fatal("Failed to create file, resp code:, err:", createFileResp.Result.ErrCode, createFileResp.Result.ErrMsg)
	}
}

func Test_OpenFile(t *testing.T) {
	// get the file attr
	getFilesReq := &types.GetFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		Ino: testFile.CreateIno,
	}

	getFileAttrResp, getFileAttrInfo, err := GetFileAttr(getFilesReq)
	if err != nil {
		t.Fatal("Failed to get file attr, err:", err)
	} else if getFileAttrResp.Result.ErrCode != 0 {
		t.Fatal("Failed to get file attr, resp code:, err:", getFileAttrResp.Result.ErrCode, getFileAttrResp.Result.ErrMsg)
	} else {
		t.Log("Succeed to get file attr, getFileAttrResp:", getFileAttrInfo)
	}

	// get file leader
	getLeaderReq := &types.GetLeaderReq {
		Region: Region,
		BucketName: BucketName,
		ZoneId: ZoneId,
		Ino: testFile.CreateIno,
	}

	getFileLeaderResp, getFileLeaderInfo, err := GetFileLeader(getLeaderReq)
	if err != nil {
		t.Fatal("Failed to get file leader, err:", err)
	} else if getFileLeaderResp.Result.ErrCode != 0 {
		t.Fatal("Failed to get file leader, resp code:, err:", getFileLeaderResp.Result.ErrCode, getFileLeaderResp.Result.ErrMsg)
	} else {
		t.Log("Succeed to get file leader, getFileLeaderResp:", getFileLeaderInfo)
	}

	// get segments
	getSegmentReq := &types.GetSegmentReq{
		Region:     Region,
		BucketName: BucketName,
		Ino:        testFile.CreateIno,
		Generation: Generation,
		Offset:     0,
		Size:       0,
	}

	getSegResp, getSegInfo, err := GetSegmentInfo(getSegmentReq)
	if err != nil {
		t.Fatal("Failed to get segment info, err:", err)
	} else if getSegResp.Result.ErrCode != 0 {
		t.Fatal("Failed to get segment info, resp code:, err:", getSegResp.Result.ErrCode, getSegResp.Result.ErrMsg)
	} else {
		t.Log("Succeed to get segment info, getSegmentsResp:", getSegInfo)
	}
}

func Test_WriteFile(t *testing.T) {
	// upload block
	block := &types.BlockInfo {
		Offset: Offset,
		SegStartAddr: SegStartAddr,
		SegEndAddr: SegEndAddr,
		Size: Size,
	}
	segment := &types.OneSegmentInfo {
		SegmentId0: SegmentId0,
		SegmentId1: SegmentId1,
		Block: *block,
	}
	createSegmentReq := &types.CreateSegmentReq {
		Region: Region,
		BucketName: BucketName,
		ZoneId: ZoneId,
		Machine: Machine,
		Ino: testFile.CreateIno,
		Generation: Generation,
		Segment : segment,
	}

	createSegResp, createSegInfo, err := PutSegmentInfo(createSegmentReq)
	if err != nil {
		t.Fatal("Failed to upload block, err:", err)
	} else if createSegResp.Result.ErrCode != 0 {
		t.Fatal("Failed to upload block, resp code:, err:", createSegResp.Result.ErrCode, createSegResp.Result.ErrMsg)
	}
	
	// get segments
	getSegmentReq := &types.GetSegmentReq{
		Region:     Region,
		BucketName: BucketName,
		Ino:        testFile.CreateIno,
		Generation: Generation,
		Offset:     0,
		Size: 0,
	}

	getSegResp, getSegInfo, err := GetSegmentInfo(getSegmentReq)
	if err != nil {
		t.Fatal("Failed to get segment info, err:", err)
	} else if getSegResp.Result.ErrCode != 0 {
		t.Fatal("Failed to get segment info, resp code:, err:", getSegResp.Result.ErrCode, getSegResp.Result.ErrMsg)
	} else {
		t.Log("Succeed to get segment info, result:", getSegInfo)
	}
	
	if len(getSegResp.Segments) != 0 {
		t.Log("Succeed to upload blocks, createSegResp:", createSegInfo)
	} else {
		t.Fatal("Failed to upload block.")
	}
}

func Test_SetFileAttr(t *testing.T) {
	sc := NewClient()
	newServer := Endpoint + "/v1/file/attr"

	file := &types.SetFileAttrInfo{
		Ino: testFile.CreateIno,
		Size: Size,
		Perm: UpdatePerm,
		Uid : UpdateUid,
		Gid: 0,
		Blocks: 0,
	}

	setFileAttrReq := &types.SetFileAttrReq {
		Region: Region,
		BucketName: BucketName,
		File: file,
	}

	reqStr, err := json.Marshal(setFileAttrReq)
	if err != nil {
		t.Fatal("failed to marshal setFileAttrReq, err:", err)
	}

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	if err != nil {
                t.Fatal("failed to send Test_SetFileAttr http/2 request, err:", err)
        }
	defer resp.Close()

	var setFileAttrResp types.SetFileAttrResp
	result, _ := ioutil.ReadAll(resp)
	

	if err = json.Unmarshal(result, &setFileAttrResp); err != nil {
		t.Fatal("failed to unmarshal setFileAttrResp, err:", err)
	}

	if setFileAttrResp.Result.ErrCode != 0 {
		t.Fatal("Failed to set file attr, resp code:, err:", setFileAttrResp.Result.ErrCode, setFileAttrResp.Result.ErrMsg)
	}

	if setFileAttrResp.File.Uid != UpdateUid || setFileAttrResp.File.Perm != UpdatePerm {
		t.Fatal("Failed to update file attr, the parameters is not updated.")
	}

	t.Log("Succeed to set file attr, result:", string(result))
}
