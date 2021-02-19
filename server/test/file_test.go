package test

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
	"testing"

	. "github.com/hopkings2008/yigfs/server/test/lib"
	"github.com/hopkings2008/yigfs/server/types"
)


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

func getBlockNumber(segs *types.GetSegmentResp) (blockNum int64) {
	for _, seg := range segs.Segments {
		blockNum += int64(len(seg.Blocks))
	}
	return
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
		t.Fatalf("failed to marshal initDirReq, err: %v", err)
	}

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	if err != nil {
		t.Fatalf("failed to send Test_InitDir http/2 request, err: %v", err)
	}
	defer resp.Close()

	var initDirResp types.NonBodyResp
	result, _ := ioutil.ReadAll(resp)

	if err = json.Unmarshal(result, &initDirResp); err != nil {
		t.Fatalf("failed to unmarshal initDirResp, err: %v", err)
	}

	if initDirResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to init dir, resp code: %d, err: %v", initDirResp.Result.ErrCode, initDirResp.Result.ErrMsg)
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
		t.Fatalf("Failed to get dir files, err: %v", err)
	} else if getDirFilesResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to get dir files, resp code: %d, err: %v", getDirFilesResp.Result.ErrCode, getDirFilesResp.Result.ErrMsg)
	} else {
		t.Logf("Succeed to get dir files, resp: %s", getDirFilesInfo)
	}

	if IsInitDirFilesValid(getDirFilesResp) {
		t.Logf("Succeed to init dir, resp: %s", string(result))
	} else {
		t.Fatalf("Failed to init dir.")
	}
}

func Test_CreateFiles(t *testing.T) {
	createFileReq := &types.CreateFileReq{
		ZoneId:     ZoneId,
		Region:     Region,
		BucketName: BucketName,
		ParentIno:  FileParentIno,
		FileName:   FileName,
		Size:       Size,
		Type:       types.COMMON_FILE,
		Perm:       types.FILE_PERM,
		Nlink:      Nlink,
	}

	for i := 0; i < 5; i++ {
		// the first and second create the same file.
		if i >= 2 {
			createFileReq.FileName = FileName + strconv.Itoa(i)
		}

		createFileResp, createFileInfo, err := PutFile(createFileReq)
		if err != nil {
			t.Fatalf("Failed to create file, err: %v", err)
		}

		if i == 1 {
			if createFileResp.Result.ErrCode != 40011 {
				t.Fatalf("Failed to create already existed file, resp: %v, resp code: %d, err: %v", createFileInfo, createFileResp.Result.ErrCode, createFileResp.Result.ErrMsg)
			}
			t.Logf("Succeed to create already existed file, resp: %s", createFileInfo)
		} else {
			if createFileResp.Result.ErrCode != 0 {
				t.Fatalf("Failed to create new file, resp code: %d, err: %v", createFileResp.Result.ErrCode, createFileResp.Result.ErrMsg)
			}
			t.Logf("Succeed to create new file, resp: %s", createFileInfo)
		}

		// get the file leader
		getLeaderReq := &types.GetLeaderReq {
			Region: Region,
			BucketName: BucketName,
			ZoneId: ZoneId,
			Ino: createFileResp.File.Ino,
		}

		getFileLeaderResp, getFileLeaderInfo, err := GetFileLeader(getLeaderReq)
		if err != nil {
			t.Fatalf("Test_CreateFiles Failed to get file leader, ino: %d, err: %v", getLeaderReq.Ino, err)
		} else if getFileLeaderResp.Result.ErrCode != 0  {
			t.Fatalf("Test_CreateFiles Failed to get file leader, resp code: %d, err: %v", getFileLeaderResp.Result.ErrCode, getFileLeaderResp.Result.ErrMsg)
		} else {
			t.Logf("Test_CreateFiles Succeed to get file leader, getFileLeaderResp: %s", getFileLeaderInfo)
		}
	}

	// get dir files
	getDirFilesReq := &types.GetDirFilesReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		Offset: Offset,
	}

	getDirFilesResp, getDirFilesInfo, err := GetDirFiles(getDirFilesReq)
	if err != nil {
		t.Fatalf("Failed to get dir files, err: %v", err)
	} else if getDirFilesResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to get dir files, resp code: %d, err: %v", getDirFilesResp.Result.ErrCode, getDirFilesResp.Result.ErrMsg)
	} else {
		t.Logf("Succeed to get dir files, resp: %s", getDirFilesInfo)
	}

	if len(getDirFilesResp.Files) != 4 {
		t.Fatalf("create files number is 4, but get dir files number is: %d", len(getDirFilesResp.Files))
	}

	// get target file
	file := &types.GetDirFileInfoReq{
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName + strconv.Itoa(2),
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(file)
	if err != nil {
		t.Fatalf("Failed to get dir file attr, err: %v", err)
	} else if getDirFileResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to get dir file attr, resp code: %d, err: %v", getDirFileResp.Result.ErrCode, getDirFileResp.Result.ErrMsg)
	} else {
		t.Logf("Succeed to get dir file attr, resp: %s", getDirFileInfo)
	}

	// set offset to get dir files
	getDirFilesReq.Offset = getDirFileResp.File.Ino

	getDirFilesResp, getDirFilesInfo, err = GetDirFiles(getDirFilesReq)
	if err != nil {
		t.Fatalf("Failed to get dir files, offset: %d, err: %v", getDirFilesReq.Offset, err)
	} else if getDirFilesResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to get dir files, resp code: %d, err: %v", getDirFilesResp.Result.ErrCode, getDirFilesResp.Result.ErrMsg)
	} else {
		t.Logf("Succeed to get dir files, offset: %d, resp: %s", getDirFilesReq.Offset, getDirFilesInfo)
	}

	if len(getDirFilesResp.Files) != 2 {
		t.Fatalf("Failed to get the target number dir files, when offset is: %d", getDirFilesReq.Offset)
	}
}

func Test_WriteFile(t *testing.T) {
	// get target file
	file := &types.GetDirFileInfoReq{
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName,
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(file)
	if err != nil {
		t.Fatalf("Test_WriteFile: Failed to get dir file attr, err:%v", err)
	} else if getDirFileResp.Result.ErrCode != 0 {
		t.Fatalf("Test_WriteFile: Failed to get dir file attr, resp code: %d, err: %v", getDirFileResp.Result.ErrCode, getDirFileResp.Result.ErrMsg)
	} else {
		t.Logf("Test_WriteFile Succeed to get dir file attr, resp: %s", getDirFileInfo)
	}

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
		Ino: getDirFileResp.File.Ino,
		Generation: Generation,
		Segment : segment,
	}

	for i := 0; i < 3; i++ {
		createSegResp, createSegInfo, err := PutSegmentInfo(createSegmentReq)
		if err != nil {
			t.Fatalf("Failed to upload block, err: %v", err)
		} else if createSegResp.Result.ErrCode != 0 {
			t.Fatalf("Failed to upload block, resp code: %d, err: %v", createSegResp.Result.ErrCode, createSegResp.Result.ErrMsg)
		} else {
			t.Logf("Succeed to upload block, resp: %s", createSegInfo)
		}

		createSegmentReq.Segment.Block.SegStartAddr += Size
		createSegmentReq.Segment.Block.Offset += Size
		createSegmentReq.Segment.Block.SegEndAddr += Size
	}

	createSegmentReq.Segment.SegmentId0 ++
        createSegmentReq.Segment.SegmentId1 ++
	for i := 0; i < 3; i++ {
        	createSegResp, createSegInfo, err := PutSegmentInfo(createSegmentReq)
                if err != nil {
                        t.Fatalf("Failed to upload block, err: %v", err)
                } else if createSegResp.Result.ErrCode != 0 {
                        t.Fatalf("Failed to upload block, resp code: %d, err: %v", createSegResp.Result.ErrCode, createSegResp.Result.ErrMsg)
                } else {
                        t.Logf("Succeed to upload block, resp: %s", createSegInfo)
                }

                createSegmentReq.Segment.Block.SegStartAddr += Size
                createSegmentReq.Segment.Block.Offset += Size
                createSegmentReq.Segment.Block.SegEndAddr += Size
        }

	// get segments
	getSegmentReq := &types.GetSegmentReq {
		ZoneId: ZoneId,
		Region:     Region,
		BucketName: BucketName,
		Ino:        getDirFileResp.File.Ino,
		Generation: Generation,
		Offset:     0,
		Size: 0,
	}

	getSegResp, getSegInfo, err := GetSegmentInfo(getSegmentReq)
	if err != nil {
		t.Fatalf("Failed to get segment info, err: %v", err)
	} else if getSegResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to get segment info, resp code: %d, err: %v", getSegResp.Result.ErrCode, getSegResp.Result.ErrMsg)
	} else {
		t.Logf("Succeed to get segment info, result: %s", getSegInfo)
	}

	blockNum := getBlockNumber(getSegResp)
	if blockNum != 6 {
		t.Fatalf("The target block numbers is 6, but get segment blocks number is: %v", blockNum)
	}
	
	// get segments when offset is not 0
	getSegmentReq.Offset = 2 * Size + int64(6)
	getSegResp, getSegInfo, err = GetSegmentInfo(getSegmentReq)
	if err != nil {
		t.Fatalf("Failed to get segment info when offset is: %d, err: %v", getSegmentReq.Offset, err)
	} else if getSegResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to get segment info when offset is: %d, resp code: %d, err: %v", getSegmentReq.Offset, getSegResp.Result.ErrCode, getSegResp.Result.ErrMsg)
	} else {
		t.Logf("Succeed to get segment info, when offset is: %d, result: %s", getSegmentReq.Offset, getSegInfo)
	}
	
	blockNum = getBlockNumber(getSegResp)
	if blockNum != 4 {
		t.Fatalf("The target block numbers is 4, but get segment blocks number is: %v", blockNum)
	}

	// get segments when offset and size both not 0
	getSegmentReq.Offset = Size
	getSegmentReq.Size = 5 * Size
	getSegResp, getSegInfo, err = GetSegmentInfo(getSegmentReq)
	if err != nil {
		t.Fatalf("Failed to get segment info when offset is: %d, size is: %d, err: %v", getSegmentReq.Offset, getSegmentReq.Size, err)
	} else if getSegResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to get segment info when offset is: %d, size is: %d, resp code: %d, err: %v",
			getSegmentReq.Offset, getSegmentReq.Size, getSegResp.Result.ErrCode, getSegResp.Result.ErrMsg)
	} else {
		t.Logf("Succeed to get segment info, when offset is: %d, size is: %d, result: %s", getSegmentReq.Offset, getSegmentReq.Size, getSegInfo)
	}

	blockNum = getBlockNumber(getSegResp)
        if blockNum != 1 {
                t.Fatalf("The target block numbers is 1, but get segment blocks number is: %v", blockNum)
        }
}

func Test_SetFileAttr(t *testing.T) {
	// get target file
	getFile := &types.GetDirFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName,
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(getFile)
	if err != nil {
		t.Fatalf("Test_SetFileAttr: Failed to get dir file attr, err: %v", err)
	} else if getDirFileResp.Result.ErrCode != 0 {
		t.Fatalf("Test_SetFileAttr: Failed to get dir file attr, resp code: %d, err: %v", getDirFileResp.Result.ErrCode, getDirFileResp.Result.ErrMsg)
	} else {
		t.Logf("Test_SetFileAttr Succeed to get dir file attr, resp: %s", getDirFileInfo)
	}

	sc := NewClient()
	newServer := Endpoint + "/v1/file/attr"

	file := &types.SetFileAttrInfo{
		Ino: getDirFileResp.File.Ino,
		Size: Size,
		Perm: UpdatePerm,
		Uid : UpdateUid,
	}

	setFileAttrReq := &types.SetFileAttrReq {
		Region: Region,
		BucketName: BucketName,
		File: file,
	}

	reqStr, err := json.Marshal(setFileAttrReq)
	if err != nil {
		t.Fatalf("failed to marshal setFileAttrReq, err: %v", err)
	}

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	if err != nil {
		t.Fatalf("failed to send Test_SetFileAttr http/2 request, err: %v", err)
	}
	defer resp.Close()

	var setFileAttrResp types.SetFileAttrResp
	result, _ := ioutil.ReadAll(resp)
	

	if err = json.Unmarshal(result, &setFileAttrResp); err != nil {
		t.Fatalf("failed to unmarshal setFileAttrResp, err: %v", err)
	}

	if setFileAttrResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to set file attr, resp code: %d, err: %v", setFileAttrResp.Result.ErrCode, setFileAttrResp.Result.ErrMsg)
	}

	if setFileAttrResp.File.Uid != UpdateUid || setFileAttrResp.File.Perm != UpdatePerm {
		t.Fatalf("Failed to update file attr, the parameters is not updated.")
	}

	t.Logf("Succeed to set file attr, resp: %s", string(result))
}

func Test_GetFileAttr(t *testing.T) {
	// get target dir file attr
	getFile := &types.GetDirFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName,
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(getFile)
	if err != nil {
		t.Fatalf("Test_SetFileAttr: Failed to get dir file attr, err: %v", err)
	} else if getDirFileResp.Result.ErrCode != 0 {
		t.Fatalf("Test_SetFileAttr: Failed to get dir file attr, resp code: %d, err: %v", getDirFileResp.Result.ErrCode, getDirFileResp.Result.ErrMsg)
	} else {
		t.Logf("Test_SetFileAttr Succeed to get dir file attr, resp: %s", getDirFileInfo)
	}

	// get the target file attr using ino
	getFilesReq := &types.GetFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		Ino: getDirFileResp.File.Ino,
	}

	getFileAttrResp, getFileAttrInfo, err := GetFileAttr(getFilesReq)
	if err != nil {
		t.Fatalf("Failed to get file attr, err: %v", err)
	} else if getFileAttrResp.Result.ErrCode != 0 {
		t.Fatalf("Failed to get file attr, resp code: %d, err: %v", getFileAttrResp.Result.ErrCode, getFileAttrResp.Result.ErrMsg)
	} else {
		t.Logf("Succeed to get file attr using ino, getFileAttrResp: %s", getFileAttrInfo)
	}
}

