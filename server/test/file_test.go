package test

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
	"testing"

	. "github.com/hopkings2008/yigfs/server/test/lib"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/stretchr/testify/require"
)

func getBlockNumber(segs *types.GetSegmentResp) (blockNum int64) {
	for _, seg := range segs.Segments {
		blockNum += int64(len(seg.Blocks))
	}

	return
}

func Test_InitDir(t *testing.T) {
	r := require.New(t)
	sc := NewClient()
	newServer := Endpoint + "/v1/dir"

	initDirReq := &types.InitDirReq {
		Region: Region,
		BucketName: BucketName,
		ZoneId: ZoneId,
		Machine: Machine,
	}

	reqStr, err := json.Marshal(initDirReq)
	r.Nil(err)

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	r.Nil(err)

	defer resp.Close()

	var initDirResp types.NonBodyResp
	result, _ := ioutil.ReadAll(resp)

	err = json.Unmarshal(result, &initDirResp)
	r.Nil(err)
	r.Equal(initDirResp.Result.ErrCode, 0)

	// get dir files
	getDirFilesReq := &types.GetDirFilesReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: ParentIno,
		Offset: Offset,
	}

	getDirFilesResp, getDirFilesInfo, err := GetDirFiles(getDirFilesReq)
	r.Nil(err)
	r.Equal(getDirFilesResp.Result.ErrCode, 0)
	t.Logf("Succeed to get dir files, resp: %s", getDirFilesInfo)

	var flag int = 0
	for _, file := range getDirFilesResp.Files {
		if file.Ino == types.RootDirIno {
			r.Equal(file.FileName, ".")
			flag++
		} else if file.Ino == types.RootParentDirIno {
			r.Equal(file.FileName, "..")
			flag++
		}
	}

	r.Equal(flag, 2) 
	t.Logf("Succeed to init dir, resp: %s", string(result))
}

func Test_CreateFiles(t *testing.T) {
	r := require.New(t)
	createFileReq := &types.CreateFileReq{
		ZoneId:     ZoneId,
		Region:     Region,
		BucketName: BucketName,
		ParentIno:  FileParentIno,
		FileName:   FileName,
		Size:       CreateFileSize,
		Type:       types.COMMON_FILE,
		Perm:       types.FILE_PERM,
		Nlink:      Nlink,
		Machine:    Machine,
	}

	for i := 0; i < 5; i++ {
		// the first and second create the same file.
		if i >= 2 {
			createFileReq.Machine = Machine2
			createFileReq.FileName = FileName + strconv.Itoa(i)
		}

		createFileResp, createFileInfo, err := PutFile(createFileReq)
		r.Nil(err)

		if i == 1 {
			r.Equal(createFileResp.Result.ErrCode, 40011)
			t.Logf("Succeed to create already existed file, resp: %s", createFileInfo)
		} else {
			r.Equal(createFileResp.Result.ErrCode, 0)
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
		r.Nil(err)
		r.Equal(getFileLeaderResp.Result.ErrCode, 0)
		t.Logf("Test_CreateFiles Succeed to get file leader, getFileLeaderResp: %s", getFileLeaderInfo)
	}

	// get dir files
	getDirFilesReq := &types.GetDirFilesReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		Offset: Offset,
	}

	getDirFilesResp, getDirFilesInfo, err := GetDirFiles(getDirFilesReq)
	r.Nil(err)
	r.Equal(getDirFilesResp.Result.ErrCode, 0)
	t.Logf("Succeed to get dir files, resp: %s", getDirFilesInfo)

	r.Equal(len(getDirFilesResp.Files), 4)

	// get target file
	file := &types.GetDirFileInfoReq{
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName + strconv.Itoa(2),
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(file)
	r.Nil(err)
	r.Equal(getDirFileResp.Result.ErrCode, 0)
	t.Logf("Succeed to get dir file attr, resp: %s", getDirFileInfo)

	// set offset to get dir files
	getDirFilesReq.Offset = getDirFileResp.File.Ino
	getDirFilesResp, getDirFilesInfo, err = GetDirFiles(getDirFilesReq)
	r.Nil(err)
	r.Equal(getDirFilesResp.Result.ErrCode, 0)
	t.Logf("Succeed to get dir files, offset: %d, resp: %s", getDirFilesReq.Offset, getDirFilesInfo)
	r.Equal(len(getDirFilesResp.Files), 2)
}

func Test_WriteFile(t *testing.T) {
	r := require.New(t)
	// get target file
	file := &types.GetDirFileInfoReq{
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName,
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(file)
	r.Nil(err)
	r.Equal(getDirFileResp.Result.ErrCode, 0)
	t.Logf("Test_WriteFile Succeed to get dir file attr, resp: %s", getDirFileInfo)

	// upload block
	createSegmentReq := &types.CreateSegmentReq {
		Region: Region,
		BucketName: BucketName,
		ZoneId: ZoneId,
		Machine: Machine,
		Ino: getDirFileResp.File.Ino,
		Generation: Generation,
	}

	segment := &types.CreateBlocksInfo {
		SegmentId0: SegmentId0,
		SegmentId1: SegmentId1,
		MaxSize: SegMaxSize,
	}

	offset := Offset
	startAddr := SegStartAddr
	endAddr := SegEndAddr

	for i := 0; i < 3; i++ {
		block := &types.BlockInfo {
			Offset: int64(offset),
			SegStartAddr: startAddr,
			SegEndAddr: endAddr,
			Size: Size,
		}

		segment.Blocks = append(segment.Blocks, block)
		if i != 0 {
			segment.Blocks = segment.Blocks[1:]
			createSegmentReq.Segment = *segment
		} else {
			createSegmentReq.Segment = *segment
		}

		t.Logf("Ready to upload block, req: %v", createSegmentReq.Segment)

		createSegResp, createSegInfo, err := PutSegmentInfo(createSegmentReq)
		r.Nil(err)
		r.Equal(createSegResp.Result.ErrCode, 0)
		t.Logf("Succeed to upload block, resp: %s", createSegInfo)

		offset += Size 
		startAddr += Size
		endAddr += Size
	}

	segment.SegmentId0 ++
	segment.SegmentId1 ++

	for i := 0; i < 10; i++ {
		block := &types.BlockInfo {
			Offset: int64(offset),
			SegStartAddr: startAddr,
			SegEndAddr: endAddr,
			Size: Size,
		}

		segment.Blocks = append(segment.Blocks, block)
		offset += 2 * Size
		startAddr += 2 * Size
		endAddr += 2 * Size
	}

	createSegmentReq.Segment = *segment
	t.Logf("Ready to upload block, req: %v", createSegmentReq.Segment)

	createSegResp, createSegInfo, err := PutSegmentInfo(createSegmentReq)
	r.Nil(err)
	r.Equal(createSegResp.Result.ErrCode, 0)
	t.Logf("Succeed to upload block, resp: %s", createSegInfo)

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
	r.Nil(err)
	t.Logf("Succeed to get segment info, result: %s", getSegInfo)

	blockNum := getBlockNumber(getSegResp)
	r.Equal(blockNum, int64(11))
	
	// get segments when offset is not 0
	getSegmentReq.Offset = 3 * Size + int64(6)
	getSegResp, getSegInfo, err = GetSegmentInfo(getSegmentReq)
	r.Nil(err)
	r.Equal(getSegResp.Result.ErrCode, 0)
	t.Logf("Succeed to get segment info, when offset is: %d, result: %s", getSegmentReq.Offset, getSegInfo)
	
	blockNum = getBlockNumber(getSegResp)
	r.Equal(blockNum, int64(10))

	// get segments when offset and size both not 0
	getSegmentReq.Offset = Size
	getSegmentReq.Size = 12 * Size
	getSegResp, getSegInfo, err = GetSegmentInfo(getSegmentReq)
	r.Nil(err)
	r.Equal(getSegResp.Result.ErrCode, 0)
	t.Logf("Succeed to get segment info, when offset is: %d, size is: %d, result: %s", getSegmentReq.Offset, getSegmentReq.Size, getSegInfo)

	blockNum = getBlockNumber(getSegResp)
	r.Equal(blockNum, int64(5))
}

func Test_UpdateSegBlockInfo(t *testing.T) {
	r := require.New(t)

	block := &types.UpdateSegBlockInfo {
		SegmentId0: SegmentId0,
		SegmentId1: SegmentId1,
		LatestOffset: LatestedOffset,
	}

	segReq := &types.UpdateSegBlockInfoReq {
		ZoneId: ZoneId,
		Region: Region,
		BucketName: BucketName,
		SegBlockInfo: block,
	}

	updateResp, updateInfo, err := UpdateSegBlockInfo(segReq)
	r.Nil(err)
	r.Equal(updateResp.Result.ErrCode, 0)
	t.Logf("Test_UpdateSegBlockInfo: Succeed to update segment block info, resp: %s", updateInfo)
}

func Test_SetFileAttr(t *testing.T) {
	r := require.New(t)
	// get target file
	getFile := &types.GetDirFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName,
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(getFile)
	r.Nil(err)
	r.Equal(getDirFileResp.Result.ErrCode, 0)
	t.Logf("Test_SetFileAttr Succeed to get dir file attr, resp: %s", getDirFileInfo)

	sc := NewClient()
	newServer := Endpoint + "/v1/file/attr"

	var updateUid = uint32(20)
	var updatePerm = uint32(755)

	file := &types.SetFileAttrInfo{
		Ino: getDirFileResp.File.Ino,
		Perm: &updatePerm,
		Uid : &updateUid,
	}

	setFileAttrReq := &types.SetFileAttrReq {
		Region: Region,
		BucketName: BucketName,
		File: file,
	}

	reqStr, err := json.Marshal(setFileAttrReq)
	r.Nil(err)

	resp, err := SendHttpToYigFs("PUT", newServer, sc, reqStr)
	r.Nil(err)
	defer resp.Close()

	var setFileAttrResp types.SetFileAttrResp
	result, _ := ioutil.ReadAll(resp)
	err = json.Unmarshal(result, &setFileAttrResp)
	r.Nil(err)

	r.Equal(setFileAttrResp.Result.ErrCode, 0)
	r.Equal(setFileAttrResp.File.Uid, updateUid)
	r.Equal(setFileAttrResp.File.Perm, updatePerm)
	t.Logf("Succeed to set file attr, resp: %s", string(result))
}

func Test_GetFileAttr(t *testing.T) {
	r := require.New(t)
	// get target dir file attr
	getFile := &types.GetDirFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName,
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(getFile)
	r.Nil(err)
	r.Equal(getDirFileResp.Result.ErrCode, 0)
	t.Logf("Test_GetFileAttr Succeed to get dir file attr, resp: %s", getDirFileInfo)

	// get the target file attr using ino
	getFilesReq := &types.GetFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		Ino: getDirFileResp.File.Ino,
	}

	getFileAttrResp, getFileAttrInfo, err := GetFileAttr(getFilesReq)
	r.Nil(err)
	r.Equal(getFileAttrResp.Result.ErrCode, 0)
	t.Logf("Succeed to get file attr using ino, getFileAttrResp: %s", getFileAttrInfo)
}

func Test_UpdateSegments(t *testing.T) {
	r := require.New(t)
	// get target file
	file := &types.GetDirFileInfoReq{
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: FileName,
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(file)
	r.Nil(err)
	r.Equal(getDirFileResp.Result.ErrCode, 0)
	t.Logf("Test_UpdateSegments Succeed to get dir file attr, resp: %s", getDirFileInfo)

	// upload segments
	updateSegmentsReq := &types.UpdateSegmentsReq {
		Region: Region,
		BucketName: BucketName,
		ZoneId: ZoneId,
		Ino: getDirFileResp.File.Ino,
	}

	var segId0 uint64 = SegmentId0
	var segId1 uint64 = SegmentId1
	offset := UpdateOffset
	startAddr := SegStartAddr
	endAddr := SegEndAddr


	for i := 0; i < 50; i++ {
		segment := &types.CreateBlocksInfo {
			SegmentId0: segId0,
			SegmentId1: segId1,
			Leader: Machine,
			MaxSize: SegMaxSize,
		}

		block := &types.BlockInfo {
			Offset: int64(i * offset) + int64(Size),
			SegStartAddr: startAddr,
			SegEndAddr: endAddr,
			Size: Size,
		}

		segId0++
		segId1++

		segment.Blocks = append(segment.Blocks, block)
		updateSegmentsReq.Segments = append(updateSegmentsReq.Segments, segment)
	}

	updateSegsResp, updateSegsInfo, err := PutSegmentsInfo(updateSegmentsReq)
	r.Nil(err)
	r.Equal(updateSegsResp.Result.ErrCode, 0)
	t.Logf("Succeed to upload block, resp: %s", updateSegsInfo)
}

func Test_HeartBeat(t *testing.T) {
	r := require.New(t)
 	// get incomplete upload segs
	segReq := &types.GetIncompleteUploadSegsReq {
		ZoneId: ZoneId,
		Region: Region,
		BucketName: BucketName,
		Machine: Machine,
	}

	heartBeatResp, heartBeatInfo, err := HeartBeat(segReq)
	r.Nil(err)
	r.Equal(heartBeatResp.Result.ErrCode, 0)
	t.Logf("Succeed to test heart beat, resp: %s", heartBeatInfo)
}
