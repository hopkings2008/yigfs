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
		Capacity: Capacity,
	}

	offset := Offset
	startAddr := SegStartAddr
	for i := 0; i < 10; i++ {
		block := types.BlockInfo {
			Offset: int64(offset),
			SegStartAddr: startAddr,
			Size: Size,
		}
		offset += 2 * Size
		startAddr += 2 * Size
		t.Logf("block info: %v", block)
		segment.Blocks = append(segment.Blocks, &block)
	}

	createSegmentReq.Segment = *segment
	t.Logf("Ready to upload block, req: %v", createSegmentReq.Segment)

	createSegResp, createSegInfo, err := PutSegmentInfo(createSegmentReq)
	r.Nil(err)
	r.Equal(createSegResp.Result.ErrCode, 0)
	t.Logf("Succeed to upload block, resp: %s", createSegInfo)
	
	// get segments
	getSegmentReq := &types.GetSegmentReq {
		ZoneId: ZoneIdNew,
		Region:     Region,
		BucketName: BucketName,
		Ino:        getDirFileResp.File.Ino,
		Generation: Generation,
		Machine: Machine2,
		Offset:     0,
		Size: 0,
	}
	getSegmentReq.ZoneId = ZoneId
	getSegmentReq.Machine = Machine
	getSegResp, getSegInfo, err := GetSegmentInfo(getSegmentReq)
	r.Nil(err)
	r.Equal(getSegResp.Result.ErrCode, 0)
	t.Logf("Succeed to get segment info, result: %s", getSegInfo)

	blockNum := getBlockNumber(getSegResp)
	r.Equal(blockNum, int64(10))
	
	// get segments when offset is not 0
	getSegmentReq.Offset = 3 * Size + int64(6)
	getSegResp, getSegInfo, err = GetSegmentInfo(getSegmentReq)
	r.Nil(err)
	r.Equal(getSegResp.Result.ErrCode, 0)
	t.Logf("Succeed to get segment info, when offset is: %d, result: %s", getSegmentReq.Offset, getSegInfo)
	
	blockNum = getBlockNumber(getSegResp)
	r.Equal(blockNum, int64(8))

	// get segments when offset and size both not 0
	getSegmentReq.Offset = Size
	getSegmentReq.Size = 12 * Size
	getSegResp, getSegInfo, err = GetSegmentInfo(getSegmentReq)
	r.Nil(err)
	r.Equal(getSegResp.Result.ErrCode, 0)
	t.Logf("Succeed to get segment info, when offset is: %d, size is: %d, result: %s", getSegmentReq.Offset, getSegmentReq.Size, getSegInfo)

	blockNum = getBlockNumber(getSegResp)
	r.Equal(blockNum, int64(3))
}

func Test_GetSegmentsForNewFile(t *testing.T) {
	r := require.New(t)
	
	// get target file
	file := &types.GetDirFileInfoReq{
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: "test.txt2",
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(file)
	r.Nil(err)
	r.Equal(getDirFileResp.Result.ErrCode, 0)
	t.Logf("Test_GetSegmentsForNewFile Succeed to get dir file attr, resp: %s", getDirFileInfo)

	// get segments
	getSegmentReq := &types.GetSegmentReq {
		ZoneId: ZoneId,
		Region:     Region,
		BucketName: BucketName,
		Ino:        getDirFileResp.File.Ino,
		Generation: Generation,
		Machine: Machine,
		Offset:     0,
		Size: 0,
	}
	
	getSegResp, getSegInfo, err := GetSegmentInfo(getSegmentReq)
	r.Nil(err)
	r.Equal(getSegResp.Result.ErrCode, 0)
	t.Logf("Succeed to Test_GetSegmentsForNewFile, result: %s", getSegInfo)
}

func Test_UpdateSegBlockInfo(t *testing.T) {
	r := require.New(t)

	block := &types.UpdateSegBlockInfo {
		SegmentId0: SegmentId0,
		SegmentId1: SegmentId1,
		BackendSize: LatestedOffset,
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

	for j := 0; j < 10; j++ {
		segment := types.CreateBlocksInfo {
			SegmentId0: segId0,
			SegmentId1: segId1,
			ZoneId: ZoneId,
			Leader: Machine,
			Capacity: Capacity,
		}

		for i := 0; i < 6000; i++ {
			block := types.BlockInfo {
				Offset: int64(offset),
				SegStartAddr: startAddr,
				Size: Size,
			}
			offset += 2 * Size
			startAddr += Size
			segment.Blocks = append(segment.Blocks, &block)
		}

		segId0++
		segId1++
		updateSegmentsReq.Segments = append(updateSegmentsReq.Segments, &segment)
	}

	removeSegments := make([]*types.CreateBlocksInfo, 0)
	offset = 0
	startAddr = 0
	segId0 = SegmentId0
	segId1 = SegmentId1

	for j := 0; j < 5; j++ {
		removeSegment := types.CreateBlocksInfo {
			SegmentId0: segId0,
			SegmentId1: segId1,
			Leader: Machine,
		}

		for i := 0; i < 1000; i++ {
			block := types.BlockInfo {
				Offset: int64(offset),
				SegStartAddr: startAddr,
				Size: Size,
			}
			offset += 2 * Size
			startAddr += Size
			removeSegment.Blocks = append(removeSegment.Blocks, &block)
		}

		segId0++
		segId1++
		removeSegments = append(removeSegments, &removeSegment)
	}
	
	updateSegmentsReq.RemoveSegments = removeSegments
	updateSegsResp, updateSegsInfo, err := PutSegmentsInfo(updateSegmentsReq)
	r.Nil(err)
	r.Equal(updateSegsResp.Result.ErrCode, 0)
	t.Logf("Succeed to upload block, resp: %s, updateSegmentsReq: %v", updateSegsInfo, updateSegmentsReq.Segments)
}

func Test_UpdateNewFileSegments(t *testing.T) {
	r := require.New(t)
	// get target file
	file := &types.GetDirFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		ParentIno: FileParentIno,
		FileName: NewFileName,
	}

	getDirFileResp, getDirFileInfo, err := GetDirFileAttr(file)
	r.Nil(err)
	r.Equal(getDirFileResp.Result.ErrCode, 0)
	t.Logf("Test_UpdateNewFileSegments Succeed to get dir file attr, resp: %s", getDirFileInfo)

	// upload segments
	updateSegmentsReq := &types.UpdateSegmentsReq {
		Region: Region,
		BucketName: BucketName,
		ZoneId: ZoneId,
		Ino: getDirFileResp.File.Ino,
	}

	var segId0 uint64 = SegmentId0
	var segId1 uint64 = SegmentId1
	offset := 16777216
	startAddr := 16777216

	for j := 0; j < 2; j++ {
		segment := types.CreateBlocksInfo {
			SegmentId0: segId0,
			SegmentId1: segId1,
			ZoneId: ZoneId,
			Leader: Machine,
			Capacity: Capacity,
		}

		for i := 0; i < 4000; i++ {
			block := types.BlockInfo {
				Offset: int64(offset),
				SegStartAddr: startAddr,
				Size: Size,
			}
			offset += 2 * Size
			startAddr += Size
			segment.Blocks = append(segment.Blocks, &block)
		}

		segId0++
		segId1++
		updateSegmentsReq.Segments = append(updateSegmentsReq.Segments, &segment)
	}

	updateSegsResp, updateSegsInfo, err := PutSegmentsInfo(updateSegmentsReq)
	r.Nil(err)
	r.Equal(updateSegsResp.Result.ErrCode, 0)
	t.Logf("Succeed to upload block, resp: %s, Test_UpdateNewFileSegments: %v", updateSegsInfo, updateSegmentsReq.Segments)
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

func Test_DeleteFile(t *testing.T) {
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
	t.Logf("Test_DeleteFile: Succeed to get dir file attr, resp: %s", getDirFileInfo)

	// delete the target file
	deleteFileReq := &types.DeleteFileReq {
		Region: Region,
		BucketName: BucketName,
		Ino: getDirFileResp.File.Ino,
		ZoneId: ZoneId,
		Machine: Machine2,
	}

	deleteFileResp, deleteFileRespInfo, err := DeleteFile(deleteFileReq)
	r.Nil(err)
	r.Equal(deleteFileResp.Result.ErrCode, 40015)
	t.Logf("Succeed to resp err, for the machine is not the file leader, resp: %s", deleteFileRespInfo)

	deleteFileReq.Machine = Machine
	deleteFileResp, deleteFileRespInfo, err = DeleteFile(deleteFileReq)
	r.Nil(err)
	r.Equal(deleteFileResp.Result.ErrCode, 0)

	// get the target file attr using ino
	getFilesReq := &types.GetFileInfoReq {
		Region: Region,
		BucketName: BucketName,
		Ino: getDirFileResp.File.Ino,
	}

	getFileAttrResp, getFileAttrInfo, err := GetFileAttr(getFilesReq)
	r.Nil(err)
	r.Equal(getFileAttrResp.Result.ErrCode, 40002)
	t.Logf("Succeed to get the non existed file, getFileAttrResp: %s", getFileAttrInfo)

	t.Logf("Succeed to delete file, resp: %s", deleteFileRespInfo)
}
