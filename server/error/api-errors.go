package error

import (
	"fmt"

	"github.com/hopkings2008/yigfs/server/types"
)


type ApiError interface {
	error
	AwsErrorCode() string
	Description() string
	HttpStatusCode() int
}

type ApiErrorStruct struct {
	AwsErrorCode   string
	Description    string
	HttpStatusCode int
}

// APIErrorCode type of error status.
type ApiErrorCode int

// Error codes, non exhaustive list
const (
	NoYigFsErr ApiErrorCode = iota
	ErrYIgFsInternalErr
	ErrYigFsInvaildParams
	ErrYigFsNoSuchFile
	ErrYigFsNotFindTargetDirFiles
	ErrYigFsMissingRequiredParams
	ErrYigFsMissingBucketname
)

var ErrorCodeResponse = map[ApiErrorCode]ApiErrorStruct{
	NoYigFsErr: {
		AwsErrorCode:   "NoYigFsErr",
                Description:    "ok.",
                HttpStatusCode: 0,
	},
	ErrYIgFsInternalErr: {
		AwsErrorCode:   "ErrYIgFsInternalErr",
		Description:    "We encountered an internal error, please try again.",
		HttpStatusCode:	40000,
	},
	ErrYigFsInvaildParams: {
		AwsErrorCode:   "ErrYigFsInvaildParams",
		Description:    "Invaild parameters.",
		HttpStatusCode:	40001,
	},
	ErrYigFsNoSuchFile: {
		AwsErrorCode:   "ErrYigFsNoSuchFile",
		Description:    "The specified file does not exist.",
		HttpStatusCode: 40002,
	},
	ErrYigFsNotFindTargetDirFiles: {
		AwsErrorCode:   "ErrYigFsNotFindTargetDirFiles",
		Description:    "Not find files in the target dir, please check parameters and offset.",
		HttpStatusCode: 40003,
	},
	ErrYigFsMissingRequiredParams: {
		AwsErrorCode:	"ErrYigFsMissingRequiredParams",
		Description:	"Missing some required params.",
		HttpStatusCode: 40004,
	},
	ErrYigFsMissingBucketname: {
		AwsErrorCode:   "ErrYigFsMissingRequiredParams",
                Description:    "Missing necessary parameter bucketname.",
                HttpStatusCode: 40005,
	},
}

func (e ApiErrorCode) AwsErrorCode() string {
	awsError, ok := ErrorCodeResponse[e]
	if !ok {
		return "InternalError"
	}
	return awsError.AwsErrorCode
}

func (e ApiErrorCode) Description() string {
	awsError, ok := ErrorCodeResponse[e]
	if !ok {
		return "We encountered an internal error, please try again."
	}
	return awsError.Description
}

func (e ApiErrorCode) Error() string {
	return e.Description()
}

func (e ApiErrorCode) HttpStatusCode() int {
	awsError, ok := ErrorCodeResponse[e]
	if !ok {
		return 40001
	}
	return awsError.HttpStatusCode
}

func GetErrInfo(err error) (resp types.YigFsMetaError) {
	apiErrorCode, ok := err.(ApiError)
	if ok {
		resp = types.YigFsMetaError {
			ErrCode: apiErrorCode.HttpStatusCode(),
			ErrMsg:  apiErrorCode.Description(),
		}
	} else {
		resp = types.YigFsMetaError {
			ErrCode: 40000,
			ErrMsg:  fmt.Sprintf("We encountered an internal error, please try again."),
		}
	}
	return
}

