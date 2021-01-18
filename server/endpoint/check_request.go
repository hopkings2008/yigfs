package endpoint

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/hopkings2008/yigfs/server/types"
	. "github.com/hopkings2008/yigfs/server/error"
)

func IsRequestValid(ctx context.Context, req *http.Request) (v bool, size int64, err error) {
	// check content-length
	headers, ok := req.Header[types.HDR_CONTENT_LEN]
	if !ok || len(headers) == 0 {
		// invalid request
		return false, 0, &YigFsError{fmt.Sprintf("invalid content head length"), 400}
	}
	size, err = strconv.ParseInt(headers[0], 10, 64)
	if err != nil {
		return false, 0, &YigFsError{fmt.Sprintf("failed to convert size"), 500}
	}
	if size > types.MAX_MetaService_REQ_BODY_SIZE {
		return false, size, &YigFsError{fmt.Sprintf("body size: %i, exceed the maximum", size), 400}
	}
	return true, size, nil
}

func GetStatusCode (err error) (code int) {
	code = 500
	if find := strings.Contains(err.Error(), "statusCode: "); find {
		statusCode := strings.Split(err.Error(), "statusCode: ")[1]
		code, _ = strconv.Atoi(statusCode)
	}
	return code
}
