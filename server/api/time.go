package api

import (
	"time"
	"fmt"

	"github.com/hopkings2008/yigfs/server/helper"
)


func GetSpendTime(action string) func() {
	start := time.Now().UTC().UnixNano()
	return func() {
		end := time.Now().UTC().UnixNano()
		helper.Logger.Info(nil, fmt.Sprintf(action + " cost time: %v", end - start))
	}
}
