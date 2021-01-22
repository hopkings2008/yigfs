package api

import (
        "time"
	"log"
)


func GetSpendTime(action string) func() {
	start := time.Now().UTC().UnixNano()
        return func() {
                end := time.Now().UTC().UnixNano()
                log.Printf(action + "cost time: %v", end - start)
        }
}
