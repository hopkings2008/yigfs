package api

import (
	"github.com/hopkings2008/yigfs/server/storage"
)


type ServerConfig struct {
	YigFsLayer *storage.YigFsStorage
}

type MetaAPIHandlers struct {
	YigFsAPI YigFsLayer
}
