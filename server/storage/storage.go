package storage

import (
	"github.com/hopkings2008/yigfs/server/meta"
)

type YigFsStorage struct {
	MetaStorage *meta.Meta
}

func New() *YigFsStorage {
	yigFs := YigFsStorage{
		MetaStorage: meta.New(),
	}
	return &yigFs
}
