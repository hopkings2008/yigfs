package meta

import (
	"github.com/hopkings2008/yigfs/server/meta/client/tidbclient"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/meta/client"
)

type Meta struct {
	Client client.Client
}

func New() *Meta {
	meta := Meta{}
	if helper.CONFIG.TidbConfig.MetaStore == "tidb" {
		meta.Client = tidbclient.NewTidbClient()
	} else {
		panic("unsupport metastore")
	}
	return &meta
}
