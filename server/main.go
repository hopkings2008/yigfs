package main

import (
	"log"

	"github.com/kataras/iris"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/api"
	"github.com/hopkings2008/yigfs/server/storage"
)


func main() {
	// New ris
	app := iris.New()

	// Setup config
	helper.SetupConfig()

	// Instantiate database
	yigFsStorage := storage.New()

	// Init API
	apiServerConfig := api.ServerConfig {
		YigFsLayer: yigFsStorage,
	}
	apiHandlers := api.MetaAPIHandlers {
		YigFsAPI: apiServerConfig.YigFsLayer,
	}

	// ListDirFiles
	app.Get("/v1/dir/files", apiHandlers.GetDirFilesHandler)
	// GetDirFileAttr
	app.Get("/v1/dir/file/attr", apiHandlers.GetDirFileAttrHandler)
	// GetFileAttr
	app.Get("/v1/file/attr", apiHandlers.GetFileAttrHandler)
	// InitDir
	app.Put("/v1/dir", apiHandlers.InitDirHandler)
	// GetLeader
	app.Get("/v1/file/leader", apiHandlers.GetLeaderHandler)
	// CreateFile
	app.Put("/v1/dir/file", apiHandlers.CreateFileHandler)

	port := ":" + helper.CONFIG.MetaServiceConfig.Port
	log.Fatal(app.Run(iris.Addr(port)))
}


