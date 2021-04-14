package main

import (
	"fmt"

	"github.com/hopkings2008/yigfs/server/api"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/log"
	"github.com/hopkings2008/yigfs/server/storage"
	"github.com/kataras/iris"
)


func main() {
	// New ris
	app := iris.New()

	// Setup config
	helper.SetupConfig()

	// Configurate log
	logLevel := log.ParseLevel(helper.CONFIG.MetaServiceConfig.LogLevel)
	helper.Logger = log.NewFileLogger(helper.CONFIG.MetaServiceConfig.LogDir, logLevel)
	defer helper.Logger.Close()

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
	// GetFileLeader
	app.Get("/v1/file/leader", apiHandlers.GetFileLeaderHandler)
	// CreateFile
	app.Put("/v1/dir/file", apiHandlers.CreateFileHandler)
	// SetFileAttr
	app.Put("/v1/file/attr", apiHandlers.SetFileAttrHandler)
	// CreateSegment
	app.Put("/v1/file/block", apiHandlers.CreateSegmentHandler)
	// UpdateSegments
	app.Put("/v1/file/segments", apiHandlers.UpdateSegmentsHandler)
	// GetSegments
	app.Get("/v1/file/segments", apiHandlers.GetSegmentsHandler)

	port := ":" + helper.CONFIG.MetaServiceConfig.Port
    	err := app.Run(iris.TLS(port, helper.CONFIG.MetaServiceConfig.TlsCertFile, helper.CONFIG.MetaServiceConfig.TlsKeyFile))
	//err := app.Run(iris.Addr(port))
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to run yigfs, err: %v", err))
	}
}


