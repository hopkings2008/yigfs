package main

import (
	"log"

	"github.com/kataras/iris"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/api"
	"github.com/hopkings2008/yigfs/server/storage"
	"github.com/hopkings2008/yigfs/server/types"
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

	// Add root dir
	rootDir := &types.FileInfo{
		Ino: 1,
		FileName: ".",
		Type: types.DIR_FILE,
	}
	err := yigFsStorage.MetaStorage.Client.CreateAndUpdateRootDir(nil, rootDir)
	if err != nil {
		log.Fatal("init dir to tidb failed, err:", err)
		return
	}

	// ListDirFiles
	app.Post("/v1/dir/files", apiHandlers.GetDirFilesHandler)
	// CreateFile
	app.Post("/v1/dir/file/create", apiHandlers.CreateFileHandler)
	//GetDirFileAttr
	app.Post("/v1/dir/file/attr", apiHandlers.GetDirFileAttrHandler)
	//GetFileAttr
	app.Post("/v1/file/attr", apiHandlers.GetFileAttrHandler)

	port := ":" + helper.CONFIG.MetaServiceConfig.Port
	log.Fatal(app.Run(iris.Addr(port)))
}


