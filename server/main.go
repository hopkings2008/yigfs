package main

import (
	"net/http"
	"os"
	"log"

	"golang.org/x/net/http2"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/api"
	"github.com/hopkings2008/yigfs/server/storage"
	"github.com/hopkings2008/yigfs/server/types"
)


func checkErr(err error, msg string) {
	if err == nil {
		log.Println(msg)
		return
	}
	log.Fatal("ListenAndServeTLS failed, err:", err)
	os.Exit(1)
}

func main() {
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
	rootDir := &types.CreateDirFileReq{
		Ino: 1,
		FileName: ".",
	}
	err := yigFsStorage.MetaStorage.Client.CreateAndUpdateRootDir(nil, rootDir)
	if err != nil {
		log.Fatal("init dir to tidb failed, err:", err)
	}

	mux := api.NewMultiplexer()
	mux.HandleFunc("/v1/dir/files", apiHandlers.GetDirFilesHandler)

	httpServer := &http.Server{
		Addr:    ":" + helper.CONFIG.MetaServiceConfig.Port,
		Handler: mux.Handler,
	}

	http2Server := &http2.Server{}
	_ = http2.ConfigureServer(httpServer, http2Server)

	checkErr(httpServer.ListenAndServeTLS(helper.CONFIG.MetaServiceConfig.TlsCertFile, helper.CONFIG.MetaServiceConfig.TlsKeyFile), "http2 listening")
}


