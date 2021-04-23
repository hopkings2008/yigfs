package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/hopkings2008/yigfs/server/api"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/log"
	"github.com/hopkings2008/yigfs/server/storage"
	"github.com/kataras/iris"
)


func DumpStacks() {
	buf := make([]byte, 1<<16)
	stackLen := runtime.Stack(buf, true)
	helper.Logger.Error(nil, "Received SIGQUIT, goroutine dump")
	helper.Logger.Error(nil, buf[:stackLen])
	helper.Logger.Error(nil, "*** dump end")
}

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
	// UpdateSegBlockInfo
	app.Put("/v1/segment/block", apiHandlers.UpdateSegBlockInfoHandler)
	// HeartBeat
	app.Get("/v1/machine/heartbeat", apiHandlers.HeartBeatHandler)

	port := ":" + helper.CONFIG.MetaServiceConfig.Port
	isHTTP2 := false
    
	go func() {
		var err error
		if isHTTP2 {
			err = app.Run(iris.TLS(port, helper.CONFIG.MetaServiceConfig.TlsCertFile, helper.CONFIG.MetaServiceConfig.TlsKeyFile))
		} else {
			err = app.Run(iris.Addr(port))
		}
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("Failed to run yigfs metaservice, err: %v", err))
		}
	}()

	// ignore signal handlers set by Iris
	signal.Ignore()
	signalQueue := make(chan os.Signal, 1)
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		case syscall.SIGUSR1:
			go DumpStacks()
		default:
			// stop Yigfs MetaService
			helper.Logger.Info(nil, "Yigfs MetaService stopped")
			return
		}
	}
}


