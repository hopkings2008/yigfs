package asynctasks

import (
	"fmt"

	"github.com/hopkings2008/yigfs/server/asynctasks/tasks"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/helper"
)


func RunTasks() {
	delete_blocks := &tasks.DeleteBlocksTask {
		GroupId: types.DeleteBlocksGroup,
		Topic: types.DeleteBlocksTopic,
	}

	err := delete_blocks.Start()
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to start delete blocks task, err: %v", err))
		panic("Failed to start delete blocks task!")
	}

	delete_blocks.Run()
}