package urchin_task

import (
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"github.com/gin-gonic/gin"
	"net/http"
)

type UrchinTaskManager struct {
	peerTaskManager peer.TaskManager
}

func NewUrchinTaskManager(peerTaskManager peer.TaskManager) *UrchinTaskManager {
	return &UrchinTaskManager{
		peerTaskManager: peerTaskManager,
	}
}

func (urtm *UrchinTaskManager) GetTask(ctx *gin.Context) {
	var taskParams UrchinTaskParams
	if err := ctx.ShouldBindUri(&taskParams); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	// Check scheduler if other peers hold the task
	task, err := urtm.peerTaskManager.StatTask(ctx, taskParams.TaskID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	// Task available for download only if task is in succeeded state and has available peer
	ctx.JSON(http.StatusOK, gin.H{
		"status_code":         0,
		"status_msg":          "",
		"task_id":             task.Id,
		"task_state":          task.State,
		"task_content_length": task.ContentLength,
	})
	return
}
