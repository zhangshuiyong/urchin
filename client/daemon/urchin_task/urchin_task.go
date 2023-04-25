package urchin_task

import (
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"github.com/gin-gonic/gin"
	"net/http"
)

type UrchinTask struct {
	ID string
}

func GetTask(ctx *gin.Context) {
	var params UrchinTaskParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var (
		taskID = params.ID
	)

	//ToDo: Check peerID validation
	logger.Infof("get task id: %s %#v", taskID)

	ctx.JSON(http.StatusOK, gin.H{
		"status_code": 0,
		"status_msg":  "",
		"id":          taskID,
	})
}
