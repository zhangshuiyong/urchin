package urchin_task

type UrchinTaskParams struct {
	TaskID string `uri:"task_id" binding:"required"`
}
