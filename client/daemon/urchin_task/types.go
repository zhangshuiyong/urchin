package urchin_task

type UrchinTaskParams struct {
	ID string `uri:"task_id" binding:"required"`
}
