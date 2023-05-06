package urchin_dataset

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

type UrchinDataSet struct {
	ID string
}

//ToDo: to connect redis to store urchin dataset metadata!
//POST /api/v1/dataset
func CreateDataSet(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}

//ToDo
//PATCH /api/v1/dataset/:datasetid
func UpdateDataSet(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}

//ToDo
//GET /api/v1/dataset/:datasetid
func GetDataSet(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}

//ToDo
//GET /api/v1/datasets
func ListDataSets(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}

//ToDo
//DELETE /api/v1/dataset/:datasetid
func DeleteDataSet(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}
