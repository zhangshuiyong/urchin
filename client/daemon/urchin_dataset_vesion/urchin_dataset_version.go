package urchin_dataset_vesion

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

type UrchinDataSetVersion struct {
	ID string
}

//ToDo: to connect redis to store urchin dataset version metadata!
//POST /api/v1/dataset/:datasetid/version
func CreateDataSetVersion(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}

//ToDo
//PATCH /api/v1/dataset/:datasetid/version/:versionid
func UpdateDataSetVersion(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}

//ToDo
//GET /api/v1/dataset/:datasetid/version/:versionid
func GetDataSetVersion(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}

//ToDo
//DELETE /api/v1/dataset/:datasetid/version/:versionid
func DeleteDataSetVersion(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}

//ToDo
//GET /api/v1/datasets/:datasetid/versions
func ListDataSetVersions(ctx *gin.Context) {
	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "not implemented"})
	return
}
