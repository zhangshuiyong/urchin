package urchin_dataset_vesion

import (
	"d7y.io/dragonfly/v2/client/daemon/urchin_dataset"
	"d7y.io/dragonfly/v2/client/util"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/satori/go.uuid"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ToDo: to connect redis to store urchin dataset version metadata!
// POST /api/v1/dataset/:datasetid/version
func CreateDataSetVersion(ctx *gin.Context) {

	var uriParams UrchinDataSetVersionCreateUriParams
	if err := ctx.ShouldBindUri(&uriParams); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	var versionInfo UrchinDataSetVersionInfo
	if err := ctx.ShouldBind(&versionInfo); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}
	datasetId := uriParams.DATASET_ID
	logger.Infof("parsed datasetId:%s", datasetId)

	versionId := uuid.NewV4().String()
	versionInfo.CreateAt = time.Now().Unix()
	versionInfo.ID = versionId
	datasetVersionValue, err := json.Marshal(versionInfo)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}
	redisClient := util.NewRedisStorage(urchin_dataset.RedisClusterIP, "dragonfly", false)
	redisClient.SetMapElement(datasetversionPrefix+datasetId, versionId, datasetVersionValue)

	ctx.JSON(http.StatusOK, gin.H{
		"status_code": 0,
		"status_msg":  "Succeed",
		"version_id":  versionId,
	})
	return
}

// ToDo
// PATCH /api/v1/dataset/:datasetid/version/:versionid
func UpdateDataSetVersion(ctx *gin.Context) {
	var uriParams UrchinDataSetVersionUriParams
	if err := ctx.ShouldBindUri(&uriParams); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	var params UrchinDataSetVersionInfo
	if err := ctx.ShouldBind(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}
	datasetId := uriParams.DATASET_ID
	versionId := uriParams.VERSION_ID
	logger.Infof("parsed datasetId:%s, versionId:%s", datasetId, versionId)
	redisClient := util.NewRedisStorage(urchin_dataset.RedisClusterIP, "dragonfly", false)
	dvString, err := redisClient.GetMapElement(datasetversionPrefix+datasetId, versionId)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	dvInfo := &UrchinDataSetVersionInfo{}
	err = json.Unmarshal([]byte(dvString), &dvInfo)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	if params.Name != "" {
		dvInfo.Name = params.Name
	}

	if params.Desc != "" {
		dvInfo.Desc = params.Desc
	}

	if params.MetaCaches != "" {
		dvInfo.MetaCaches = params.MetaCaches
	}

	if params.MetaSources != "" {
		dvInfo.MetaSources = params.MetaSources
	}

	datasetVersionValue, err := json.Marshal(dvInfo)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	err = redisClient.SetMapElement(datasetversionPrefix+datasetId, versionId, datasetVersionValue)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status_code": 0,
		"status_msg":  "Succeed",
		"datasetId":   datasetId,
	})
	return
}

// ToDo
// GET /api/v1/dataset/:datasetid/version/:versionid
func GetDataSetVersion(ctx *gin.Context) {
	var uriParams UrchinDataSetVersionUriParams
	if err := ctx.ShouldBindUri(&uriParams); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}
	datasetId := uriParams.DATASET_ID
	versionId := uriParams.VERSION_ID
	logger.Infof("parsed datasetId:%s, versionId:%s", datasetId, versionId)

	redisClient := util.NewRedisStorage(urchin_dataset.RedisClusterIP, "dragonfly", false)
	dvString, err := redisClient.GetMapElement(datasetversionPrefix+datasetId, versionId)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	dvInfo := &UrchinDataSetVersionInfo{}
	err = json.Unmarshal([]byte(dvString), &dvInfo)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	//dataset_name := "imagenet-10k"
	//dataset_desc := "数据集简介"
	//meta_sources := "[{endpoint:obs.cn-southwest-228.cdzs.cn, endpoint_path:open-data},{endpoint:obs.cn-south-222.ai.pcl.cn, endpoint:grampus}]"
	//meta_caches := "[{endpoint:obs.cn-southwest-228.cdzs.cn, endpoint_path:urchincache},{endpoint:obs.cn-south-222.ai.pcl.cn, endpoint:grampus2}]"

	ctx.JSON(http.StatusOK, gin.H{
		"status_code":  0,
		"status_msg":   "Succeed",
		"dataset_id":   datasetId,
		"name":         dvInfo.Name,
		"desc":         dvInfo.Desc,
		"meta_sources": dvInfo.MetaSources,
		"meta_caches":  dvInfo.MetaCaches,
	})
	return
}

// ToDo
// DELETE /api/v1/dataset/:datasetid/version/:versionid
func DeleteDataSetVersion(ctx *gin.Context) {
	var uriParams UrchinDataSetVersionUriParams
	if err := ctx.ShouldBindUri(&uriParams); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	datasetId := uriParams.DATASET_ID
	versionId := uriParams.VERSION_ID
	logger.Infof("parsed datasetId:%s, versionId:%s", datasetId, versionId)

	redisClient := util.NewRedisStorage(urchin_dataset.RedisClusterIP, "dragonfly", false)
	err := redisClient.DeleteMapElement(datasetversionPrefix+datasetId, versionId)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status_code": 0,
		"status_msg":  "Succeed",
		"datasetId":   datasetId,
	})
	return
}

// ToDo
// GET /api/v1/datasets/:datasetid/versions
func ListDataSetVersions(ctx *gin.Context) {
	var uriParams UrchinDataSetVersionCreateUriParams
	if err := ctx.ShouldBindUri(&uriParams); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var params UrchinDataSetVersionListParams
	if err := ctx.ShouldBind(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	datasetId := uriParams.DATASET_ID
	logger.Infof("parsed datasetId:%s, page_index:%d, page_size:%d, search_key:%s, order_by:%s, sort_by:%d, create_at_less:%d, create_at_greater:%d",
		datasetId, params.PageIndex, params.PageSize, params.SearchKey, params.OrderBy, params.SortBy, params.CreateAtLess, params.CreateAtGreater)

	redisClient := util.NewRedisStorage(urchin_dataset.RedisClusterIP, "dragonfly", false)
	dvmap, err := redisClient.ReadMap(datasetversionPrefix + datasetId)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		logger.Errorf("DataSetVersion Error: %s", err.Error())
		return
	}

	var dvList []UrchinDataSetVersionInfo

	for _, versionInfo := range dvmap {
		dvInfo := &UrchinDataSetVersionInfo{}
		err = json.Unmarshal(versionInfo, &dvInfo)
		if err != nil {
			ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
			logger.Errorf("DataSetVersion Error: %s", err.Error())
			return
		}

		// filter user defined name desc or create time for dataversion list
		if params.CreateAtLess > 0 && (dvInfo.CreateAt > params.CreateAtLess || dvInfo.CreateAt < params.CreateAtGreater) {
			continue
		}
		if params.SearchKey != "" && !strings.Contains(dvInfo.Name, params.SearchKey) && !strings.Contains(dvInfo.Desc, params.SearchKey) {
			continue
		}

		dvList = append(dvList, *dvInfo)
	}

	sort.Slice(dvList, func(p, q int) bool {

		if params.OrderBy == "name" {
			if params.SortBy == -1 {
				return dvList[p].Name > dvList[q].Name
			} else {
				return dvList[p].Name < dvList[q].Name
			}
		} else if params.OrderBy == "create_at" {
			if params.SortBy == -1 {
				return dvList[p].CreateAt > dvList[q].CreateAt
			} else {
				return dvList[p].CreateAt < dvList[q].CreateAt
			}
		} else {
			return dvList[p].CreateAt > dvList[q].CreateAt
		}

	})

	sliceStart := params.PageSize * params.PageIndex
	sliceEnd := params.PageSize * (params.PageIndex + 1)
	if sliceEnd > len(dvList) {
		sliceEnd = len(dvList)
	}
	if sliceStart < 0 {
		sliceStart = 0
	}

	if sliceStart > len(dvList) {
		sliceStart = len(dvList)
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status_code": 0,
		"status_msg":  "Succeed",
		"versions":    dvList[sliceStart:sliceEnd],
	})
	return
}
