package urchin_file

import (
	"bytes"
	"context"
	commonv1 "d7y.io/api/pkg/apis/common/v1"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	urchinstatus "d7y.io/dragonfly/v2/client/daemon/urchin_status"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-http-utils/headers"
	"github.com/opentracing/opentracing-go/log"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultFileMode      = os.FileMode(0644)
	defaultDirectoryMode = os.FileMode(0755)
)

const (
	//Source to compatible urchin1
	Source = iota
	Meta
	// Ephemeral only writes the object to the dfdaemon.
	// It is only provided for creating temporary objects between peers,
	// and users are not allowed to use this mode.
	Ephemeral
	Chunk
	ChunkEnd
	Blob
)

// DataMode Enum json value maps for DataMode.
var (
	DataModeEnum2Json = map[int32]string{
		0: "Source",
		1: "Meta",
		2: "Ephemeral",
		3: "Chunk",
		4: "ChunkEnd",
		5: "Blob",
	}

	DataModeJson2Enum = map[string]int32{
		"Source":    0,
		"Meta":      1,
		"Ephemeral": 2,
		"Chunk":     3,
		"ChunkEnd":  4,
		"Blob":      5,
	}
)

const (
	// defaultSignExpireTime is default expire of sign url.
	defaultSignExpireTime = 5 * time.Minute
)

type UrchinFileManager struct {
	config          *config.DaemonOption
	dynconfig       config.Dynconfig
	peerTaskManager peer.TaskManager
	storageManager  storage.Manager
	peerIDGenerator peer.IDGenerator
}

func NewUrchinFileManager(config *config.DaemonOption, dynconfig config.Dynconfig,
	peerTaskManager peer.TaskManager, storageManager storage.Manager,
	peerIDGenerator peer.IDGenerator) *UrchinFileManager {
	return &UrchinFileManager{
		config,
		dynconfig,
		peerTaskManager,
		storageManager,
		peerIDGenerator,
	}
}

func (urfm *UrchinFileManager) genLocalBlobFileParentDir(datasetId string) string {
	dataDir := dfpath.DefaultDataDir
	if urfm.config.DataDir != "" {
		dataDir = urfm.config.DataDir
	}

	datasetBlobFileParentDir := path.Join(dataDir, "blobs", datasetId)

	return datasetBlobFileParentDir
}

func (urfm *UrchinFileManager) genLocalBlobFileName(versionId string, versionDigest string) string {
	return fmt.Sprintf("%s_%s_%s", "blob", versionDigest, versionId)
}

func (urfm *UrchinFileManager) genLocalBlobFilePath(datasetId string, versionId string, versionDigest string) string {

	datasetBlobFilePath := path.Join(urfm.genLocalBlobFileParentDir(datasetId), urfm.genLocalBlobFileName(versionId, versionDigest))

	return datasetBlobFilePath
}

func (urfm *UrchinFileManager) genBackendBlobFileObjectKey(datasetId string, versionId string, versionDigest string) string {

	datasetBlobFilePath := fmt.Sprintf("%s/%s", datasetId, urfm.genLocalBlobFileName(versionId, versionDigest))

	return datasetBlobFilePath
}

func (urfm *UrchinFileManager) genLocalChunkFileName(chunkNum uint64, chunkStart uint64) string {
	return fmt.Sprintf("%s-%s", strconv.FormatUint(chunkNum, 10), strconv.FormatUint(chunkStart, 10))
}

func (urfm *UrchinFileManager) genLocalChunkFilePath(chunksParentDir string, chunkNum uint64, chunkStart uint64) string {

	chunkFilePath := path.Join(chunksParentDir, urfm.genLocalChunkFileName(chunkNum, chunkStart))

	return chunkFilePath
}

func (urfm *UrchinFileManager) genLocalChunksParentDir(datasetId string, versionId string, versionDigest string) string {
	dataDir := dfpath.DefaultDataDir
	if urfm.config.DataDir != "" {
		dataDir = urfm.config.DataDir
	}

	chunksParentDir := path.Join(dataDir, "chunks", datasetId, urfm.genLocalBlobFileName(versionId, versionDigest))

	return chunksParentDir
}

// UploadFile uses to upload object data.
func (urfm *UrchinFileManager) UploadFile(ctx *gin.Context) {

	var form UploadFileRequest
	if err := ctx.ShouldBind(&form); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	//ToDo: set bucketName dynamic
	var (
		bucketName       = "urchincache"
		dataMode         = form.DataMode
		fileHeader       = form.File
		datasetHashAlgo  = strings.Split(form.Digest, ":")[0]
		datasetHash      = strings.Split(form.Digest, ":")[1]
		datasetId        = form.DatasetId
		datasetVersionId = form.DatasetVersionId
	)

	// Handle task for backend.
	switch DataModeJson2Enum[dataMode] {
	case Source, Meta:
		objectKey := fmt.Sprintf("%s/%s/%s", datasetId, datasetVersionId, fileHeader.Filename)

		client, err := urfm.client()
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		signURL, err := client.GetSignURL(ctx, bucketName, objectKey, objectstorage.MethodGet, defaultSignExpireTime)
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		// Initialize url meta.
		urlMeta := &commonv1.UrlMeta{Filter: urfm.config.ObjectStorage.Filter}

		dgst := digest.New(datasetHashAlgo, datasetHash)
		urlMeta.Digest = ""

		// Initialize max replicas.
		maxReplicas := urfm.config.ObjectStorage.MaxReplicas

		// Initialize task id and peer id.
		taskID := idgen.TaskIDV1(signURL, urlMeta)
		peerID := urfm.peerIDGenerator.PeerID()

		log := logger.WithTaskAndPeerID(taskID, peerID)
		log.Infof("upload object %s meta: %s %#v", objectKey, signURL, urlMeta)

		// Import object to local storage.
		log.Infof("import object %s to local storage", objectKey)
		if err := urfm.importObjectToLocalStorage(ctx, taskID, peerID, fileHeader); err != nil {
			log.Error(err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		// Announce peer information to scheduler.
		log.Info("announce peer to scheduler")
		if err := urfm.peerTaskManager.AnnouncePeerTask(ctx, storage.PeerTaskMetadata{
			TaskID: taskID,
			PeerID: peerID,
		}, signURL, commonv1.TaskType_DfStore, urlMeta); err != nil {
			log.Error(err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		// Import object to seed peer.
		go func() {
			if err := urfm.importObjectToSeedPeers(context.Background(), bucketName, objectKey, urlMeta.Filter, Ephemeral, fileHeader, maxReplicas, log); err != nil {
				log.Errorf("import object %s to seed peers failed: %s", objectKey, err)
			}
		}()

		// Import object to object storage.
		go func() {
			log.Infof("import object %s to bucket %s", objectKey, bucketName)
			if err := urfm.importObjectToBackend(context.Background(), bucketName, objectKey, dgst, fileHeader, client); err != nil {
				log.Errorf("import object %s to bucket %s failed: %s", objectKey, bucketName, err.Error())
				return
			}
		}()

		ctx.Status(http.StatusOK)
		return
	case Chunk:
		fmt.Printf("!!!upload DataChunk, bucketName:%s, objectKey:%s !!!", bucketName)

		if err := urfm.storeLocalChunks(form.DatasetId, form.DatasetVersionId, form.Digest, form.ChunkStart, form.ChunkNum, fileHeader); err != nil {
			fmt.Errorf("storeDataChunk chunkStart:%d, ChunkNum:%d failed: %s", form.ChunkStart, form.ChunkNum, err.Error())
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		ctx.Status(http.StatusOK)
		return
	case ChunkEnd:
		fmt.Printf("!!!upload DataChunkEnd, bucketName:%s, objectKey:%s !!!", bucketName)

		if err := urfm.mergeLocalChunks(form.DatasetId, form.DatasetVersionId, form.Digest); err != nil {
			fmt.Errorf("storeDataChunk chunkStart:%d, ChunkNum:%d failed: %s", form.ChunkStart, form.ChunkNum, err.Error())
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		mergedDataBlobFilePath := urfm.genLocalBlobFilePath(datasetId, form.DatasetVersionId, form.Digest)

		backendBlobFileObjectKey := urfm.genBackendBlobFileObjectKey(datasetId, form.DatasetVersionId, form.Digest)

		client, err := urfm.client()
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		// Initialize url meta.
		urlMeta := &commonv1.UrlMeta{Filter: urfm.config.ObjectStorage.Filter}

		dgst := digest.New(datasetHashAlgo, datasetHash)

		// Initialize max replicas.
		maxReplicas := urfm.config.ObjectStorage.MaxReplicas

		// Initialize task id and peer id.
		taskID := dgst.String()
		peerID := urfm.peerIDGenerator.PeerID()

		log := logger.WithTaskAndPeerID(taskID, peerID)
		log.Infof("upload object %s meta: %s", backendBlobFileObjectKey, taskID)

		// Import object to local storage.
		log.Infof("import object %s to local storage", backendBlobFileObjectKey)
		if err := urfm.importFileToLocalStorage(ctx, taskID, peerID, mergedDataBlobFilePath); err != nil {
			log.Error(err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		// Announce peer information to scheduler.
		log.Info("announce peer to scheduler")
		if err := urfm.peerTaskManager.AnnouncePeerTask(ctx, storage.PeerTaskMetadata{
			TaskID: taskID,
			PeerID: peerID,
		}, taskID, commonv1.TaskType_DfStore, urlMeta); err != nil {
			log.Error(err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		// Import object to seed peer.
		go func() {
			if err := urfm.importFileToSeedPeers(context.Background(), bucketName, backendBlobFileObjectKey, urlMeta.Filter, Ephemeral, mergedDataBlobFilePath, maxReplicas, log); err != nil {
				log.Errorf("import object %s to seed peers failed: %s", backendBlobFileObjectKey, err)
			}
		}()

		// Import local file to backend object storage.
		go func() {
			log.Infof("import object %s to bucket %s", backendBlobFileObjectKey, bucketName)
			if err := urfm.importFileToBackend(context.Background(), bucketName, backendBlobFileObjectKey, dgst, mergedDataBlobFilePath, client); err != nil {
				log.Errorf("import object %s to bucket %s failed: %s", backendBlobFileObjectKey, bucketName, err.Error())
				return
			}

			//ToDo: dataset replicas async task

			//if err := urfm.deleteLocalChunks(datasetId, datasetVersionId, dgst); err != nil {
			//	log.Errorf("deleteLocalChunks failed: %s, datasetId:%s, datasetVersionId:%s", err.Error(), datasetId, datasetVersionId)
			//	return
			//}

			if err := urfm.deleteLocalBlobFile(datasetId, datasetVersionId, dgst); err != nil {
				log.Errorf("deleteLocalBlobFile failed: %s, datasetId:%s, datasetVersionId:%s", err.Error(), datasetId, datasetVersionId)
				return
			}
		}()

		ctx.JSON(http.StatusOK, gin.H{
			"content_length": fmt.Sprint(form.TotalSize),
			"digest":         form.Digest,
			"status_code":    0,
			"status_msg":     "Succeed",
			"task_id":        taskID,
		})
		return
	case Ephemeral:
		fmt.Printf("!!!upload DataCache, bucketName:%s, objectKey:%s !!!", bucketName)
		ctx.Status(http.StatusOK)
		return
	}

	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": fmt.Sprintf("unknow dataMode %s", dataMode)})
	return
}

func (urfm *UrchinFileManager) StatFile(ctx *gin.Context) {
	var statParams StatFileParams
	if err := ctx.ShouldBindQuery(&statParams); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	statDataMode := statParams.DataMode

	switch DataModeJson2Enum[statDataMode] {
	case Blob:

		//ToDo: get dataset source replicas filepath
		bucketName := "urchincache"
		backendBlobFileObjectKey := urfm.genBackendBlobFileObjectKey(statParams.DatasetId, statParams.DatasetVersionId, statParams.Digest)
		client, err := urfm.client()
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}
		_, isExist, err := client.GetObjectMetadata(ctx, bucketName, backendBlobFileObjectKey)
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		if !isExist {

			isExist, err := urfm.checkLocalBlobFile(statParams)

			if err != nil {
				log.Error(err)
				ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
				return
			}

			if isExist {
				localFileExistStat := urchinstatus.NewStatus(urchinstatus.Exist)

				ctx.JSON(http.StatusOK, gin.H{
					"status_code":        localFileExistStat.StatusCode,
					"status_msg":         localFileExistStat.StatusMessage,
					"mode":               statParams.DataMode,
					"dataset_id":         statParams.DatasetId,
					"dataset_version_id": statParams.DatasetVersionId,
					"digest":             statParams.Digest,
				})
				return
			} else {
				localFileNotFoundStat := urchinstatus.NewStatus(urchinstatus.NotFound)

				ctx.JSON(http.StatusOK, gin.H{
					"status_code":        localFileNotFoundStat.StatusCode,
					"status_msg":         localFileNotFoundStat.StatusMessage,
					"mode":               statParams.DataMode,
					"dataset_id":         statParams.DatasetId,
					"dataset_version_id": statParams.DatasetVersionId,
					"digest":             statParams.Digest,
				})
				return
			}
		}

		backendFileExistStat := urchinstatus.NewStatus(urchinstatus.Exist)

		ctx.JSON(http.StatusOK, gin.H{
			"status_code":        backendFileExistStat.StatusCode,
			"status_msg":         backendFileExistStat.StatusMessage,
			"mode":               statParams.DataMode,
			"dataset_id":         statParams.DatasetId,
			"dataset_version_id": statParams.DatasetVersionId,
			"digest":             statParams.Digest,
		})
		return
	case Meta:
		return
	case Chunk:
		isExist, err := urfm.checkLocalChunkFile(statParams)

		if err != nil {
			log.Error(err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		if isExist {
			localFileExistStat := urchinstatus.NewStatus(urchinstatus.Exist)

			ctx.JSON(http.StatusOK, gin.H{
				"status_code":        localFileExistStat.StatusCode,
				"status_msg":         localFileExistStat.StatusMessage,
				"mode":               statParams.DataMode,
				"dataset_id":         statParams.DatasetId,
				"dataset_version_id": statParams.DatasetVersionId,
				"digest":             statParams.Digest,
				"chunk_num":          statParams.ChunkNum,
				"chunk_start":        statParams.ChunkStart,
				"chunk_size":         statParams.ChunkSize,
			})
			return
		} else {
			localFileNotFoundStat := urchinstatus.NewStatus(urchinstatus.NotFound)

			ctx.JSON(http.StatusOK, gin.H{
				"status_code":        localFileNotFoundStat.StatusCode,
				"status_msg":         localFileNotFoundStat.StatusMessage,
				"mode":               statParams.DataMode,
				"dataset_id":         statParams.DatasetId,
				"dataset_version_id": statParams.DatasetVersionId,
				"digest":             statParams.Digest,
				"chunk_num":          statParams.ChunkNum,
				"chunk_start":        statParams.ChunkStart,
				"chunk_size":         statParams.ChunkSize,
			})
			return
		}
	}

	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": fmt.Sprintf("unknow dataMode %d", statDataMode)})
	return
}

func (urfm *UrchinFileManager) checkLocalBlobFile(statParams StatFileParams) (bool, error) {
	tryLocalBlobFilePath := urfm.genLocalBlobFilePath(statParams.DatasetId, statParams.DatasetVersionId, statParams.Digest)

	localBlobFileInfo, err := os.Stat(tryLocalBlobFilePath)
	if err == nil {
		logger.Infof("check local blob file %q exists", tryLocalBlobFilePath)
		isPass := urfm.validateLocalBlobFile(uint64(localBlobFileInfo.Size()), statParams.TotalSize)
		return isPass, nil
	} else if !os.IsExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (urfm *UrchinFileManager) validateLocalBlobFile(localFileSize uint64, expectFileSize uint64) bool {
	return localFileSize == expectFileSize
}

//deleteLocalBlobFile to save local disk space
func (urfm *UrchinFileManager) deleteLocalBlobFile(datasetId string, versionId string, versionDigest *digest.Digest) error {
	tryLocalBlobFilePath := urfm.genLocalBlobFilePath(datasetId, versionId, versionDigest.String())

	_, err := os.Stat(tryLocalBlobFilePath)
	if err == nil {
		logger.Infof("Blob File %q exists, delete it", tryLocalBlobFilePath)
		os.Remove(tryLocalBlobFilePath)
	}

	return err
}

func (urfm *UrchinFileManager) checkLocalChunkFile(statParams StatFileParams) (bool, error) {

	localChunksParentDir := urfm.genLocalChunksParentDir(statParams.DatasetId, statParams.DatasetVersionId, statParams.Digest)

	tryLocalChunkFilePath := urfm.genLocalChunkFilePath(localChunksParentDir, statParams.ChunkNum, statParams.ChunkStart)

	localChunkFileInfo, err := os.Stat(tryLocalChunkFilePath)
	if err == nil {
		logger.Infof("check local chunk file %q exists", tryLocalChunkFilePath)
		isPass := urfm.validateLocalChunkFile(uint64(localChunkFileInfo.Size()), statParams)
		return isPass, nil
	} else if !os.IsExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (urfm *UrchinFileManager) validateLocalChunkFile(localFileSize uint64, statParams StatFileParams) bool {

	chunkEnd := statParams.ChunkStart + statParams.ChunkSize

	return localFileSize == statParams.ChunkSize && chunkEnd >= 0 && chunkEnd <= statParams.TotalSize
}

//deleteLocalChunks to save local disk space
func (urfm *UrchinFileManager) deleteLocalChunks(datasetId string, versionId string, versionDigest *digest.Digest) error {

	localChunksParentDir := urfm.genLocalChunksParentDir(datasetId, versionId, versionDigest.String())
	_, err := os.Stat(localChunksParentDir)
	if err == nil {
		logger.Infof("Chunks Dir %q exists, delete it", localChunksParentDir)
		os.RemoveAll(localChunksParentDir)
	}

	return err
}

func (urfm *UrchinFileManager) storeLocalChunks(datasetId string, versionId string, versionDigest string, chunkStart uint64, chunkNum uint64, uploadDataChunk *multipart.FileHeader) error {
	uploadChunkFile, err := uploadDataChunk.Open()
	if err != nil {
		return err
	}
	defer uploadChunkFile.Close()

	chunksParentDir := urfm.genLocalChunksParentDir(datasetId, versionId, versionDigest)

	if err := os.MkdirAll(chunksParentDir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return err
	}

	chunkFilePath := urfm.genLocalChunkFilePath(chunksParentDir, chunkNum, chunkStart)
	_, err = os.Stat(chunkFilePath)
	if err == nil {
		// remove exist file
		logger.Infof("destination chunk file %q exists, purge it first", chunkFilePath)
		os.Remove(chunkFilePath)
	}

	localChunkFile, err := os.OpenFile(chunkFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, defaultFileMode)
	if err != nil {
		logger.Errorf("open destination chunk file error: %s", err)
		return err
	}
	defer localChunkFile.Close()

	n, err := io.Copy(localChunkFile, uploadChunkFile)
	logger.Debugf("copied chunk file data %d bytes to %s", n, chunkFilePath)
	return err
}

func (urfm *UrchinFileManager) mergeLocalChunks(datasetId string, versionId string, versionDigest string) error {

	localBlobFileParentDir := urfm.genLocalBlobFileParentDir(datasetId)
	if err := os.MkdirAll(localBlobFileParentDir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return err
	}

	localBlobFilePath := urfm.genLocalBlobFilePath(datasetId, versionId, versionDigest)

	_, err := os.Stat(localBlobFilePath)
	if err == nil {
		// remove exist file
		logger.Infof("destination datasetBlobFile %q exists, purge it first", localBlobFilePath)
		os.Remove(localBlobFilePath)
	}

	localBlobFile, err := os.OpenFile(localBlobFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, defaultFileMode)
	if err != nil {
		return err
	}
	defer localBlobFile.Close()

	localChunksParentDir := urfm.genLocalChunksParentDir(datasetId, versionId, versionDigest)

	if err := os.MkdirAll(localChunksParentDir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return err
	}

	localChunkFileInfos, err := os.ReadDir(localChunksParentDir)

	if err != nil {
		return err
	}

	sort.Slice(localChunkFileInfos, func(pro, pre int) bool {
		fileNamePre := localChunkFileInfos[pre].Name()

		chunkNumPre, _ := strconv.ParseUint(strings.Split(fileNamePre, "-")[0], 10, 64)

		fileNamePro := localChunkFileInfos[pro].Name()

		chunkNumPro, _ := strconv.ParseUint(strings.Split(fileNamePro, "-")[0], 10, 64)
		//True will change item position
		return chunkNumPre > chunkNumPro
	})

	for _, localChunkFileInfo := range localChunkFileInfos {
		localChunkFileName := localChunkFileInfo.Name()

		localChunkFilePath := path.Join(localChunksParentDir, localChunkFileName)

		localChunkFile, err := os.OpenFile(localChunkFilePath, os.O_RDONLY, defaultFileMode)
		if err != nil {
			return err
		}

		_, err = io.Copy(localBlobFile, localChunkFile)

		if err != nil {
			return err
		}

		logger.Infof("merged chunkFile Name:%s", localChunkFileName)

		localChunkFile.Close()
	}

	return nil
}

// importFileToBackend uses to import file to backend.
func (urfm *UrchinFileManager) importFileToBackend(ctx context.Context, bucketName, objectKey string, dgst *digest.Digest, sourceFilePath string, client objectstorage.ObjectStorage) error {
	sourceFile, err := os.OpenFile(sourceFilePath, os.O_RDONLY, defaultFileMode)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	if err := client.PutObject(ctx, bucketName, objectKey, dgst.String(), sourceFile); err != nil {
		return err
	}
	return nil
}

// importFileToLocalStorage uses to import file to local storage.
func (urfm *UrchinFileManager) importFileToLocalStorage(ctx context.Context, taskID, peerID string, sourceFilePath string) error {

	sourceFile, err := os.OpenFile(sourceFilePath, os.O_RDONLY, defaultFileMode)
	if err != nil {
		return err
	}
	sourceFileInfo, err := sourceFile.Stat()
	if err != nil {
		return err
	}

	defer sourceFile.Close()

	meta := storage.PeerTaskMetadata{
		TaskID: taskID,
		PeerID: peerID,
	}

	// Register task.
	tsd, err := urfm.storageManager.RegisterTask(ctx, &storage.RegisterTaskRequest{
		PeerTaskMetadata: meta,
	})
	if err != nil {
		return err
	}

	// Import task data to dfdaemon.
	if err := urfm.peerTaskManager.GetPieceManager().Import(ctx, meta, tsd, sourceFileInfo.Size(), sourceFile); err != nil {
		return err
	}

	return nil
}

// importFileToSeedPeers uses to import file to available seed peers.
func (urfm *UrchinFileManager) importFileToSeedPeers(ctx context.Context, bucketName, objectKey, filter string, mode int, sourceFilePath string, maxReplicas int, log *logger.SugaredLoggerOnWith) error {
	schedulers, err := urfm.dynconfig.GetSchedulers()
	if err != nil {
		return err
	}

	var seedPeerHosts []string
	for _, scheduler := range schedulers {
		for _, seedPeer := range scheduler.SeedPeers {
			if urfm.config.Host.AdvertiseIP.String() != seedPeer.Ip && seedPeer.ObjectStoragePort > 0 {
				seedPeerHosts = append(seedPeerHosts, fmt.Sprintf("%s:%d", seedPeer.Ip, seedPeer.ObjectStoragePort))
			}
		}
	}
	seedPeerHosts = pkgstrings.Unique(seedPeerHosts)

	var replicas int
	for _, seedPeerHost := range seedPeerHosts {
		log.Infof("import file %s to seed peer %s", objectKey, seedPeerHost)
		if err := urfm.importFileToSeedPeer(ctx, seedPeerHost, bucketName, objectKey, filter, mode, sourceFilePath); err != nil {
			log.Errorf("import file %s to seed peer %s failed: %s", objectKey, seedPeerHost, err)
			continue
		}

		replicas++
		if replicas >= maxReplicas {
			break
		}
	}

	log.Infof("import %d file %s to seed peers", replicas, objectKey)
	return nil
}

// importFileToSeedPeer uses to import object to seed peer.
func (urfm *UrchinFileManager) importFileToSeedPeer(ctx context.Context, seedPeerHost, bucketName, objectKey, filter string, mode int, sourceFilePath string) error {
	sourceFile, err := os.OpenFile(sourceFilePath, os.O_RDONLY, defaultFileMode)
	if err != nil {
		return err
	}
	sourceFileInfo, err := sourceFile.Stat()
	if err != nil {
		return err
	}
	sourceFileName := sourceFileInfo.Name()
	defer sourceFile.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("mode", fmt.Sprint(mode)); err != nil {
		return err
	}

	if filter != "" {
		if err := writer.WriteField("filter", filter); err != nil {
			return err
		}
	}

	part, err := writer.CreateFormFile("file", sourceFileName)
	if err != nil {
		return err
	}

	if _, err := io.Copy(part, sourceFile); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	u := url.URL{
		Scheme: "http",
		Host:   seedPeerHost,
		Path:   filepath.Join("buckets", bucketName, "objects", objectKey),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Add(headers.ContentType, writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad response status %s", resp.Status)
	}

	return nil
}

// importObjectToBackend uses to import object to backend.
func (urfm *UrchinFileManager) importObjectToBackend(ctx context.Context, bucketName, objectKey string, dgst *digest.Digest, fileHeader *multipart.FileHeader, client objectstorage.ObjectStorage) error {
	f, err := fileHeader.Open()
	if err != nil {
		return err
	}
	defer f.Close()

	if err := client.PutObject(ctx, bucketName, objectKey, dgst.String(), f); err != nil {
		return err
	}
	return nil
}

// importObjectToSeedPeers uses to import object to local storage.
func (urfm *UrchinFileManager) importObjectToLocalStorage(ctx context.Context, taskID, peerID string, fileHeader *multipart.FileHeader) error {
	f, err := fileHeader.Open()
	if err != nil {
		return nil
	}
	defer f.Close()

	meta := storage.PeerTaskMetadata{
		TaskID: taskID,
		PeerID: peerID,
	}

	// Register task.
	tsd, err := urfm.storageManager.RegisterTask(ctx, &storage.RegisterTaskRequest{
		PeerTaskMetadata: meta,
	})
	if err != nil {
		return err
	}

	// Import task data to dfdaemon.
	if err := urfm.peerTaskManager.GetPieceManager().Import(ctx, meta, tsd, fileHeader.Size, f); err != nil {
		return err
	}

	return nil
}

// importObjectToSeedPeers uses to import object to available seed peers.
func (urfm *UrchinFileManager) importObjectToSeedPeers(ctx context.Context, bucketName, objectKey, filter string, mode int, fileHeader *multipart.FileHeader, maxReplicas int, log *logger.SugaredLoggerOnWith) error {
	schedulers, err := urfm.dynconfig.GetSchedulers()
	if err != nil {
		return err
	}

	var seedPeerHosts []string
	for _, scheduler := range schedulers {
		for _, seedPeer := range scheduler.SeedPeers {
			if urfm.config.Host.AdvertiseIP.String() != seedPeer.Ip && seedPeer.ObjectStoragePort > 0 {
				seedPeerHosts = append(seedPeerHosts, fmt.Sprintf("%s:%d", seedPeer.Ip, seedPeer.ObjectStoragePort))
			}
		}
	}
	seedPeerHosts = pkgstrings.Unique(seedPeerHosts)

	var replicas int
	for _, seedPeerHost := range seedPeerHosts {
		log.Infof("import object %s to seed peer %s", objectKey, seedPeerHost)
		if err := urfm.importObjectToSeedPeer(ctx, seedPeerHost, bucketName, objectKey, filter, mode, fileHeader); err != nil {
			log.Errorf("import object %s to seed peer %s failed: %s", objectKey, seedPeerHost, err)
			continue
		}

		replicas++
		if replicas >= maxReplicas {
			break
		}
	}

	log.Infof("import %d object %s to seed peers", replicas, objectKey)
	return nil
}

// importObjectToSeedPeer uses to import object to seed peer.
func (urfm *UrchinFileManager) importObjectToSeedPeer(ctx context.Context, seedPeerHost, bucketName, objectKey, filter string, mode int, fileHeader *multipart.FileHeader) error {
	f, err := fileHeader.Open()
	if err != nil {
		return err
	}
	defer f.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("mode", fmt.Sprint(mode)); err != nil {
		return err
	}

	if filter != "" {
		if err := writer.WriteField("filter", filter); err != nil {
			return err
		}
	}

	part, err := writer.CreateFormFile("file", fileHeader.Filename)
	if err != nil {
		return err
	}

	if _, err := io.Copy(part, f); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	u := url.URL{
		Scheme: "http",
		Host:   seedPeerHost,
		Path:   filepath.Join("buckets", bucketName, "objects", objectKey),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Add(headers.ContentType, writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad response status %s", resp.Status)
	}

	return nil
}

// client uses to generate client of object storage.
func (urfm *UrchinFileManager) client() (objectstorage.ObjectStorage, error) {
	config, err := urfm.dynconfig.GetObjectStorage()
	if err != nil {
		return nil, err
	}

	client, err := objectstorage.New(config.Name, config.Region, config.Endpoint,
		config.AccessKey, config.SecretKey, objectstorage.WithS3ForcePathStyle(config.S3ForcePathStyle))
	if err != nil {
		return nil, err
	}

	return client, nil
}
