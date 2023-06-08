package urchin_file

import (
	"bytes"
	"context"
	commonv1 "d7y.io/api/pkg/apis/common/v1"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-http-utils/headers"
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
	Source = iota
	Meta
	// Ephemeral only writes the object to the dfdaemon.
	// It is only provided for creating temporary objects between peers,
	// and users are not allowed to use this mode.
	Ephemeral
	Chunk
	ChunkEnd
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

// UploadFile uses to upload object data.
func (urfm *UrchinFileManager) UploadFile(ctx *gin.Context) {

	var form UploadFileRequest
	if err := ctx.ShouldBind(&form); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var (
		bucketName       = "urchincache"
		mode             = form.Mode
		fileHeader       = form.File
		datasetDigest    = form.Digest
		datasetDigester  = form.Digester
		datasetId        = form.DatasetId
		datasetVersionId = form.DatasetVersionId
	)

	// Handle task for backend.
	switch mode {
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

		dgst := digest.New(datasetDigester, datasetDigest)
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

		if err := urfm.storeDataSetChunk(form.DatasetId, form.Digest, form.ChunkStart, form.ChunkNum, fileHeader); err != nil {
			fmt.Errorf("storeDataSetChunk chunkStart:%d, ChunkNum:%d failed: %s", form.ChunkStart, form.ChunkNum, err.Error())
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		ctx.Status(http.StatusOK)
		return
	case ChunkEnd:
		fmt.Printf("!!!upload DataChunkEnd, bucketName:%s, objectKey:%s !!!", bucketName)

		if err := urfm.mergeDataSetChunks(form.DatasetId, form.Digest); err != nil {
			fmt.Errorf("storeDataSetChunk chunkStart:%d, ChunkNum:%d failed: %s", form.ChunkStart, form.ChunkNum, err.Error())
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		mergedDataBlobFilePath := urfm.getDataBlobFilePath(datasetId)

		objectKey := fmt.Sprintf("%s/%s/%s", datasetId, form.Digest, "blob")

		client, err := urfm.client()
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		// Initialize url meta.
		urlMeta := &commonv1.UrlMeta{Filter: urfm.config.ObjectStorage.Filter}

		dgst := digest.New(datasetDigester, datasetDigest)

		// Initialize max replicas.
		maxReplicas := urfm.config.ObjectStorage.MaxReplicas

		// Initialize task id and peer id.
		taskID := dgst.String()
		peerID := urfm.peerIDGenerator.PeerID()

		log := logger.WithTaskAndPeerID(taskID, peerID)
		log.Infof("upload object %s meta: %s", objectKey, taskID)

		// Import object to local storage.
		log.Infof("import object %s to local storage", objectKey)
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
			if err := urfm.importFileToSeedPeers(context.Background(), bucketName, objectKey, urlMeta.Filter, Ephemeral, mergedDataBlobFilePath, maxReplicas, log); err != nil {
				log.Errorf("import object %s to seed peers failed: %s", objectKey, err)
			}
		}()

		// Import object to object storage.
		go func() {
			log.Infof("import object %s to bucket %s", objectKey, bucketName)
			if err := urfm.importFileToBackend(context.Background(), bucketName, objectKey, dgst, mergedDataBlobFilePath, client); err != nil {
				log.Errorf("import object %s to bucket %s failed: %s", objectKey, bucketName, err.Error())
				return
			}
		}()

		ctx.JSON(http.StatusOK, gin.H{
			"content_length": fmt.Sprint(form.TotalSize),
			"digest":         form.Digest,
			"digester":       form.Digester,
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

	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": fmt.Sprintf("unknow mode %d", mode)})
	return
}

func (urfm *UrchinFileManager) storeDataSetChunk(datasetId string, datasetDigest string, chunkStart uint64, chunkNum uint64, dataSource *multipart.FileHeader) error {
	sourceFile, err := dataSource.Open()
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	chunksParentDir := urfm.getDataChunksParentDir(datasetId, datasetDigest)

	if err := os.MkdirAll(chunksParentDir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return err
	}

	chunkFilePath := urfm.getDataChunkFilePath(chunksParentDir, chunkNum, chunkStart)
	_, err = os.Stat(chunkFilePath)
	if err == nil {
		// remove exist file
		logger.Infof("destination chunk file %q exists, purge it first", chunkFilePath)
		os.Remove(chunkFilePath)
	}

	dstFile, err := os.OpenFile(chunkFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, defaultFileMode)
	if err != nil {
		logger.Errorf("open destination chunk file error: %s", err)
		return err
	}
	defer dstFile.Close()

	n, err := io.Copy(dstFile, sourceFile)
	logger.Debugf("copied chunk file data %d bytes to %s", n, chunkFilePath)
	return err
}

func (urfm *UrchinFileManager) getDataBlobFilePath(datasetId string) string {
	dataDir := dfpath.DefaultDataDir
	if urfm.config.DataDir != "" {
		dataDir = urfm.config.DataDir
	}

	datasetBlobFilePath := path.Join(dataDir, "chunks", datasetId, "blob")

	return datasetBlobFilePath
}

func (urfm *UrchinFileManager) getDataChunkFilePath(chunksParentDir string, chunkNum uint64, chunkStart uint64) string {

	chunkFilePath := path.Join(chunksParentDir, fmt.Sprintf("%s-%s", strconv.FormatUint(chunkNum, 10), strconv.FormatUint(chunkStart, 10)))

	return chunkFilePath
}

func (urfm *UrchinFileManager) getDataChunksParentDir(datasetId string, datasetDigest string) string {
	dataDir := dfpath.DefaultDataDir
	if urfm.config.DataDir != "" {
		dataDir = urfm.config.DataDir
	}

	chunksParentDir := path.Join(dataDir, "chunks", datasetId, datasetDigest)

	return chunksParentDir
}

func (urfm *UrchinFileManager) mergeDataSetChunks(datasetId string, datasetDigest string) error {

	chunksParentDir := urfm.getDataChunksParentDir(datasetId, datasetDigest)

	if err := os.MkdirAll(chunksParentDir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return err
	}

	datasetBlobFilePath := urfm.getDataBlobFilePath(datasetId)

	_, err := os.Stat(datasetBlobFilePath)
	if err == nil {
		// remove exist file
		logger.Infof("destination datasetBlobFile %q exists, purge it first", datasetBlobFilePath)
		os.Remove(datasetBlobFilePath)
	}

	datasetBlobFile, err := os.OpenFile(datasetBlobFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, defaultFileMode)
	if err != nil {
		return err
	}
	defer datasetBlobFile.Close()

	chunkFileInfos, err := os.ReadDir(chunksParentDir)

	if err != nil {
		return err
	}

	sort.Slice(chunkFileInfos, func(pro, pre int) bool {
		fileNamePre := chunkFileInfos[pre].Name()

		chunkNumPre, _ := strconv.ParseUint(strings.Split(fileNamePre, "-")[0], 10, 64)

		fileNamePro := chunkFileInfos[pro].Name()

		chunkNumPro, _ := strconv.ParseUint(strings.Split(fileNamePro, "-")[0], 10, 64)

		return chunkNumPre > chunkNumPro
	})

	for _, chunkFileInfo := range chunkFileInfos {
		chunkFileName := chunkFileInfo.Name()

		chunkFilePath := path.Join(chunksParentDir, chunkFileName)

		chunkFile, err := os.OpenFile(chunkFilePath, os.O_RDONLY, defaultFileMode)
		if err != nil {
			return err
		}

		_, err = io.Copy(datasetBlobFile, chunkFile)

		if err != nil {
			return err
		}

		logger.Infof("merged chunkFile Name:%s", chunkFileName)

		chunkFile.Close()
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
