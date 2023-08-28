/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//go:generate mockgen -destination mocks/objectstorage_mock.go -source objectstorage.go -package mocks

package objectstorage

import (
	"context"
	commonv1 "d7y.io/api/pkg/apis/common/v1"
	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// AsyncWriteBack writes the object asynchronously to the backend.
	AsyncWriteBack = iota

	// WriteBack writes the object synchronously to the backend.
	WriteBack

	// Ephemeral only writes the object to the dfdaemon.
	// It is only provided for creating temporary objects between peers,
	// and users are not allowed to use this mode.
	Ephemeral

	// Persistence writes the object to the seed-peer backend.
	ReplicaObjectStorage
)

// ObjectMetadata provides metadata of object.
type ObjectMetadata struct {
	// Key is object key.
	Key string

	// ContentDisposition is Content-Disposition header.
	ContentDisposition string

	// ContentEncoding is Content-Encoding header.
	ContentEncoding string

	// ContentLanguage is Content-Language header.
	ContentLanguage string

	// ContentLanguage is Content-Length header.
	ContentLength int64

	// ContentType is Content-Type header.
	ContentType string

	// ETag is ETag header.
	ETag string

	// Digest is object digest.
	Digest string
}

// BucketMetadata provides metadata of bucket.
type BucketMetadata struct {
	// Name is bucket name.
	Name string

	// CreateAt is bucket create time.
	CreateAt time.Time
}

// ObjectStorage is the interface used for object storage.
type ObjectStorage interface {
	// GetBucketMetadata returns metadata of bucket.
	GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error)

	// CreateBucket creates bucket of object storage.
	CreateBucket(ctx context.Context, bucketName string) error

	// DeleteBucket deletes bucket of object storage.
	DeleteBucket(ctx context.Context, bucketName string) error

	// ListBucketMetadatas returns metadata of buckets.
	ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error)

	// IsBucketExist returns whether the bucket exists.
	IsBucketExist(ctx context.Context, bucketName string) (bool, error)

	// GetObjectMetadata returns metadata of object.
	GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error)

	// GetOject returns data of object.
	GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error)

	// PutObject puts data of object.
	PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error

	// PutObjectWithTotalLength puts data of object.
	PutObjectWithTotalLength(ctx context.Context, bucketName, objectKey, digest string, totalLength int64, reader io.Reader) error

	// DeleteObject deletes data of object.
	DeleteObject(ctx context.Context, bucketName, objectKey string) error

	// DeleteObjects deletes data of objects.
	DeleteObjects(ctx context.Context, bucketName string, objects []*ObjectMetadata) error

	// ListObjectMetadatas returns metadata of objects.
	ListObjectMetadatas(ctx context.Context, bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error)

	// IsObjectExist returns whether the object exists.
	IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error)

	// GetSignURL returns sign url of object.
	GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error)

	// CreateFolder creates folder of object storage.
	CreateFolder(ctx context.Context, bucketName, folderName string, isEmptyFolder bool) error

	// ListFolderObjects returns all objects of folder.
	ListFolderObjects(ctx context.Context, bucketName, prefix string) ([]*ObjectMetadata, error)

	// GetFolderMetadata returns metadata of folder.
	GetFolderMetadata(ctx context.Context, bucketName, folderKey string) (*ObjectMetadata, bool, error)
}

// objectStorage provides object storage.
type objectStorage struct {
	// name is object storage name of type, it can be s3, oss or obs.
	name string

	// region is storage region.
	region string

	// endpoint is datacenter endpoint.
	endpoint string

	// accessKey is access key ID.
	accessKey string

	// secretKey is access key secret.
	secretKey string

	// secretKey is access key secret.
	s3ForcePathStyle bool
}

// Option is a functional option for configuring the objectStorage.
type Option func(o *objectStorage)

type RetryRes struct {
	Res     *ObjectMetadata
	IsExist bool
}

type ObjectParams struct {
	ID        string `uri:"id" binding:"required"`
	ObjectKey string `uri:"object_key" binding:"required"`
}

type GetObjectQuery struct {
	Filter string `form:"filter" binding:"omitempty"`
}

// WithS3ForcePathStyle set the S3ForcePathStyle for objectStorage.
func WithS3ForcePathStyle(s3ForcePathStyle bool) Option {
	return func(o *objectStorage) {
		o.s3ForcePathStyle = s3ForcePathStyle
	}
}

// New object storage interface.
func New(name, region, endpoint, accessKey, secretKey string, options ...Option) (ObjectStorage, error) {
	o := &objectStorage{
		name:             name,
		region:           region,
		endpoint:         endpoint,
		accessKey:        accessKey,
		secretKey:        secretKey,
		s3ForcePathStyle: true,
	}

	for _, opt := range options {
		opt(o)
	}

	switch o.name {
	case ServiceNameS3:
		return newS3(o.region, o.endpoint, o.accessKey, o.secretKey, o.s3ForcePathStyle)
	case ServiceNameOSS:
		return newOSS(o.region, o.endpoint, o.accessKey, o.secretKey)
	case ServiceNameOBS:
		return newOBS(o.region, o.endpoint, o.accessKey, o.secretKey)
	case ServiceNameSUGON:
		return newSugon(o.region, o.endpoint, o.accessKey, o.secretKey)
	case ServiceNameSTARLIGHT:
		return newStarlight(o.region, o.endpoint, o.accessKey, o.secretKey)
	case ServiceNameMINIO:
		return newMinio(o.region, o.endpoint, o.accessKey, o.secretKey)
	}

	return nil, fmt.Errorf("unknow service name %s", name)
}

func Client(Name, Region, Endpoint, AccessKey, SecretKey string) (ObjectStorage, error) {
	client, err := New(Name, Region, Endpoint, AccessKey, SecretKey)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func PushToOwnBackend(ctx context.Context, storageName, cacheBucket, objectKey string, meta *ObjectMetadata, pr io.ReadCloser, client ObjectStorage) error {
	logger.Debugf("pushToOwnBackend begin, objectKey:%s digest:%s storage cacheBucket:%s, name:%s", objectKey, meta.Digest, cacheBucket)
	if storageName == "sugon" || storageName == "starlight" {
		err := client.PutObjectWithTotalLength(ctx, cacheBucket, objectKey, meta.Digest, meta.ContentLength, pr)
		if err != nil {
			return err
		}
	} else {
		err := client.PutObject(ctx, cacheBucket, objectKey, meta.Digest, pr)
		if err != nil {
			return err
		}
	}
	return nil
}

func ConvertSignURL(ctx *gin.Context, signURL string, urlMeta *commonv1.UrlMeta) (string, *commonv1.UrlMeta) {
	signParse, err := url.Parse(signURL)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return signURL, urlMeta
	}
	urlMap, err := url.ParseQuery(signParse.RawQuery)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return signURL, urlMeta
	}
	if len(urlMap["token"]) > 0 && urlMap["token"][0] != "" {
		urlMeta.Header = make(map[string]string)
		urlMeta.Header["token"] = urlMap["token"][0]
		urlMap.Del("token")
		signParse.RawQuery = urlMap.Encode()
		signURL = signParse.String()
	}
	if len(urlMap["bihu-token"]) > 0 && urlMap["bihu-token"][0] != "" {
		urlMeta.Header = make(map[string]string)
		urlMeta.Header["bihu-token"] = urlMap["bihu-token"][0]
		urlMap.Del("bihu-token")
		signParse.RawQuery = urlMap.Encode()
		signURL = signParse.String()
	}
	return signURL, urlMeta
}

func NeedRetry(err error) bool {
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "context deadline exceeded") ||
		strings.Contains(errMsg, "connection failed") ||
		strings.Contains(errMsg, "timeout")
}

func ConfirmDataSourceInBackendPool(config *config.DaemonOption, sourceEndpoint string) bool {
	//check sourceEndpoint is in control by BackendPool
	for _, backend := range config.BackendPool {
		if sourceEndpoint == backend.Endpoint {
			config.SourceObs.Endpoint = backend.Endpoint
			config.SourceObs.Name = backend.Name
			config.SourceObs.Region = backend.Region
			config.SourceObs.AccessKey = backend.AccessKey
			config.SourceObs.SecretKey = backend.SecretKey
			return true
		}
	}
	logger.Errorf("sourceEndpoint %s  is not in control by backendPool", sourceEndpoint)
	return false
}

func CheckAllCacheBucketIsInControl(config *config.DaemonOption) bool {
	//Every Dst Peer check CacheBucket is existed by itself
	client, err := Client(config.ObjectStorage.Name,
		config.ObjectStorage.Region,
		config.ObjectStorage.Endpoint,
		config.ObjectStorage.AccessKey,
		config.ObjectStorage.SecretKey)
	if err != nil {
		return false
	}

	isExist, err := client.IsBucketExist(context.Background(), config.ObjectStorage.CacheBucket)
	if !isExist {
		logger.Errorf("Dst Peer Obs cacheBucket %s do not exist,please check config, err:%v", config.ObjectStorage.CacheBucket, err)
		return false
	}

	//check BackendPool cacheBucket is in control
	allCacheBucketsInControl := len(config.BackendPool)
	for _, backend := range config.BackendPool {
		for _, bucket := range backend.Buckets {
			if backend.CacheBucket == bucket.Name && bucket.Enable {
				allCacheBucketsInControl--
				break
			}
		}
	}

	if allCacheBucketsInControl != 0 {
		logger.Errorf("Not all BackendPool cacheBucket config is allowed, please check config")
		return false
	}
	return true
}

func CheckTargetBucketIsInControl(config *config.DaemonOption, targetEndpoint, targetBucketName string) bool {
	//check target bucket is in control
	targetBucketIsInControl := false

	for _, backend := range config.BackendPool {
		if targetEndpoint == backend.Endpoint {
			for _, bucket := range backend.Buckets {
				if targetBucketName == bucket.Name && bucket.Enable {
					targetBucketIsInControl = true
					break
				}
			}
			break
		}
	}

	if !targetBucketIsInControl {
		logger.Errorf("Peer binding targetEndpoint %s have not allowed the targetBucketName:%s", targetEndpoint, targetBucketName)
		return false
	}

	return true
}
