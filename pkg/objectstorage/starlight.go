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

package objectstorage

import (
	"context"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"fmt"
	sl "github.com/urchinfs/starlight-sdk/starlight"
	//sl "d7y.io/dragonfly/v2/pkg/starlight"
	"io"
	"path/filepath"
	"strings"
	"time"
)

type starlight struct {
	// OBS client.
	client sl.Starlightclient
}

// New oss instance.
func newStarlight(region, endpoint, accessKey, secretKey string) (ObjectStorage, error) {

	//logger.Errorf("starlight---new %s %s %s %s", endpoint, accessKey, secretKey, region)
	protocolPrefix := "https://"
	lustreType, _, found := strings.Cut(endpoint, ".")
	if !found {
		return nil, fmt.Errorf("new starlight client failed, invalid endpoint %s", endpoint)
	}
	client, err := sl.New(lustreType, accessKey, secretKey, protocolPrefix+region+"/api")
	if err != nil {
		return nil, fmt.Errorf("new starlight client failed: %s", err)
	}

	//logger.Errorf("starlight---success-new %s %s %s %s", endpoint, accessKey, secretKey, region)
	return &starlight{
		client: client,
	}, nil
}

// GetBucketMetadata returns metadata of bucket.
func (o *starlight) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
	bucketName = convertBucketToPath(bucketName)
	isExist, err := o.client.FileExist(bucketName)
	if err != nil || !isExist {
		return nil, err
	}

	return &BucketMetadata{
		Name: bucketName,
	}, nil
}

// CreateBucket creates bucket of object storage.
func (o *starlight) CreateBucket(ctx context.Context, bucketName string) error {
	bucketName = convertBucketToPath(bucketName)
	_, err := o.client.CreateDir(bucketName)
	return err
}

// DeleteBucket deletes bucket of object storage.
func (o *starlight) DeleteBucket(ctx context.Context, bucketName string) error {
	bucketName = convertBucketToPath(bucketName)
	_, err := o.client.DeleteFile(bucketName)
	return err
}

// ListBucketMetadatas list bucket meta data of object storage.
func (o *starlight) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := o.client.GetFileList("", true)
	if err != nil {
		return nil, err
	}

	var metadatas []*BucketMetadata
	for _, bucket := range resp {
		fileCreateAt, _ := time.ParseInLocation("2006-01-02 15:04:05", bucket.Time, time.Local)
		metadatas = append(metadatas, &BucketMetadata{
			Name:     bucket.Name,
			CreateAt: fileCreateAt,
		})
	}

	return metadatas, nil
}

// GetObjectMetadata returns metadata of object.
// bucketName: base dir
// objectkey: file path
func (o *starlight) GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error) {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	metadata, err := o.client.GetFileMeta(path)
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, false, nil
		}
		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: "",
		ContentEncoding:    "",
		ContentLanguage:    "",
		ContentLength:      int64(metadata.Size),
		ContentType:        string(metadata.Type),
		ETag:               metadata.Time,
		Digest:             "",
	}, true, nil
}

// GetOject returns data of object.
func (o *starlight) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	tmpDir := "/var/lib/tmp"
	resp, err := o.client.Download(path, tmpDir)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// PutObject puts data of object.
func (o *starlight) PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	dir := filepath.Dir(path)
	exist, err := o.client.FileExist(dir)
	if err == nil && !exist {
		_, err = o.client.CreateDir(dir)
	}
	if err != nil {
		return err
	}
	return o.client.UploadTinyFile(path, reader)
}

// PutObject puts data of object.
func (o *starlight) PutObjectWithTotalLength(ctx context.Context, bucketName, objectKey, digest string, totalLength int64, reader io.Reader) error {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	dir := filepath.Dir(path)
	exist, err := o.client.FileExist(dir)
	if err == nil && !exist {
		_, err = o.client.CreateDir(dir)
	}
	if err != nil {
		return err
	}
	return o.client.Upload(path, reader, totalLength)
}

// DeleteObject deletes data of object.
func (o *starlight) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	_, err := o.client.DeleteFile(path)
	return err
}

// ListObjectMetadatas returns metadata of objects.
func (o *starlight) ListObjectMetadatas(ctx context.Context, bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error) {
	bucketName = convertBucketToPath(bucketName)
	resp, err := o.client.GetFileList(bucketName, true)
	if err != nil {
		return nil, err
	}

	var metadatas []*ObjectMetadata
	for _, object := range resp {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:  object.Path,
			ETag: object.Time,
		})
	}

	return metadatas, nil
}

// IsObjectExist returns whether the object exists.
func (o *starlight) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	return o.client.FileExist(path)
}

// IsBucketExist returns whether the bucket exists.
func (o *starlight) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	bucketName = convertBucketToPath(bucketName)
	//logger.Errorf("starlight---IsBucketExist %s", bucketName)
	return o.client.FileExist(bucketName)
}

// GetSignURL returns sign url of object.
func (o *starlight) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	return o.client.GetSignURL(path), nil
}

// ListFolderObjects returns all objects of folder.
func (o *starlight) ListFolderObjects(ctx context.Context, bucketName, prefix string) ([]*ObjectMetadata, error) {

	bucketName = convertBucketToPath(bucketName)
	folder := filepath.Join(bucketName, prefix)
	fileMetas, err := o.client.GetFileListRecursive(folder, true, MaxFolderDepth)
	if err != nil {
		return nil, err
	}
	var metadatas []*ObjectMetadata
	for _, object := range fileMetas {
		objectKey := strings.TrimPrefix(strings.TrimPrefix(object.Path, bucketName), "/")
		if object.Type == 1 {
			objectKey += "/"
		}
		metadatas = append(metadatas, &ObjectMetadata{
			Key:           objectKey,
			ETag:          object.Time,
			ContentLength: int64(object.Size),
		})
	}

	return metadatas, nil
}

// CreateFolder creates folder of object storage.
func (o *starlight) CreateFolder(ctx context.Context, bucketName, folderName string, isEmptyFolder bool) error {
	if !isEmptyFolder {
		return nil
	}
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, folderName)
	_, err := o.client.CreateDir(path)
	return err
}

// GetFolderMetadata returns metadata of folder.
func (o *starlight) GetFolderMetadata(ctx context.Context, bucketName, folderKey string) (*ObjectMetadata, bool, error) {

	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, folderKey)
	metadata, err := o.client.GetFileMeta(path)
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, false, nil
		}
		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                folderKey,
		ContentDisposition: "",
		ContentEncoding:    "",
		ContentLanguage:    "",
		ContentLength:      int64(metadata.Size),
		ContentType:        string(metadata.Type),
		ETag:               metadata.Time,
		Digest:             "",
	}, true, nil

}

// DeleteObjects deletes data of objects.
func (o *starlight) DeleteObjects(ctx context.Context, bucketName string, objects []*ObjectMetadata) error {

	var lastErr error
	lastErr = nil
	for _, object := range objects {
		logger.Infof("starlight***DeleteObjects bucketName=%s, objectKey=%s", bucketName, object.Key)
		err := o.DeleteObject(ctx, bucketName, object.Key)
		if err != nil {
			logger.Errorf("starlight---DeleteObjects bucketName=%s, objectKey=%s error=%s", bucketName, object.Key, err.Error())
			lastErr = err
		}
	}
	return lastErr
}
