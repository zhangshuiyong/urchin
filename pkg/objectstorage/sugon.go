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
	sg "github.com/urchinfs/sugon-sdk/sugon"
	"io"
	"path/filepath"
	"strings"
	"time"
)

type sugon struct {
	// OBS client.
	client sg.Sgclient
}

// New oss instance.
func newSugon(region, endpoint, accessKey, secretKey string) (ObjectStorage, error) {
	//clusterId, home, found := strings.Cut(endpoint, "-")
	//if !found {
	//	return nil, fmt.Errorf("new sugon client failed, endpoint format error: %s", endpoint)
	//}
	//logger.Errorf("sugon---new %s %s %s %s", endpoint, accessKey, secretKey, region)
	region_split := strings.Split(region, ",")
	protocolPrefix := "https://"
	if len(region_split) != 3 {
		return nil, fmt.Errorf("new sugon client failed, invalid region %s", region)
	}
	clusterId, _, found := strings.Cut(endpoint, ".")
	if !found {
		return nil, fmt.Errorf("new sugon client failed, invalid endpoint %s", endpoint)
	}
	client, err := sg.New(clusterId, accessKey, secretKey, region_split[0], protocolPrefix+region_split[1], protocolPrefix+region_split[2])
	if err != nil {
		return nil, fmt.Errorf("new sugon client failed: %s", err)
	}

	//logger.Errorf("sugon---success-new %s %s %s %s", endpoint, accessKey, secretKey, region)
	return &sugon{
		client: client,
	}, nil
}

// GetBucketMetadata returns metadata of bucket.
func (o *sugon) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
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
func (o *sugon) CreateBucket(ctx context.Context, bucketName string) error {
	bucketName = convertBucketToPath(bucketName)
	_, err := o.client.CreateDir(bucketName)
	return err
}

// DeleteBucket deletes bucket of object storage.
func (o *sugon) DeleteBucket(ctx context.Context, bucketName string) error {
	bucketName = convertBucketToPath(bucketName)
	_, err := o.client.DeleteFile(bucketName)
	return err
}

// ListBucketMetadatas list bucket meta data of object storage.
func (o *sugon) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := o.client.GetFileList("", "", 0, 1000)
	if err != nil {
		return nil, err
	}

	var metadatas []*BucketMetadata
	for _, bucket := range resp.FileList {
		fileCreateAt, _ := time.ParseInLocation("2006-01-02 15:04:05", bucket.CreationTime, time.Local)
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
func (o *sugon) GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error) {
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
		ContentLength:      metadata.Size,
		ContentType:        metadata.Type,
		ETag:               metadata.LastModifiedTime,
		Digest:             "",
	}, true, nil
}

// GetOject returns data of object.
func (o *sugon) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	resp, err := o.client.Download(path)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// PutObject puts data of object.
func (o *sugon) PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
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
func (o *sugon) PutObjectWithTotalLength(ctx context.Context, bucketName, objectKey, digest string, totalLength int64, reader io.Reader) error {
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
func (o *sugon) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	_, err := o.client.DeleteFile(path)
	return err
}

// ListObjectMetadatas returns metadata of objects.
func (o *sugon) ListObjectMetadatas(ctx context.Context, bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error) {
	bucketName = convertBucketToPath(bucketName)
	resp, err := o.client.GetFilesMeta(bucketName, prefix, 0, limit)
	if err != nil {
		return nil, err
	}

	var metadatas []*ObjectMetadata
	for _, object := range resp {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:  object.Path,
			ETag: object.LastModifiedTime,
		})
	}

	return metadatas, nil
}

// IsObjectExist returns whether the object exists.
func (o *sugon) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	return o.client.FileExist(path)
}

// IsBucketExist returns whether the bucket exists.
func (o *sugon) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	bucketName = convertBucketToPath(bucketName)
	//logger.Errorf("sugon---IsBucketExist %s", bucketName)
	return o.client.FileExist(bucketName)
}

// GetSignURL returns sign url of object.
func (o *sugon) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, objectKey)
	return o.client.GetSignURL(path), nil
}

func convertBucketToPath(bucketName string) string {
	return filepath.Join("/", strings.ReplaceAll(bucketName, "-", "/"))
}

// ListFolderObjects returns all objects of folder.
func (o *sugon) ListFolderObjects(ctx context.Context, bucketName, prefix string) ([]*ObjectMetadata, error) {

	bucketName = convertBucketToPath(bucketName)
	folder := filepath.Join(bucketName, prefix)
	fileMetas, err := o.client.GetFileListRecursive(folder, "", 0, MaxFolderListPageSize, MaxFolderDepth)
	if err != nil {
		return nil, err
	}
	var metadatas []*ObjectMetadata
	for _, object := range fileMetas {
		objectKey := strings.TrimPrefix(strings.TrimPrefix(object.Path, bucketName), "/")
		if object.IsDirectory {
			objectKey += "/"
		}
		metadatas = append(metadatas, &ObjectMetadata{
			Key:           objectKey,
			ETag:          object.LastModifiedTime,
			ContentLength: int64(object.Size),
		})
	}

	return metadatas, nil
}

// CreateFolder creates folder of object storage.
func (o *sugon) CreateFolder(ctx context.Context, bucketName, folderName string, isEmptyFolder bool) error {
	if !isEmptyFolder {
		return nil
	}
	bucketName = convertBucketToPath(bucketName)
	path := filepath.Join(bucketName, folderName)
	_, err := o.client.CreateDir(path)
	return err
}

// GetFolderMetadata returns metadata of folder.
func (o *sugon) GetFolderMetadata(ctx context.Context, bucketName, folderKey string) (*ObjectMetadata, bool, error) {

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
		ETag:               metadata.LastModifiedTime,
		Digest:             "",
	}, true, nil

}

// DeleteObjects deletes data of objects.
func (o *sugon) DeleteObjects(ctx context.Context, bucketName string, objects []*ObjectMetadata) error {

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
