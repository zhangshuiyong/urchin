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
	"fmt"
	"github.com/go-http-utils/headers"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"net/url"
	"strings"
	"time"

	MinIO "github.com/minio/minio-go/v7"
)

type minio struct {
	// Minio client.
	client *MinIO.Client
	region string
}

// New minio instance.
func newMinio(region, endpoint, accessKey, secretKey string) (ObjectStorage, error) {
	useSSL := false
	client, err := MinIO.New(endpoint, &MinIO.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
		// fix bug: too much idle connections exhaust system port resources 
		//Transport: &http.Transport{
		//	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		//},
		Region: region,
	})
	if err != nil {
		return nil, fmt.Errorf("new minio client failed: %s", err)
	}

	return &minio{
		client: client,
		region: region,
	}, nil
}

// GetBucketMetadata returns metadata of bucket.
func (m *minio) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
	isExist, err := m.client.BucketExists(ctx, bucketName)
	if err != nil || !isExist {
		return nil, err
	}

	return &BucketMetadata{
		Name: bucketName,
	}, nil
}

// CreateBucket creates bucket of object storage.
func (m *minio) CreateBucket(ctx context.Context, bucketName string) error {
	err := m.client.MakeBucket(ctx, bucketName, MinIO.MakeBucketOptions{Region: m.region})
	return err
}

// DeleteBucket deletes bucket of object storage.
func (m *minio) DeleteBucket(ctx context.Context, bucketName string) error {
	err := m.client.RemoveBucket(ctx, bucketName)
	return err
}

// ListBucketMetadatas list bucket meta data of object storage.
func (m *minio) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := m.client.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	var metadatas []*BucketMetadata
	for _, bucket := range resp {
		metadatas = append(metadatas, &BucketMetadata{
			Name:     bucket.Name,
			CreateAt: bucket.CreationDate,
		})
	}

	return metadatas, nil
}

// GetObjectMetadata returns metadata of object.
func (m *minio) GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error) {
	resp, err := m.client.StatObject(ctx, bucketName, objectKey, MinIO.StatObjectOptions{})
	if err != nil {
		if minioErr, ok := err.(MinIO.ErrorResponse); ok && minioErr.Code == "NoSuchKey" {
			return nil, false, nil
		}

		return nil, false, err
	}

	//contentLength, err := strconv.ParseInt(resp.Metadata.Get(headers.ContentLength), 10, 64)
	//if err != nil {
	//	return nil, false, err
	//}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: resp.Metadata.Get(headers.ContentDisposition),
		ContentEncoding:    resp.Metadata.Get(headers.ContentEncoding),
		ContentLanguage:    resp.Metadata.Get(headers.ContentLanguage),
		ContentLength:      resp.Size,
		ContentType:        resp.ContentType,
		ETag:               resp.ETag,
		Digest:             resp.UserMetadata[MetaDigestUpper],
	}, true, nil
}

// GetOject returns data of object.
func (m *minio) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	resp, err := m.client.GetObject(ctx, bucketName, objectKey, MinIO.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// PutObject puts data of object.
func (m *minio) PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
	meta := map[string]string{}
	meta[MetaDigest] = digest

	_, err := m.client.PutObject(ctx, bucketName, objectKey, reader, -1, MinIO.PutObjectOptions{UserMetadata: meta})
	return err
}

func (o *minio) PutObjectWithTotalLength(ctx context.Context, bucketName, objectKey, digest string, totalLength int64, reader io.Reader) error {
	return nil
}

// DeleteObject deletes data of object.
func (m *minio) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	err := m.client.RemoveObject(ctx, bucketName, objectKey, MinIO.RemoveObjectOptions{})

	return err
}

// DeleteObjects deletes data of objects.
func (m *minio) DeleteObjects(ctx context.Context, bucketName string, objects []*ObjectMetadata) error {
	objectsCh := make(chan MinIO.ObjectInfo)
	go func() {
		for _, obj := range objects {
			objectsCh <- MinIO.ObjectInfo{Key: obj.Key}
		}

		close(objectsCh)
	}()

	for rErr := range m.client.RemoveObjects(ctx, bucketName, objectsCh, MinIO.RemoveObjectsOptions{GovernanceBypass: true}) {
		return rErr.Err
	}

	return nil
}

// ListObjectMetadatas returns metadata of objects.
func (m *minio) ListObjectMetadatas(ctx context.Context, bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error) {
	objectCh := m.client.ListObjects(ctx, bucketName, MinIO.ListObjectsOptions{
		Prefix:       prefix,
		MaxKeys:      int(limit),
		WithMetadata: true,
		Recursive:    true,
	})

	var metadatas []*ObjectMetadata
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}

		metadatas = append(metadatas, &ObjectMetadata{
			Key:  object.Key,
			ETag: object.ETag,
		})
	}

	return metadatas, nil
}

// ListFolderObjects returns all objects of folder.
func (m *minio) ListFolderObjects(ctx context.Context, bucketName, prefix string) ([]*ObjectMetadata, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var metadatas []*ObjectMetadata
	for object := range m.client.ListObjects(ctx, bucketName, MinIO.ListObjectsOptions{
		Prefix:       prefix,
		WithMetadata: true,
		Recursive:    true,
	}) {
		if object.Err != nil {
			return nil, object.Err
		}

		metadatas = append(metadatas, &ObjectMetadata{
			Key:           object.Key,
			ETag:          object.ETag,
			ContentLength: object.Size,
		})
	}

	return metadatas, nil
}

// IsObjectExist returns whether the object exists.
func (m *minio) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	_, isExist, err := m.GetObjectMetadata(ctx, bucketName, objectKey)
	if err != nil {
		return false, err
	}

	if !isExist {
		return false, nil
	}

	return true, nil
}

// IsBucketExist returns whether the bucket exists.
func (m *minio) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	isExist, err := m.client.BucketExists(ctx, bucketName)
	if err != nil {
		return false, err
	}

	return isExist, nil
}

// GetSignURL returns sign url of object.
func (m *minio) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	var req *url.URL
	reqParams := make(url.Values)
	switch method {
	case MethodGet:
		req, _ = m.client.PresignedGetObject(ctx, bucketName, objectKey, expire, reqParams)
	case MethodPut:
		req, _ = m.client.PresignedPutObject(ctx, bucketName, objectKey, expire)
	case MethodHead:
		req, _ = m.client.PresignedHeadObject(ctx, bucketName, objectKey, expire, reqParams)
	default:
		return "", fmt.Errorf("not support method %s", method)
	}

	return req.String(), nil
}

// CreateFolder creates folder of object storage.
func (m *minio) CreateFolder(ctx context.Context, bucketName, folderName string, isEmptyFolder bool) error {
	//if !isEmptyFolder {
	//	return nil
	//}

	if !strings.HasSuffix(folderName, "/") {
		folderName += "/"
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, err := m.client.PutObject(ctx, bucketName, folderName, nil, 0, MinIO.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
}

// GetFolderMetadata returns metadata of folder.
func (m *minio) GetFolderMetadata(ctx context.Context, bucketName, folderKey string) (*ObjectMetadata, bool, error) {
	meta, isExist, err := m.GetObjectMetadata(ctx, bucketName, folderKey)
	if err != nil || isExist {
		return meta, isExist, err
	}

	exist := false
	for object := range m.client.ListObjects(ctx, bucketName, MinIO.ListObjectsOptions{
		Prefix:    folderKey,
		Recursive: false,
	}) {
		if object.Err != nil {
			return nil, false, object.Err
		}

		exist = true
		return &ObjectMetadata{
			Key: folderKey,
		}, exist, nil
	}

	return nil, exist, nil
}
