package urchinfs

import (
	"context"
	"d7y.io/dragonfly/v2/client/config"
	urfs "d7y.io/dragonfly/v2/client/dfstore"
	"d7y.io/dragonfly/v2/client/util"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
)

type Urchinfs interface {

	// schedule source dataset to target peer
	ScheduleDataToPeer(sourceUrl, destPeerHost string) (string, error)

	// check schedule data to peer task status
	CheckScheduleTaskStatus(sourceUrl, destPeerHost string) (string, error)
}


type urchinfs struct {
	// Initialize default urfs config.
	cfg *config.DfstoreConfig
}

// New dfstore instance.
func New() Urchinfs {

	urfs := &urchinfs{
		cfg:   config.NewDfstore(),
	}
	return urfs
}


const (
	// UrfsScheme if the scheme of object storage.
	UrfsScheme = "urfs"
)


func (urfs *urchinfs) ScheduleDataToPeer(sourceUrl, destPeerHost string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := urfs.cfg.Validate(); err != nil {
		return "", err
	}

	if err := validateSchedulelArgs(sourceUrl, destPeerHost); err != nil {
		return "", err
	}

	// Copy object storage to local file.
	endpoint, bucketName, objectKey, err := parseUrfsURL(sourceUrl)
	if err != nil {
		return "", err
	}
	peerResult, err := processScheduleDataToPeer(ctx, urfs.cfg, endpoint, bucketName, objectKey, destPeerHost)
	if err != nil {
		return "", err
	}

	return peerResult, err
}

func (urfs *urchinfs) CheckScheduleTaskStatus(sourceUrl, destPeerHost string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := urfs.cfg.Validate(); err != nil {
		return "", err
	}

	if err := validateSchedulelArgs(sourceUrl, destPeerHost); err != nil {
		return "", err
	}

	// Copy object storage to local file.
	endpoint, bucketName, objectKey, err := parseUrfsURL(sourceUrl)
	if err != nil {
		return "", err
	}
	peerResult, err := processCheckScheduleTaskStatus(ctx, urfs.cfg, endpoint, bucketName, objectKey, destPeerHost)
	if err != nil {
		return "", err
	}

	return peerResult, err
}


// isUrfsURL determines whether the raw url is urfs url.
func isUrfsURL(rawURL string) bool {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return false
	}

	if u.Scheme != UrfsScheme || u.Host == "" || u.Path == "" {
		return false
	}

	return true
}

// Validate copy arguments.
func validateSchedulelArgs(sourceUrl, destPeer string) error {
	if !isUrfsURL(sourceUrl) {
		return errors.New("source url should be urfs:// protocol")
	}

	return nil
}

// Parse object storage url. eg: urfs://源数据$endpoint/源数据$bucket/源数据filepath
func parseUrfsURL(rawURL string) (string, string, string, error) {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return "", "", "", err
	}

	if u.Scheme != UrfsScheme {
		return "", "", "", fmt.Errorf("invalid scheme, e.g. %s://endpoint/bucket_name/object_key", UrfsScheme)
	}

	if u.Host == "" {
		return "", "", "", errors.New("empty endpoint name")
	}

	if u.Path == "" {
		return "", "", "", errors.New("empty object path")
	}

	bucket, key, found := strings.Cut(strings.Trim(u.Path, "/"), "/")
	if found == false {
		return "", "", "", errors.New("invalid bucket and object key " + u.Path)
	}

	return u.Host, bucket, key, nil
}

// Schedule object storage to peer.
func processScheduleDataToPeer(ctx context.Context, cfg *config.DfstoreConfig, endpoint, bucketName, objectKey, dstPeer string) (string, error) {
	dfs := urfs.New(cfg.Endpoint)
	meta, err := dfs.GetUrfsMetadataWithContext(ctx, &urfs.GetUrfsMetadataInput{
		Endpoint:   endpoint,
		BucketName: bucketName,
		ObjectKey:  objectKey,
		DstPeer:    dstPeer,
	})
	if err != nil {
		return "", err
	}

	reader, err := dfs.GetUrfsWithContext(ctx, &urfs.GetUrfsInput{
		Endpoint:   endpoint,
		BucketName: bucketName,
		ObjectKey:  objectKey,
		DstPeer:    dstPeer,
	})
	if err != nil {
		return "", err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)

	var peerResult PeerResult
	if err == nil {
		err = json.Unmarshal((body), &peerResult)
	}
	peerResult.SignedUrl = strings.ReplaceAll(peerResult.SignedUrl, "\\u0026", "&")

	fileContentLength, err := strconv.ParseInt(peerResult.ContentLength, 10, 64)
	if err != nil {
		logger.Errorf("peerResult.ContentLength parse err=%#v", err)
		return "", err
	}
	if fileContentLength != meta.ContentLength {
		logger.Errorf("Incomplete content err=%#v， meta.ContentLength=%#v, data.ContentLength=%#v", err, meta.ContentLength, peerResult.ContentLength)
		return "", errors.New("content length inconsistent with meta")
	}

	resultJson, err := util.Marshal(&peerResult)
	if err != nil {
		logger.Errorf("serialize resultJson err=%#v", err)
		return "", err
	}
	return string(resultJson), err
}

// check schedule task status.
func processCheckScheduleTaskStatus(ctx context.Context, cfg *config.DfstoreConfig, endpoint, bucketName, objectKey, dstPeer string) (string, error) {
	dfs := urfs.New(cfg.Endpoint)
	meta, err := dfs.GetUrfsMetadataWithContext(ctx, &urfs.GetUrfsMetadataInput{
		Endpoint:   endpoint,
		BucketName: bucketName,
		ObjectKey:  objectKey,
		DstPeer:    dstPeer,
	})
	if err != nil {
		return "", err
	}

	reader, err := dfs.GetUrfsStatusWithContext(ctx, &urfs.GetUrfsInput{
		Endpoint:   endpoint,
		BucketName: bucketName,
		ObjectKey:  objectKey,
		DstPeer:    dstPeer,
	})
	if err != nil {
		return "", err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)

	var peerResult PeerResult
	if err == nil {
		err = json.Unmarshal((body), &peerResult)
	}
	peerResult.SignedUrl = strings.ReplaceAll(peerResult.SignedUrl, "\\u0026", "&")

	fileContentLength, err := strconv.ParseInt(peerResult.ContentLength, 10, 64)
	if err != nil {
		logger.Errorf("peerResult.ContentLength parse err=%#v", err)
		return "", err
	}
	if fileContentLength != meta.ContentLength {
		logger.Errorf("Incomplete content err=%#v， meta.ContentLength=%#v, data.ContentLength=%#v", err, meta.ContentLength, peerResult.ContentLength)
		return "", err
	}

	resultJson, err := util.Marshal(&peerResult)
	if err != nil {
		logger.Errorf("serialize resultJson err=%#v", err)
		return "", err
	}
	return string(resultJson), err
}

type PeerResult struct {
	ContentType string `json:"Content-Type"`
	ContentLength string `json:"Content-Length"`
	SignedUrl string
	DataRoot string
	DataPath string
	DataEndpoint string
	StatusCode int
	StatusMsg string
	TaskID string
}