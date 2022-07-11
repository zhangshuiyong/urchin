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

package backend

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"
)

var defaultHTTPClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}

var (
	notTemporaryStatusCode = []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusProxyAuthRequired}
)

func init() {
	Register("http", &HTTP{client: defaultHTTPClient})
	Register("https", &HTTP{client: defaultHTTPClient})
}

type HTTP struct {
	client *http.Client
}

func (h *HTTP) DownloadWithContext(ctx context.Context, input *DownloadInput) (*DownloadOutput, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, input.URL.String(), nil)
	if err != nil {
		return nil, err
	}

	for key, values := range input.Header {
		for i := range values {
			req.Header.Add(key, values[i])
		}
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}

	var output *DownloadOutput
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		output.Validate = nil
	} else {
		output.Validate = fmt.Errorf("status code is %d", resp.StatusCode)
	}

	return &DownloadOutput{}, nil
}
