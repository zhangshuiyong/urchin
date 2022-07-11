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

//go:generate mockgen -destination mocks/mock_backend.go -source backend.go -package mocks

package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

var (
	// m is the map from scheme to backend.
	m = make(map[string]Backend)
)

func Register(scheme string, b Backend) {
	m[strings.ToLower(scheme)] = b
}

type backend struct{}

type Backend interface {
	DownloadWithContext(ctx context.Context, input *DownloadInput) (*DownloadOutput, error)
}

func New(pluginDir string) (*backend, error) {
	for scheme, backend := range LoadPlugins(pluginDir) {
		Register(scheme, backend)
	}

	return &backend{}, nil
}

func (b *backend) client(schema string) (Backend, error) {
	backend, ok := m[schema]
	if !ok {
		return nil, fmt.Errorf("can not found %s backend", schema)
	}

	return backend, nil
}

type DownloadInput struct {
	URL    *url.URL
	Header http.Header
}

type DownloadOutput struct {
	Body          io.ReadCloser
	ContentLength int64
	Status        string
	StatusCode    int
	Header        http.Header
	// Validate returns value of validating response.
	Validate error
	// Temporary check the error whether the error is temporary, if is true, we can retry it later.
	Temporary bool
}

func (b *backend) DownloadWithContext(ctx context.Context, input *DownloadInput) (*DownloadOutput, error) {
	client, err := b.client(input.URL.Scheme)
	if err != nil {
		return nil, err
	}

	return client.DownloadWithContext(ctx, input)
}
