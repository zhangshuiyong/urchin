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

package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// Parse object storage url. eg: urchinfs://源数据$endpoint/源数据$bucket/源数据filepath
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
	//println("u.path:", u.Path, " u.host:", u.Host, " bucket:", bucket, " key:", key)
	if found == false {
		return "", "", "", errors.New("invalid bucket and object key " + u.Path)
	}

	return u.Host, bucket, key, nil

}

// isUrfsURL determines whether the raw url is urchinfs url.
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

func parseResponse(response *http.Response) (map[string]interface{}, error) {
	var result map[string]interface{}
	body, err := ioutil.ReadAll(response.Body)
	if err == nil {
		err = json.Unmarshal(body, &result)
	}

	return result, err
}
