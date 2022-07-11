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
	"os"

	"d7y.io/dragonfly/v2/internal/dfplugin"
)

func LoadPlugins(dir string) map[string]Backend {
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]Backend{}
		}

		return map[string]Backend{}
	}

	backends := map[string]Backend{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		subs := dfplugin.PluginFormatExpr.FindStringSubmatch(file.Name())
		if len(subs) != 3 {
			continue
		}

		typ, name := subs[1], subs[2]
		if dfplugin.PluginType(typ) == dfplugin.PluginTypeResource {
			plugin, meta, err := dfplugin.Load(dir, dfplugin.PluginType(typ), name, map[string]string{})
			if err != nil {
				continue
			}

			if backend, ok := plugin.(Backend); ok && meta[dfplugin.PluginMetaKeyScheme] == name {
				backends[name] = backend
			}
		}
	}

	return backends
}
