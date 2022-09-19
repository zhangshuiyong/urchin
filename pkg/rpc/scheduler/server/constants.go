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

package server

import "time"

const (
	// defaultQPS is default qps of grpc server.
	defaultQPS = 10 * 1000

	// defaultBurst is default burst of grpc server.
	defaultBurst = 20 * 1000

	// defaultMaxConnectionIdle is default max connection idle of grpc keepalive.
	defaultMaxConnectionIdle = 10 * time.Minute

	// defaultMaxConnectionAge is default max connection age of grpc keepalive.
	defaultMaxConnectionAge = 12 * time.Hour

	// defaultMaxConnectionAgeGrace is default max connection age grace of grpc keepalive.
	defaultMaxConnectionAgeGrace = 5 * time.Minute
)
