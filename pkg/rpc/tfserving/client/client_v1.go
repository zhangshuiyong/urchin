/*
 *     Copyright 2023 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/client_v1_mock.go -source client_v1.go -package mocks

package client

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	tfservingv1 "d7y.io/api/pkg/apis/tfserving/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

const (
	// contextTimeout is timeout of grpc invoke.
	contextTimeout = 2 * time.Minute

	// maxRetries is maximum number of retries.
	maxRetries = 3

	// backoffWaitBetween is waiting for a fixed period of
	// time between calls in backoff linear.
	backoffWaitBetween = 500 * time.Millisecond
)

// GetV1 returns v1 version of the prediction client.
func GetV1(ctx context.Context, target string, opts ...grpc.DialOption) (V1, error) {
	conn, err := grpc.DialContext(
		ctx,
		target,
		append([]grpc.DialOption{
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				otelgrpc.UnaryClientInterceptor(),
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				otelgrpc.StreamClientInterceptor(),
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &v1{
		PredictionServiceClient: tfservingv1.NewPredictionServiceClient(conn),
		ClientConn:              conn,
	}, nil
}

// ClientV1 is the interface for v1 version of the grpc client.
type V1 interface {
	// Predict provides access to loaded TensorFlow model.
	Predict(context.Context, *tfservingv1.PredictRequest, ...grpc.CallOption) (*tfservingv1.PredictResponse, error)

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// clientV1 provides v1 version of the prediction grpc function.
type v1 struct {
	tfservingv1.PredictionServiceClient
	*grpc.ClientConn
}

// Predict provides access to loaded TensorFlow model.
func (v *v1) Predict(ctx context.Context, req *tfservingv1.PredictRequest, opts ...grpc.CallOption) (*tfservingv1.PredictResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.PredictionServiceClient.Predict(ctx, req, opts...)
}
