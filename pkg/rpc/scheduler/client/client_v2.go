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

//go:generate mockgen -destination mocks/client_v2_mock.go -source client_v2.go -package mocks

package client

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"

	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/resolver"
	"d7y.io/dragonfly/v2/pkg/rpc"
)

// NewV2 get v2 scheduler clients using resolver and balancer,
func NewV2(ctx context.Context, dynconfig config.Dynconfig, opts ...grpc.DialOption) (ClientV2, error) {
	// Register resolver and balancer.
	resolver.RegisterScheduler(dynconfig)
	balancer.Register(pkgbalancer.NewConsistentHashingBuilder())

	conn, err := grpc.DialContext(
		ctx,
		resolver.SchedulerVirtualTarget,
		append([]grpc.DialOption{
			grpc.WithDefaultServiceConfig(pkgbalancer.BalancerServiceConfig),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				rpc.ConvertErrorUnaryClientInterceptor,
				otelgrpc.UnaryClientInterceptor(),
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithPerRetryTimeout(perRetryTimeout),
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
				rpc.RefresherUnaryClientInterceptor(dynconfig),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				rpc.ConvertErrorStreamClientInterceptor,
				otelgrpc.StreamClientInterceptor(),
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
				rpc.RefresherStreamClientInterceptor(dynconfig),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &clientV2{
		SchedulerClient: schedulerv2.NewSchedulerClient(conn),
		ClientConn:      conn,
	}, nil
}

// ClientV2 is the v2 interface for grpc client.
type ClientV2 interface {
	// AnnouncePeer announces peer to scheduler.
	AnnouncePeer(context.Context, *schedulerv2.RegisterRequest, ...grpc.CallOption) (schedulerv2.Scheduler_AnnouncePeerClient, error)

	// Checks information of peer.
	StatPeer(context.Context, *schedulerv2.StatPeerRequest, ...grpc.CallOption) (*schedulerv2.Peer, error)

	// LeavePeer releases peer in scheduler.
	LeavePeer(context.Context, *schedulerv2.LeavePeerRequest, ...grpc.CallOption) error

	// ExchangePeer exchanges peer information.
	ExchangePeer(context.Context, *schedulerv2.ExchangePeerRequest, ...grpc.CallOption) (*schedulerv2.ExchangePeerResponse, error)

	// Checks information of task.
	StatTask(context.Context, *schedulerv2.StatTaskRequest, ...grpc.CallOption) (*schedulerv2.Task, error)

	// LeaveTask releases task in scheduler.
	LeaveTask(context.Context, *schedulerv2.LeaveTaskRequest, ...grpc.CallOption) error

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// clientV2 provides v2 scheduler grpc function.
type clientV2 struct {
	schedulerv2.SchedulerClient
	*grpc.ClientConn
}

// AnnouncePeer announces peer to scheduler.
func (c *clientV2) AnnouncePeer(ctx context.Context, req *schedulerv2.RegisterRequest, opts ...grpc.CallOption) (schedulerv2.Scheduler_AnnouncePeerClient, error) {
	stream, err := c.SchedulerClient.AnnouncePeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		opts...,
	)
	if err != nil {
		return nil, err
	}

	// Send begin of piece.
	return stream, stream.Send(&schedulerv2.AnnouncePeerRequest{
		Request: &schedulerv2.AnnouncePeerRequest_RegisterRequest{
			RegisterRequest: req,
		},
	})
}

// Checks information of peer.
func (c *clientV2) StatPeer(ctx context.Context, req *schedulerv2.StatPeerRequest, opts ...grpc.CallOption) (*schedulerv2.Peer, error) {
	return c.SchedulerClient.StatPeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// LeavePeer releases peer in scheduler.
func (c *clientV2) LeavePeer(ctx context.Context, req *schedulerv2.LeavePeerRequest, opts ...grpc.CallOption) error {
	_, err := c.SchedulerClient.LeavePeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// ExchangePeer exchanges peer information.
func (c *clientV2) ExchangePeer(ctx context.Context, req *schedulerv2.ExchangePeerRequest, opts ...grpc.CallOption) (*schedulerv2.ExchangePeerResponse, error) {
	return c.SchedulerClient.ExchangePeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// Checks information of task.
func (c *clientV2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest, opts ...grpc.CallOption) (*schedulerv2.Task, error) {
	return c.SchedulerClient.StatTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// LeaveTask releases task in scheduler.
func (c *clientV2) LeaveTask(ctx context.Context, req *schedulerv2.LeaveTaskRequest, opts ...grpc.CallOption) error {
	_, err := c.SchedulerClient.LeaveTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}
