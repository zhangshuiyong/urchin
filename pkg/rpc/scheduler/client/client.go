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

//go:generate mockgen -destination mocks/client_mock.go -source client.go -package mocks

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
	"google.golang.org/grpc/balancer"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/resolver"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
)

const (
	// maxRetries is maximum number of retries.
	maxRetries = 3

	// backoffWaitBetween is waiting for a fixed period of
	// time between calls in backoff linear.
	backoffWaitBetween = 500 * time.Millisecond

	// perRetryTimeout is GRPC timeout per call (including initial call) on this call.
	perRetryTimeout = 3 * time.Second
)

// Client is the interface for grpc client.
type Client interface {
	// V1 provides v1 client interface of scheduler.
	V1() V1

	// V1 provides v2 client interface of scheduler.
	V2() V2

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// client provides scheduler grpc function.
type client struct {
	// v1 is v1 client interface of scheduler.
	v1 V1

	// v2 is v2 client interface of scheduler.
	v2 V2

	// conn is grpc connection.
	conn *grpc.ClientConn
}

// New returns scheduler client.
func New(ctx context.Context, dynconfig config.Dynconfig, opts ...grpc.DialOption) (Client, error) {
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

	return &client{
		v1:   &v1{schedulerv1.NewSchedulerClient(conn)},
		v2:   &v2{schedulerv2.NewSchedulerClient(conn)},
		conn: conn,
	}, nil
}

func (c *client) V1() V1 {
	return c.v1
}

func (c *client) V2() V2 {
	return c.v2
}

func (c *client) Close() error {
	return c.conn.Close()
}

// V1 is the v1 interface for grpc client.
type V1 interface {
	// RegisterPeerTask registers a peer into task.
	RegisterPeerTask(context.Context, *schedulerv1.PeerTaskRequest, ...grpc.CallOption) (*schedulerv1.RegisterResult, error)

	// ReportPieceResult reports piece results and receives peer packets.
	ReportPieceResult(context.Context, *schedulerv1.PeerTaskRequest, ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error)

	// ReportPeerResult reports downloading result for the peer.
	ReportPeerResult(context.Context, *schedulerv1.PeerResult, ...grpc.CallOption) error

	// LeaveTask makes the peer leaving from task.
	LeaveTask(context.Context, *schedulerv1.PeerTarget, ...grpc.CallOption) error

	// Checks if any peer has the given task.
	StatTask(context.Context, *schedulerv1.StatTaskRequest, ...grpc.CallOption) (*schedulerv1.Task, error)

	// A peer announces that it has the announced task to other peers.
	AnnounceTask(context.Context, *schedulerv1.AnnounceTaskRequest, ...grpc.CallOption) error
}

// v1 provides v1 scheduler grpc function.
type v1 struct {
	schedulerv1.SchedulerClient
}

// RegisterPeerTask registers a peer into task.
func (v *v1) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (*schedulerv1.RegisterResult, error) {
	return v.SchedulerClient.RegisterPeerTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// ReportPieceResult reports piece results and receives peer packets.
func (v *v1) ReportPieceResult(ctx context.Context, req *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error) {
	stream, err := v.SchedulerClient.ReportPieceResult(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		opts...,
	)
	if err != nil {
		return nil, err
	}

	// Send begin of piece.
	return stream, stream.Send(&schedulerv1.PieceResult{
		TaskId: req.TaskId,
		SrcPid: req.PeerId,
		PieceInfo: &commonv1.PieceInfo{
			PieceNum: common.BeginOfPiece,
		},
	})
}

// ReportPeerResult reports downloading result for the peer.
func (v *v1) ReportPeerResult(ctx context.Context, req *schedulerv1.PeerResult, opts ...grpc.CallOption) error {
	_, err := v.SchedulerClient.ReportPeerResult(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// LeaveTask makes the peer leaving from task.
func (v *v1) LeaveTask(ctx context.Context, req *schedulerv1.PeerTarget, opts ...grpc.CallOption) error {
	_, err := v.SchedulerClient.LeaveTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// Checks if any peer has the given task.
func (v *v1) StatTask(ctx context.Context, req *schedulerv1.StatTaskRequest, opts ...grpc.CallOption) (*schedulerv1.Task, error) {
	return v.SchedulerClient.StatTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// A peer announces that it has the announced task to other peers.
func (v *v1) AnnounceTask(ctx context.Context, req *schedulerv1.AnnounceTaskRequest, opts ...grpc.CallOption) error {
	_, err := v.SchedulerClient.AnnounceTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// V2 is the v2 interface for grpc client.
type V2 interface {
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
}

// v2 provides v2 scheduler grpc function.
type v2 struct {
	schedulerv2.SchedulerClient
}

// AnnouncePeer announces peer to scheduler.
func (v *v2) AnnouncePeer(ctx context.Context, req *schedulerv2.RegisterRequest, opts ...grpc.CallOption) (schedulerv2.Scheduler_AnnouncePeerClient, error) {
	stream, err := v.SchedulerClient.AnnouncePeer(
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
func (v *v2) StatPeer(ctx context.Context, req *schedulerv2.StatPeerRequest, opts ...grpc.CallOption) (*schedulerv2.Peer, error) {
	return v.SchedulerClient.StatPeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// LeavePeer releases peer in scheduler.
func (v *v2) LeavePeer(ctx context.Context, req *schedulerv2.LeavePeerRequest, opts ...grpc.CallOption) error {
	_, err := v.SchedulerClient.LeavePeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// ExchangePeer exchanges peer information.
func (v *v2) ExchangePeer(ctx context.Context, req *schedulerv2.ExchangePeerRequest, opts ...grpc.CallOption) (*schedulerv2.ExchangePeerResponse, error) {
	return v.SchedulerClient.ExchangePeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// Checks information of task.
func (v *v2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest, opts ...grpc.CallOption) (*schedulerv2.Task, error) {
	return v.SchedulerClient.StatTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// LeaveTask releases task in scheduler.
func (v *v2) LeaveTask(ctx context.Context, req *schedulerv2.LeaveTaskRequest, opts ...grpc.CallOption) error {
	_, err := v.SchedulerClient.LeaveTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}
