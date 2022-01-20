/*
 *     Copyright 2020 The Dragonfly Authors
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

package rpcserver

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfdaemongrpc "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type Server interface {
	clientutil.KeepAlive
	ServeDownload(listener net.Listener) error
	ServePeer(listener net.Listener) error
	Stop()
}

type server struct {
	clientutil.KeepAlive
	peerHost        *scheduler.PeerHost
	peerTaskManager peer.TaskManager
	storageManager  storage.Manager

	downloadServer *grpc.Server
	peerServer     *grpc.Server
	uploadAddr     string
	dfdaemongrpc.UnimplementedDaemonServer
}

func New(peerHost *scheduler.PeerHost, peerTaskManager peer.TaskManager, storageManager storage.Manager, downloadOpts []grpc.ServerOption, peerOpts []grpc.ServerOption) (Server, error) {
	svr := &server{
		KeepAlive:       clientutil.NewKeepAlive("rpc server"),
		peerHost:        peerHost,
		peerTaskManager: peerTaskManager,
		storageManager:  storageManager,
	}
	peerServer := grpc.NewServer(append(rpc.DefaultServerOptions, peerOpts...)...)
	dfdaemongrpc.RegisterDaemonServer(peerServer, svr)
	downloadServer := grpc.NewServer(append(rpc.DefaultServerOptions, downloadOpts...)...)
	dfdaemongrpc.RegisterDaemonServer(downloadServer, svr)
	svr.peerServer = peerServer
	svr.downloadServer = downloadServer
	return svr, nil
}

func (m *server) ServeDownload(listener net.Listener) error {
	return m.downloadServer.Serve(listener)
}

func (m *server) ServePeer(listener net.Listener) error {
	m.uploadAddr = fmt.Sprintf("%s:%d", m.peerHost.Ip, m.peerHost.DownPort)
	return m.peerServer.Serve(listener)
}

func (m *server) Stop() {
	m.peerServer.GracefulStop()
	m.downloadServer.GracefulStop()
}

func (m *server) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	m.Keep()
	p, err := m.storageManager.GetPieces(ctx, request)
	if err != nil {
		code := base.Code_UnknownError
		if err == dferrors.ErrInvalidArgument {
			code = base.Code_BadRequest
		}
		if err != storage.ErrTaskNotFound {
			logger.Errorf("get piece tasks error: %s, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				err, request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, status.Error(codes.Code(code), err.Error())
		}
		// dst peer is not running
		if !m.peerTaskManager.IsPeerTaskRunning(request.TaskId) {
			code = base.Code_PeerTaskNotFound
			logger.Errorf("get piece tasks error: peer task not found, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, status.Error(codes.Code(code), err.Error())
		}

		logger.Infof("try to get piece tasks, "+
			"but target peer task is initializing, "+
			"there is no available pieces, "+
			"task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
			request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
		// dst peer is running, send empty result, src peer will retry later
		return &base.PiecePacket{
			TaskId:        request.TaskId,
			DstPid:        request.DstPid,
			DstAddr:       m.uploadAddr,
			PieceInfos:    nil,
			TotalPiece:    -1,
			ContentLength: -1,
			PieceMd5Sign:  "",
		}, nil
	}

	logger.Debugf("receive get piece tasks request, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d, length: %d",
		request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit, len(p.PieceInfos))
	p.DstAddr = m.uploadAddr
	return p, nil
}

func (m *server) CheckHealth(context.Context, *empty.Empty) (*empty.Empty, error) {
	m.Keep()
	return new(empty.Empty), nil
}

func (m *server) Download(req *dfdaemongrpc.DownRequest, stream dfdaemongrpc.Daemon_DownloadServer) error {
	ctx := stream.Context()
	m.Keep()
	// init peer task request, peer uses different peer id to generate every request
	peerTask := &peer.FileTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      req.Url,
			UrlMeta:  req.UrlMeta,
			PeerId:   idgen.PeerID(m.peerHost.Ip),
			PeerHost: m.peerHost,
		},
		Output:            req.Output,
		Limit:             req.Limit,
		DisableBackSource: req.DisableBackSource,
		Pattern:           req.Pattern,
		Callsystem:        req.Callsystem,
	}
	log := logger.With("peer", peerTask.PeerId, "component", "downloadService")

	peerTaskProgress, tiny, err := m.peerTaskManager.StartFileTask(ctx, peerTask)
	if err != nil {
		return status.Error(codes.Code(base.Code_UnknownError), fmt.Sprintf("%s", err))
	}
	if tiny != nil {
		if err := stream.Send(&dfdaemongrpc.DownResult{
			TaskId:          tiny.TaskID,
			PeerId:          tiny.PeerID,
			CompletedLength: uint64(len(tiny.Content)),
			Done:            true,
		}); err != nil {
			log.Errorf("send download result failed: %s", err)
			return err
		}
		log.Infof("tiny file, wrote to output")
		if req.Uid != 0 && req.Gid != 0 {
			if err = os.Chown(req.Output, int(req.Uid), int(req.Gid)); err != nil {
				log.Errorf("change own failed: %s", err)
				return err
			}
		}

		return nil
	}
	for {
		select {
		case p, ok := <-peerTaskProgress:
			if !ok {
				err = errors.New("progress closed unexpected")
				log.Errorf(err.Error())
				return status.Error(codes.Code(base.Code_UnknownError), err.Error())
			}
			if !p.State.Success {
				log.Errorf("task %s/%s failed: %d/%s", p.PeerID, p.TaskID, p.State.Code, p.State.Msg)
				return status.Error(codes.Code(p.State.Code), p.State.Msg)
			}
			if err := stream.Send(&dfdaemongrpc.DownResult{
				TaskId:          p.TaskID,
				PeerId:          p.PeerID,
				CompletedLength: uint64(p.CompletedLength),
				Done:            p.PeerTaskDone,
			}); err != nil {
				log.Errorf("send download result failed: %s", err)
				return err
			}
			// peer task sets PeerTaskDone to true only once
			if p.PeerTaskDone {
				p.DoneCallback()
				log.Infof("task %s/%s done", p.PeerID, p.TaskID)
				if req.Uid != 0 && req.Gid != 0 {
					log.Infof("change own to uid %d gid %d", req.Uid, req.Gid)
					if err = os.Chown(req.Output, int(req.Uid), int(req.Gid)); err != nil {
						log.Errorf("change own failed: %s", err)
						return err
					}
				}
				return nil
			}
		case <-ctx.Done():
			if err := stream.Send(&dfdaemongrpc.DownResult{
				CompletedLength: 0,
				Done:            true,
			}); err != nil {
				log.Errorf("failed to send download result: %v", err)
				return err
			}
			log.Infof("context done due to %s", ctx.Err())
			return status.Error(codes.Canceled, ctx.Err().Error())
		}
	}
}
