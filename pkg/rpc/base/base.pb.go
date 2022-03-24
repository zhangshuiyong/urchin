//
//     Copyright 2020 The Dragonfly Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.18.1
// source: pkg/rpc/base/base.proto

package base

import (
	_ "github.com/envoyproxy/protoc-gen-validate/validate"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Code int32

const (
	Code_X_UNSPECIFIED Code = 0
	// success code 200-299
	Code_Success Code = 200
	// framework can not find server node
	Code_ServerUnavailable Code = 500
	// common response error 1000-1999
	// client can be migrated to another scheduler/CDN
	Code_ResourceLacked   Code = 1000
	Code_BadRequest       Code = 1400
	Code_PeerTaskNotFound Code = 1404
	Code_UnknownError     Code = 1500
	Code_RequestTimeOut   Code = 1504
	// client response error 4000-4999
	Code_ClientError             Code = 4000
	Code_ClientPieceRequestFail  Code = 4001 // get piece task from other peer error
	Code_ClientScheduleTimeout   Code = 4002 // wait scheduler response timeout
	Code_ClientContextCanceled   Code = 4003
	Code_ClientWaitPieceReady    Code = 4004 // when target peer downloads from source slowly, should wait
	Code_ClientPieceDownloadFail Code = 4005
	Code_ClientRequestLimitFail  Code = 4006
	Code_ClientConnectionError   Code = 4007
	Code_ClientPieceNotFound     Code = 4404
	// scheduler response error 5000-5999
	Code_SchedError                     Code = 5000
	Code_SchedNeedBackSource            Code = 5001 // client should try to download from source
	Code_SchedPeerGone                  Code = 5002 // client should disconnect from scheduler
	Code_SchedPeerNotFound              Code = 5004 // peer not found in scheduler
	Code_SchedPeerPieceResultReportFail Code = 5005 // report piece
	Code_SchedTaskStatusError           Code = 5006 // task status is fail
	// cdnsystem response error 6000-6999
	Code_CDNError            Code = 6000
	Code_CDNTaskRegistryFail Code = 6001
	Code_CDNTaskDownloadFail Code = 6002
	Code_CDNTaskNotFound     Code = 6404
	// manager response error 7000-7999
	Code_InvalidResourceType Code = 7001
)

// Enum value maps for Code.
var (
	Code_name = map[int32]string{
		0:    "X_UNSPECIFIED",
		200:  "Success",
		500:  "ServerUnavailable",
		1000: "ResourceLacked",
		1400: "BadRequest",
		1404: "PeerTaskNotFound",
		1500: "UnknownError",
		1504: "RequestTimeOut",
		4000: "ClientError",
		4001: "ClientPieceRequestFail",
		4002: "ClientScheduleTimeout",
		4003: "ClientContextCanceled",
		4004: "ClientWaitPieceReady",
		4005: "ClientPieceDownloadFail",
		4006: "ClientRequestLimitFail",
		4007: "ClientConnectionError",
		4404: "ClientPieceNotFound",
		5000: "SchedError",
		5001: "SchedNeedBackSource",
		5002: "SchedPeerGone",
		5004: "SchedPeerNotFound",
		5005: "SchedPeerPieceResultReportFail",
		5006: "SchedTaskStatusError",
		6000: "CDNError",
		6001: "CDNTaskRegistryFail",
		6002: "CDNTaskDownloadFail",
		6404: "CDNTaskNotFound",
		7001: "InvalidResourceType",
	}
	Code_value = map[string]int32{
		"X_UNSPECIFIED":                  0,
		"Success":                        200,
		"ServerUnavailable":              500,
		"ResourceLacked":                 1000,
		"BadRequest":                     1400,
		"PeerTaskNotFound":               1404,
		"UnknownError":                   1500,
		"RequestTimeOut":                 1504,
		"ClientError":                    4000,
		"ClientPieceRequestFail":         4001,
		"ClientScheduleTimeout":          4002,
		"ClientContextCanceled":          4003,
		"ClientWaitPieceReady":           4004,
		"ClientPieceDownloadFail":        4005,
		"ClientRequestLimitFail":         4006,
		"ClientConnectionError":          4007,
		"ClientPieceNotFound":            4404,
		"SchedError":                     5000,
		"SchedNeedBackSource":            5001,
		"SchedPeerGone":                  5002,
		"SchedPeerNotFound":              5004,
		"SchedPeerPieceResultReportFail": 5005,
		"SchedTaskStatusError":           5006,
		"CDNError":                       6000,
		"CDNTaskRegistryFail":            6001,
		"CDNTaskDownloadFail":            6002,
		"CDNTaskNotFound":                6404,
		"InvalidResourceType":            7001,
	}
)

func (x Code) Enum() *Code {
	p := new(Code)
	*p = x
	return p
}

func (x Code) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Code) Descriptor() protoreflect.EnumDescriptor {
	return file_pkg_rpc_base_base_proto_enumTypes[0].Descriptor()
}

func (Code) Type() protoreflect.EnumType {
	return &file_pkg_rpc_base_base_proto_enumTypes[0]
}

func (x Code) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Code.Descriptor instead.
func (Code) EnumDescriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{0}
}

type PieceStyle int32

const (
	PieceStyle_PLAIN PieceStyle = 0
)

// Enum value maps for PieceStyle.
var (
	PieceStyle_name = map[int32]string{
		0: "PLAIN",
	}
	PieceStyle_value = map[string]int32{
		"PLAIN": 0,
	}
)

func (x PieceStyle) Enum() *PieceStyle {
	p := new(PieceStyle)
	*p = x
	return p
}

func (x PieceStyle) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (PieceStyle) Descriptor() protoreflect.EnumDescriptor {
	return file_pkg_rpc_base_base_proto_enumTypes[1].Descriptor()
}

func (PieceStyle) Type() protoreflect.EnumType {
	return &file_pkg_rpc_base_base_proto_enumTypes[1]
}

func (x PieceStyle) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use PieceStyle.Descriptor instead.
func (PieceStyle) EnumDescriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{1}
}

type SizeScope int32

const (
	// size > one piece size
	SizeScope_NORMAL SizeScope = 0
	// 128 byte < size <= one piece size and be plain type
	SizeScope_SMALL SizeScope = 1
	// size <= 128 byte and be plain type
	SizeScope_TINY SizeScope = 2
)

// Enum value maps for SizeScope.
var (
	SizeScope_name = map[int32]string{
		0: "NORMAL",
		1: "SMALL",
		2: "TINY",
	}
	SizeScope_value = map[string]int32{
		"NORMAL": 0,
		"SMALL":  1,
		"TINY":   2,
	}
)

func (x SizeScope) Enum() *SizeScope {
	p := new(SizeScope)
	*p = x
	return p
}

func (x SizeScope) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (SizeScope) Descriptor() protoreflect.EnumDescriptor {
	return file_pkg_rpc_base_base_proto_enumTypes[2].Descriptor()
}

func (SizeScope) Type() protoreflect.EnumType {
	return &file_pkg_rpc_base_base_proto_enumTypes[2]
}

func (x SizeScope) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use SizeScope.Descriptor instead.
func (SizeScope) EnumDescriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{2}
}

type GrpcDfError struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code    Code   `protobuf:"varint,1,opt,name=code,proto3,enum=base.Code" json:"code,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *GrpcDfError) Reset() {
	*x = GrpcDfError{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_base_base_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GrpcDfError) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GrpcDfError) ProtoMessage() {}

func (x *GrpcDfError) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_base_base_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GrpcDfError.ProtoReflect.Descriptor instead.
func (*GrpcDfError) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{0}
}

func (x *GrpcDfError) GetCode() Code {
	if x != nil {
		return x.Code
	}
	return Code_X_UNSPECIFIED
}

func (x *GrpcDfError) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

// UrlMeta describes url meta info.
type UrlMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// digest checks integrity of url content, for example md5:xxx or sha256:yyy
	Digest string `protobuf:"bytes,1,opt,name=digest,proto3" json:"digest,omitempty"`
	// url tag identifies different task for same url, conflict with digest
	Tag string `protobuf:"bytes,2,opt,name=tag,proto3" json:"tag,omitempty"`
	// content range for url
	Range string `protobuf:"bytes,3,opt,name=range,proto3" json:"range,omitempty"`
	// filter url used to generate task id
	Filter string `protobuf:"bytes,4,opt,name=filter,proto3" json:"filter,omitempty"`
	// other url header infos
	Header map[string]string `protobuf:"bytes,5,rep,name=header,proto3" json:"header,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *UrlMeta) Reset() {
	*x = UrlMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_base_base_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UrlMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UrlMeta) ProtoMessage() {}

func (x *UrlMeta) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_base_base_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UrlMeta.ProtoReflect.Descriptor instead.
func (*UrlMeta) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{1}
}

func (x *UrlMeta) GetDigest() string {
	if x != nil {
		return x.Digest
	}
	return ""
}

func (x *UrlMeta) GetTag() string {
	if x != nil {
		return x.Tag
	}
	return ""
}

func (x *UrlMeta) GetRange() string {
	if x != nil {
		return x.Range
	}
	return ""
}

func (x *UrlMeta) GetFilter() string {
	if x != nil {
		return x.Filter
	}
	return ""
}

func (x *UrlMeta) GetHeader() map[string]string {
	if x != nil {
		return x.Header
	}
	return nil
}

type HostLoad struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// cpu usage
	CpuRatio float32 `protobuf:"fixed32,1,opt,name=cpu_ratio,json=cpuRatio,proto3" json:"cpu_ratio,omitempty"`
	// memory usage
	MemRatio float32 `protobuf:"fixed32,2,opt,name=mem_ratio,json=memRatio,proto3" json:"mem_ratio,omitempty"`
	// disk space usage
	DiskRatio float32 `protobuf:"fixed32,3,opt,name=disk_ratio,json=diskRatio,proto3" json:"disk_ratio,omitempty"`
}

func (x *HostLoad) Reset() {
	*x = HostLoad{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_base_base_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HostLoad) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HostLoad) ProtoMessage() {}

func (x *HostLoad) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_base_base_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HostLoad.ProtoReflect.Descriptor instead.
func (*HostLoad) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{2}
}

func (x *HostLoad) GetCpuRatio() float32 {
	if x != nil {
		return x.CpuRatio
	}
	return 0
}

func (x *HostLoad) GetMemRatio() float32 {
	if x != nil {
		return x.MemRatio
	}
	return 0
}

func (x *HostLoad) GetDiskRatio() float32 {
	if x != nil {
		return x.DiskRatio
	}
	return 0
}

type PieceTaskRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskId string `protobuf:"bytes,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	SrcPid string `protobuf:"bytes,2,opt,name=src_pid,json=srcPid,proto3" json:"src_pid,omitempty"`
	DstPid string `protobuf:"bytes,3,opt,name=dst_pid,json=dstPid,proto3" json:"dst_pid,omitempty"`
	// piece number
	StartNum uint32 `protobuf:"varint,4,opt,name=start_num,json=startNum,proto3" json:"start_num,omitempty"`
	// expected piece count, limit = 0 represent request pieces as many shards as possible
	Limit uint32 `protobuf:"varint,5,opt,name=limit,proto3" json:"limit,omitempty"`
}

func (x *PieceTaskRequest) Reset() {
	*x = PieceTaskRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_base_base_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PieceTaskRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PieceTaskRequest) ProtoMessage() {}

func (x *PieceTaskRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_base_base_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PieceTaskRequest.ProtoReflect.Descriptor instead.
func (*PieceTaskRequest) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{3}
}

func (x *PieceTaskRequest) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *PieceTaskRequest) GetSrcPid() string {
	if x != nil {
		return x.SrcPid
	}
	return ""
}

func (x *PieceTaskRequest) GetDstPid() string {
	if x != nil {
		return x.DstPid
	}
	return ""
}

func (x *PieceTaskRequest) GetStartNum() uint32 {
	if x != nil {
		return x.StartNum
	}
	return 0
}

func (x *PieceTaskRequest) GetLimit() uint32 {
	if x != nil {
		return x.Limit
	}
	return 0
}

type PieceInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// piece_num < 0 represent start report piece flag
	PieceNum    int32      `protobuf:"varint,1,opt,name=piece_num,json=pieceNum,proto3" json:"piece_num,omitempty"`
	RangeStart  uint64     `protobuf:"varint,2,opt,name=range_start,json=rangeStart,proto3" json:"range_start,omitempty"`
	RangeSize   uint32     `protobuf:"varint,3,opt,name=range_size,json=rangeSize,proto3" json:"range_size,omitempty"`
	PieceMd5    string     `protobuf:"bytes,4,opt,name=piece_md5,json=pieceMd5,proto3" json:"piece_md5,omitempty"`
	PieceOffset uint64     `protobuf:"varint,5,opt,name=piece_offset,json=pieceOffset,proto3" json:"piece_offset,omitempty"`
	PieceStyle  PieceStyle `protobuf:"varint,6,opt,name=piece_style,json=pieceStyle,proto3,enum=base.PieceStyle" json:"piece_style,omitempty"`
	// total time(millisecond) consumed
	DownloadCost uint64 `protobuf:"varint,7,opt,name=download_cost,json=downloadCost,proto3" json:"download_cost,omitempty"`
}

func (x *PieceInfo) Reset() {
	*x = PieceInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_base_base_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PieceInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PieceInfo) ProtoMessage() {}

func (x *PieceInfo) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_base_base_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PieceInfo.ProtoReflect.Descriptor instead.
func (*PieceInfo) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{4}
}

func (x *PieceInfo) GetPieceNum() int32 {
	if x != nil {
		return x.PieceNum
	}
	return 0
}

func (x *PieceInfo) GetRangeStart() uint64 {
	if x != nil {
		return x.RangeStart
	}
	return 0
}

func (x *PieceInfo) GetRangeSize() uint32 {
	if x != nil {
		return x.RangeSize
	}
	return 0
}

func (x *PieceInfo) GetPieceMd5() string {
	if x != nil {
		return x.PieceMd5
	}
	return ""
}

func (x *PieceInfo) GetPieceOffset() uint64 {
	if x != nil {
		return x.PieceOffset
	}
	return 0
}

func (x *PieceInfo) GetPieceStyle() PieceStyle {
	if x != nil {
		return x.PieceStyle
	}
	return PieceStyle_PLAIN
}

func (x *PieceInfo) GetDownloadCost() uint64 {
	if x != nil {
		return x.DownloadCost
	}
	return 0
}

type PiecePacket struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskId string `protobuf:"bytes,2,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	DstPid string `protobuf:"bytes,3,opt,name=dst_pid,json=dstPid,proto3" json:"dst_pid,omitempty"`
	// ip:port
	DstAddr    string       `protobuf:"bytes,4,opt,name=dst_addr,json=dstAddr,proto3" json:"dst_addr,omitempty"`
	PieceInfos []*PieceInfo `protobuf:"bytes,5,rep,name=piece_infos,json=pieceInfos,proto3" json:"piece_infos,omitempty"`
	// total piece count for url, total_piece represent total piece is unknown
	TotalPiece int32 `protobuf:"varint,6,opt,name=total_piece,json=totalPiece,proto3" json:"total_piece,omitempty"`
	// content_length < 0 represent content length is unknown
	ContentLength int64 `protobuf:"varint,7,opt,name=content_length,json=contentLength,proto3" json:"content_length,omitempty"`
	// sha256 code of all piece md5
	PieceMd5Sign string `protobuf:"bytes,8,opt,name=piece_md5_sign,json=pieceMd5Sign,proto3" json:"piece_md5_sign,omitempty"`
}

func (x *PiecePacket) Reset() {
	*x = PiecePacket{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_base_base_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PiecePacket) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PiecePacket) ProtoMessage() {}

func (x *PiecePacket) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_base_base_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PiecePacket.ProtoReflect.Descriptor instead.
func (*PiecePacket) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_base_base_proto_rawDescGZIP(), []int{5}
}

func (x *PiecePacket) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *PiecePacket) GetDstPid() string {
	if x != nil {
		return x.DstPid
	}
	return ""
}

func (x *PiecePacket) GetDstAddr() string {
	if x != nil {
		return x.DstAddr
	}
	return ""
}

func (x *PiecePacket) GetPieceInfos() []*PieceInfo {
	if x != nil {
		return x.PieceInfos
	}
	return nil
}

func (x *PiecePacket) GetTotalPiece() int32 {
	if x != nil {
		return x.TotalPiece
	}
	return 0
}

func (x *PiecePacket) GetContentLength() int64 {
	if x != nil {
		return x.ContentLength
	}
	return 0
}

func (x *PiecePacket) GetPieceMd5Sign() string {
	if x != nil {
		return x.PieceMd5Sign
	}
	return ""
}

var File_pkg_rpc_base_base_proto protoreflect.FileDescriptor

var file_pkg_rpc_base_base_proto_rawDesc = []byte{
	0x0a, 0x17, 0x70, 0x6b, 0x67, 0x2f, 0x72, 0x70, 0x63, 0x2f, 0x62, 0x61, 0x73, 0x65, 0x2f, 0x62,
	0x61, 0x73, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x62, 0x61, 0x73, 0x65, 0x1a,
	0x17, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61,
	0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x51, 0x0a, 0x0b, 0x47, 0x72, 0x70, 0x63,
	0x44, 0x66, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x28, 0x0a, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0a, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x43, 0x6f, 0x64,
	0x65, 0x42, 0x08, 0xfa, 0x42, 0x05, 0x82, 0x01, 0x02, 0x10, 0x01, 0x52, 0x04, 0x63, 0x6f, 0x64,
	0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x93, 0x02, 0x0a, 0x07,
	0x55, 0x72, 0x6c, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x3f, 0x0a, 0x06, 0x64, 0x69, 0x67, 0x65, 0x73,
	0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x27, 0xfa, 0x42, 0x24, 0x72, 0x22, 0x32, 0x1d,
	0x5e, 0x28, 0x6d, 0x64, 0x35, 0x29, 0x7c, 0x28, 0x73, 0x68, 0x61, 0x32, 0x35, 0x36, 0x29, 0x3a,
	0x5b, 0x41, 0x2d, 0x46, 0x61, 0x2d, 0x66, 0x30, 0x2d, 0x39, 0x5d, 0x2b, 0x24, 0xd0, 0x01, 0x01,
	0x52, 0x06, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x74, 0x61, 0x67, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x74, 0x61, 0x67, 0x12, 0x2f, 0x0a, 0x05, 0x72, 0x61,
	0x6e, 0x67, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x42, 0x19, 0xfa, 0x42, 0x16, 0x72, 0x14,
	0x32, 0x0f, 0x5e, 0x5b, 0x30, 0x2d, 0x39, 0x5d, 0x2b, 0x2d, 0x5b, 0x30, 0x2d, 0x39, 0x5d, 0x2b,
	0x24, 0xd0, 0x01, 0x01, 0x52, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x66,
	0x69, 0x6c, 0x74, 0x65, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x69, 0x6c,
	0x74, 0x65, 0x72, 0x12, 0x31, 0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x05, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x55, 0x72, 0x6c, 0x4d, 0x65,
	0x74, 0x61, 0x2e, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06,
	0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x1a, 0x39, 0x0a, 0x0b, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38,
	0x01, 0x22, 0x96, 0x01, 0x0a, 0x08, 0x48, 0x6f, 0x73, 0x74, 0x4c, 0x6f, 0x61, 0x64, 0x12, 0x2c,
	0x0a, 0x09, 0x63, 0x70, 0x75, 0x5f, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x02, 0x42, 0x0f, 0xfa, 0x42, 0x0c, 0x0a, 0x0a, 0x1d, 0x00, 0x00, 0x80, 0x3f, 0x2d, 0x00, 0x00,
	0x00, 0x00, 0x52, 0x08, 0x63, 0x70, 0x75, 0x52, 0x61, 0x74, 0x69, 0x6f, 0x12, 0x2c, 0x0a, 0x09,
	0x6d, 0x65, 0x6d, 0x5f, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x02, 0x42,
	0x0f, 0xfa, 0x42, 0x0c, 0x0a, 0x0a, 0x1d, 0x00, 0x00, 0x80, 0x3f, 0x2d, 0x00, 0x00, 0x00, 0x00,
	0x52, 0x08, 0x6d, 0x65, 0x6d, 0x52, 0x61, 0x74, 0x69, 0x6f, 0x12, 0x2e, 0x0a, 0x0a, 0x64, 0x69,
	0x73, 0x6b, 0x5f, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x18, 0x03, 0x20, 0x01, 0x28, 0x02, 0x42, 0x0f,
	0xfa, 0x42, 0x0c, 0x0a, 0x0a, 0x1d, 0x00, 0x00, 0x80, 0x3f, 0x2d, 0x00, 0x00, 0x00, 0x00, 0x52,
	0x09, 0x64, 0x69, 0x73, 0x6b, 0x52, 0x61, 0x74, 0x69, 0x6f, 0x22, 0xbd, 0x01, 0x0a, 0x10, 0x50,
	0x69, 0x65, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x20, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x42, 0x07, 0xfa, 0x42, 0x04, 0x72, 0x02, 0x10, 0x01, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49,
	0x64, 0x12, 0x20, 0x0a, 0x07, 0x73, 0x72, 0x63, 0x5f, 0x70, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x72, 0x02, 0x10, 0x01, 0x52, 0x06, 0x73, 0x72, 0x63,
	0x50, 0x69, 0x64, 0x12, 0x20, 0x0a, 0x07, 0x64, 0x73, 0x74, 0x5f, 0x70, 0x69, 0x64, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x72, 0x02, 0x10, 0x01, 0x52, 0x06, 0x64,
	0x73, 0x74, 0x50, 0x69, 0x64, 0x12, 0x24, 0x0a, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x6e,
	0x75, 0x6d, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0d, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x2a, 0x02, 0x28,
	0x00, 0x52, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74, 0x4e, 0x75, 0x6d, 0x12, 0x1d, 0x0a, 0x05, 0x6c,
	0x69, 0x6d, 0x69, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0d, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x2a,
	0x02, 0x28, 0x00, 0x52, 0x05, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x22, 0xe1, 0x02, 0x0a, 0x09, 0x50,
	0x69, 0x65, 0x63, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x1b, 0x0a, 0x09, 0x70, 0x69, 0x65, 0x63,
	0x65, 0x5f, 0x6e, 0x75, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x70, 0x69, 0x65,
	0x63, 0x65, 0x4e, 0x75, 0x6d, 0x12, 0x28, 0x0a, 0x0b, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x73,
	0x74, 0x61, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x32,
	0x02, 0x28, 0x00, 0x52, 0x0a, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x53, 0x74, 0x61, 0x72, 0x74, 0x12,
	0x26, 0x0a, 0x0a, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x0d, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x2a, 0x02, 0x28, 0x00, 0x52, 0x09, 0x72, 0x61,
	0x6e, 0x67, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x58, 0x0a, 0x09, 0x70, 0x69, 0x65, 0x63, 0x65,
	0x5f, 0x6d, 0x64, 0x35, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x42, 0x3b, 0xfa, 0x42, 0x38, 0x72,
	0x36, 0x32, 0x31, 0x28, 0x5b, 0x61, 0x2d, 0x66, 0x5c, 0x64, 0x5d, 0x7b, 0x33, 0x32, 0x7d, 0x7c,
	0x5b, 0x41, 0x2d, 0x46, 0x5c, 0x64, 0x5d, 0x7b, 0x33, 0x32, 0x7d, 0x7c, 0x5b, 0x61, 0x2d, 0x66,
	0x5c, 0x64, 0x5d, 0x7b, 0x31, 0x36, 0x7d, 0x7c, 0x5b, 0x41, 0x2d, 0x46, 0x5c, 0x64, 0x5d, 0x7b,
	0x31, 0x36, 0x7d, 0x29, 0xd0, 0x01, 0x01, 0x52, 0x08, 0x70, 0x69, 0x65, 0x63, 0x65, 0x4d, 0x64,
	0x35, 0x12, 0x2a, 0x0a, 0x0c, 0x70, 0x69, 0x65, 0x63, 0x65, 0x5f, 0x6f, 0x66, 0x66, 0x73, 0x65,
	0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x32, 0x02, 0x28, 0x00,
	0x52, 0x0b, 0x70, 0x69, 0x65, 0x63, 0x65, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x31, 0x0a,
	0x0b, 0x70, 0x69, 0x65, 0x63, 0x65, 0x5f, 0x73, 0x74, 0x79, 0x6c, 0x65, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x10, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x50, 0x69, 0x65, 0x63, 0x65, 0x53,
	0x74, 0x79, 0x6c, 0x65, 0x52, 0x0a, 0x70, 0x69, 0x65, 0x63, 0x65, 0x53, 0x74, 0x79, 0x6c, 0x65,
	0x12, 0x2c, 0x0a, 0x0d, 0x64, 0x6f, 0x77, 0x6e, 0x6c, 0x6f, 0x61, 0x64, 0x5f, 0x63, 0x6f, 0x73,
	0x74, 0x18, 0x07, 0x20, 0x01, 0x28, 0x04, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x32, 0x02, 0x28, 0x00,
	0x52, 0x0c, 0x64, 0x6f, 0x77, 0x6e, 0x6c, 0x6f, 0x61, 0x64, 0x43, 0x6f, 0x73, 0x74, 0x22, 0x95,
	0x02, 0x0a, 0x0b, 0x50, 0x69, 0x65, 0x63, 0x65, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x12, 0x20,
	0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x42,
	0x07, 0xfa, 0x42, 0x04, 0x72, 0x02, 0x10, 0x01, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64,
	0x12, 0x20, 0x0a, 0x07, 0x64, 0x73, 0x74, 0x5f, 0x70, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x09, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x72, 0x02, 0x10, 0x01, 0x52, 0x06, 0x64, 0x73, 0x74, 0x50,
	0x69, 0x64, 0x12, 0x22, 0x0a, 0x08, 0x64, 0x73, 0x74, 0x5f, 0x61, 0x64, 0x64, 0x72, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x09, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x72, 0x02, 0x10, 0x01, 0x52, 0x07, 0x64,
	0x73, 0x74, 0x41, 0x64, 0x64, 0x72, 0x12, 0x30, 0x0a, 0x0b, 0x70, 0x69, 0x65, 0x63, 0x65, 0x5f,
	0x69, 0x6e, 0x66, 0x6f, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x62, 0x61,
	0x73, 0x65, 0x2e, 0x50, 0x69, 0x65, 0x63, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x0a, 0x70, 0x69,
	0x65, 0x63, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x12, 0x1f, 0x0a, 0x0b, 0x74, 0x6f, 0x74, 0x61,
	0x6c, 0x5f, 0x70, 0x69, 0x65, 0x63, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0a, 0x74,
	0x6f, 0x74, 0x61, 0x6c, 0x50, 0x69, 0x65, 0x63, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x63, 0x6f, 0x6e,
	0x74, 0x65, 0x6e, 0x74, 0x5f, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x18, 0x07, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x0d, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68,
	0x12, 0x24, 0x0a, 0x0e, 0x70, 0x69, 0x65, 0x63, 0x65, 0x5f, 0x6d, 0x64, 0x35, 0x5f, 0x73, 0x69,
	0x67, 0x6e, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x70, 0x69, 0x65, 0x63, 0x65, 0x4d,
	0x64, 0x35, 0x53, 0x69, 0x67, 0x6e, 0x2a, 0xa1, 0x05, 0x0a, 0x04, 0x43, 0x6f, 0x64, 0x65, 0x12,
	0x11, 0x0a, 0x0d, 0x58, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44,
	0x10, 0x00, 0x12, 0x0c, 0x0a, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x10, 0xc8, 0x01,
	0x12, 0x16, 0x0a, 0x11, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x55, 0x6e, 0x61, 0x76, 0x61, 0x69,
	0x6c, 0x61, 0x62, 0x6c, 0x65, 0x10, 0xf4, 0x03, 0x12, 0x13, 0x0a, 0x0e, 0x52, 0x65, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x4c, 0x61, 0x63, 0x6b, 0x65, 0x64, 0x10, 0xe8, 0x07, 0x12, 0x0f, 0x0a,
	0x0a, 0x42, 0x61, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x10, 0xf8, 0x0a, 0x12, 0x15,
	0x0a, 0x10, 0x50, 0x65, 0x65, 0x72, 0x54, 0x61, 0x73, 0x6b, 0x4e, 0x6f, 0x74, 0x46, 0x6f, 0x75,
	0x6e, 0x64, 0x10, 0xfc, 0x0a, 0x12, 0x11, 0x0a, 0x0c, 0x55, 0x6e, 0x6b, 0x6e, 0x6f, 0x77, 0x6e,
	0x45, 0x72, 0x72, 0x6f, 0x72, 0x10, 0xdc, 0x0b, 0x12, 0x13, 0x0a, 0x0e, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x4f, 0x75, 0x74, 0x10, 0xe0, 0x0b, 0x12, 0x10, 0x0a,
	0x0b, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x10, 0xa0, 0x1f, 0x12,
	0x1b, 0x0a, 0x16, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x50, 0x69, 0x65, 0x63, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x46, 0x61, 0x69, 0x6c, 0x10, 0xa1, 0x1f, 0x12, 0x1a, 0x0a, 0x15,
	0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x54, 0x69,
	0x6d, 0x65, 0x6f, 0x75, 0x74, 0x10, 0xa2, 0x1f, 0x12, 0x1a, 0x0a, 0x15, 0x43, 0x6c, 0x69, 0x65,
	0x6e, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x43, 0x61, 0x6e, 0x63, 0x65, 0x6c, 0x65,
	0x64, 0x10, 0xa3, 0x1f, 0x12, 0x19, 0x0a, 0x14, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x57, 0x61,
	0x69, 0x74, 0x50, 0x69, 0x65, 0x63, 0x65, 0x52, 0x65, 0x61, 0x64, 0x79, 0x10, 0xa4, 0x1f, 0x12,
	0x1c, 0x0a, 0x17, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x50, 0x69, 0x65, 0x63, 0x65, 0x44, 0x6f,
	0x77, 0x6e, 0x6c, 0x6f, 0x61, 0x64, 0x46, 0x61, 0x69, 0x6c, 0x10, 0xa5, 0x1f, 0x12, 0x1b, 0x0a,
	0x16, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x4c, 0x69,
	0x6d, 0x69, 0x74, 0x46, 0x61, 0x69, 0x6c, 0x10, 0xa6, 0x1f, 0x12, 0x1a, 0x0a, 0x15, 0x43, 0x6c,
	0x69, 0x65, 0x6e, 0x74, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x45, 0x72,
	0x72, 0x6f, 0x72, 0x10, 0xa7, 0x1f, 0x12, 0x18, 0x0a, 0x13, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74,
	0x50, 0x69, 0x65, 0x63, 0x65, 0x4e, 0x6f, 0x74, 0x46, 0x6f, 0x75, 0x6e, 0x64, 0x10, 0xb4, 0x22,
	0x12, 0x0f, 0x0a, 0x0a, 0x53, 0x63, 0x68, 0x65, 0x64, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x10, 0x88,
	0x27, 0x12, 0x18, 0x0a, 0x13, 0x53, 0x63, 0x68, 0x65, 0x64, 0x4e, 0x65, 0x65, 0x64, 0x42, 0x61,
	0x63, 0x6b, 0x53, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x10, 0x89, 0x27, 0x12, 0x12, 0x0a, 0x0d, 0x53,
	0x63, 0x68, 0x65, 0x64, 0x50, 0x65, 0x65, 0x72, 0x47, 0x6f, 0x6e, 0x65, 0x10, 0x8a, 0x27, 0x12,
	0x16, 0x0a, 0x11, 0x53, 0x63, 0x68, 0x65, 0x64, 0x50, 0x65, 0x65, 0x72, 0x4e, 0x6f, 0x74, 0x46,
	0x6f, 0x75, 0x6e, 0x64, 0x10, 0x8c, 0x27, 0x12, 0x23, 0x0a, 0x1e, 0x53, 0x63, 0x68, 0x65, 0x64,
	0x50, 0x65, 0x65, 0x72, 0x50, 0x69, 0x65, 0x63, 0x65, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52,
	0x65, 0x70, 0x6f, 0x72, 0x74, 0x46, 0x61, 0x69, 0x6c, 0x10, 0x8d, 0x27, 0x12, 0x19, 0x0a, 0x14,
	0x53, 0x63, 0x68, 0x65, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45,
	0x72, 0x72, 0x6f, 0x72, 0x10, 0x8e, 0x27, 0x12, 0x0d, 0x0a, 0x08, 0x43, 0x44, 0x4e, 0x45, 0x72,
	0x72, 0x6f, 0x72, 0x10, 0xf0, 0x2e, 0x12, 0x18, 0x0a, 0x13, 0x43, 0x44, 0x4e, 0x54, 0x61, 0x73,
	0x6b, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x72, 0x79, 0x46, 0x61, 0x69, 0x6c, 0x10, 0xf1, 0x2e,
	0x12, 0x18, 0x0a, 0x13, 0x43, 0x44, 0x4e, 0x54, 0x61, 0x73, 0x6b, 0x44, 0x6f, 0x77, 0x6e, 0x6c,
	0x6f, 0x61, 0x64, 0x46, 0x61, 0x69, 0x6c, 0x10, 0xf2, 0x2e, 0x12, 0x14, 0x0a, 0x0f, 0x43, 0x44,
	0x4e, 0x54, 0x61, 0x73, 0x6b, 0x4e, 0x6f, 0x74, 0x46, 0x6f, 0x75, 0x6e, 0x64, 0x10, 0x84, 0x32,
	0x12, 0x18, 0x0a, 0x13, 0x49, 0x6e, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x52, 0x65, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x54, 0x79, 0x70, 0x65, 0x10, 0xd9, 0x36, 0x2a, 0x17, 0x0a, 0x0a, 0x50, 0x69,
	0x65, 0x63, 0x65, 0x53, 0x74, 0x79, 0x6c, 0x65, 0x12, 0x09, 0x0a, 0x05, 0x50, 0x4c, 0x41, 0x49,
	0x4e, 0x10, 0x00, 0x2a, 0x2c, 0x0a, 0x09, 0x53, 0x69, 0x7a, 0x65, 0x53, 0x63, 0x6f, 0x70, 0x65,
	0x12, 0x0a, 0x0a, 0x06, 0x4e, 0x4f, 0x52, 0x4d, 0x41, 0x4c, 0x10, 0x00, 0x12, 0x09, 0x0a, 0x05,
	0x53, 0x4d, 0x41, 0x4c, 0x4c, 0x10, 0x01, 0x12, 0x08, 0x0a, 0x04, 0x54, 0x49, 0x4e, 0x59, 0x10,
	0x02, 0x42, 0x22, 0x5a, 0x20, 0x64, 0x37, 0x79, 0x2e, 0x69, 0x6f, 0x2f, 0x64, 0x72, 0x61, 0x67,
	0x6f, 0x6e, 0x66, 0x6c, 0x79, 0x2f, 0x76, 0x32, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x72, 0x70, 0x63,
	0x2f, 0x62, 0x61, 0x73, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_rpc_base_base_proto_rawDescOnce sync.Once
	file_pkg_rpc_base_base_proto_rawDescData = file_pkg_rpc_base_base_proto_rawDesc
)

func file_pkg_rpc_base_base_proto_rawDescGZIP() []byte {
	file_pkg_rpc_base_base_proto_rawDescOnce.Do(func() {
		file_pkg_rpc_base_base_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_rpc_base_base_proto_rawDescData)
	})
	return file_pkg_rpc_base_base_proto_rawDescData
}

var file_pkg_rpc_base_base_proto_enumTypes = make([]protoimpl.EnumInfo, 3)
var file_pkg_rpc_base_base_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_pkg_rpc_base_base_proto_goTypes = []interface{}{
	(Code)(0),                // 0: base.Code
	(PieceStyle)(0),          // 1: base.PieceStyle
	(SizeScope)(0),           // 2: base.SizeScope
	(*GrpcDfError)(nil),      // 3: base.GrpcDfError
	(*UrlMeta)(nil),          // 4: base.UrlMeta
	(*HostLoad)(nil),         // 5: base.HostLoad
	(*PieceTaskRequest)(nil), // 6: base.PieceTaskRequest
	(*PieceInfo)(nil),        // 7: base.PieceInfo
	(*PiecePacket)(nil),      // 8: base.PiecePacket
	nil,                      // 9: base.UrlMeta.HeaderEntry
}
var file_pkg_rpc_base_base_proto_depIdxs = []int32{
	0, // 0: base.GrpcDfError.code:type_name -> base.Code
	9, // 1: base.UrlMeta.header:type_name -> base.UrlMeta.HeaderEntry
	1, // 2: base.PieceInfo.piece_style:type_name -> base.PieceStyle
	7, // 3: base.PiecePacket.piece_infos:type_name -> base.PieceInfo
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_pkg_rpc_base_base_proto_init() }
func file_pkg_rpc_base_base_proto_init() {
	if File_pkg_rpc_base_base_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_rpc_base_base_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GrpcDfError); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_base_base_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UrlMeta); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_base_base_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HostLoad); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_base_base_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PieceTaskRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_base_base_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PieceInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_base_base_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PiecePacket); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_rpc_base_base_proto_rawDesc,
			NumEnums:      3,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_rpc_base_base_proto_goTypes,
		DependencyIndexes: file_pkg_rpc_base_base_proto_depIdxs,
		EnumInfos:         file_pkg_rpc_base_base_proto_enumTypes,
		MessageInfos:      file_pkg_rpc_base_base_proto_msgTypes,
	}.Build()
	File_pkg_rpc_base_base_proto = out.File
	file_pkg_rpc_base_base_proto_rawDesc = nil
	file_pkg_rpc_base_base_proto_goTypes = nil
	file_pkg_rpc_base_base_proto_depIdxs = nil
}
