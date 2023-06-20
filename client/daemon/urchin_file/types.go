package urchin_file

import "mime/multipart"

type UploadFileRequest struct {
	DataMode         string                `form:"mode" binding:"required"`
	DatasetId        string                `form:"dataset_id" binding:"required"`
	DatasetVersionId string                `form:"dataset_version_id" binding:"required"`
	Digest           string                `form:"digest" binding:"required"`
	TotalSize        uint64                `form:"total_size,default=0" binding:"required,gte=0"`
	ChunkSize        uint64                `form:"chunk_size,default=0" binding:"omitempty,gte=0"`
	ChunkStart       uint64                `form:"chunk_start,default=0" binding:"omitempty,gte=0"`
	ChunkNum         uint64                `form:"chunk_num,default=0" binding:"omitempty,gte=0"`
	File             *multipart.FileHeader `form:"file" binding:"omitempty"`
}

type StoreChunkRequest struct {
	StoreDestination string
}

//ToDo: validate chunk condition: ChunkStart + ChunkSize <= TotalSize

type StatFileParams struct {
	DataMode         string `form:"mode" binding:"required"`
	DatasetId        string `form:"dataset_id" binding:"required"`
	DatasetVersionId string `form:"dataset_version_id" binding:"required"`
	Digest           string `form:"digest" binding:"required"`
	TotalSize        uint64 `form:"total_size,default=0" binding:"required,gte=0"`
	ChunkSize        uint64 `form:"chunk_size,default=0" binding:"omitempty,gte=0"`
	ChunkStart       uint64 `form:"chunk_start,default=0" binding:"omitempty,gte=0"`
	ChunkNum         uint64 `form:"chunk_num,default=0" binding:"omitempty,gte=0"`
}
