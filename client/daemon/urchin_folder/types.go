package urchin_folder

type UploadFolderRequest struct {
}

type StatFolderParams struct {
}

type FolderParams struct {
	ID        string `uri:"id" binding:"required"`
	FolderKey string `uri:"folder_key" binding:"required"`
}
