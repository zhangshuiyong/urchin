package urchin_peers

const (
	storagePrefixPeer    = "urchin:peer"
	PeersScoreExpireTime = 31
	PeersInfoExpireTime  = 60 * 2
	PeersLockExpireTime  = 10
	PeersInfoExpireLock  = "peersInfoExpireLock"
	PeersInfoExpireQue   = "peersInfoExpireQue"
	PeersInfoCreateQue   = "peersInfoCreateQue"
	PeersInfoTmpSortSet  = "tmpSortPeerInfoSet"
)

type UrchinPeerParams struct {
	ID string `uri:"peer_id" binding:"required"`
}

type UrchinPeerQueryParams struct {
	PageIndex        int    `form:"page_index,default=0" binding:"omitempty"`
	PageSize         int    `form:"page_size,default=100" binding:"omitempty"`
	SearchKey        string `form:"search_key" binding:"omitempty"`
	OrderBy          string `form:"order_by" binding:"omitempty"`
	SortBy           int    `form:"sort_by,default=1" binding:"omitempty"`
	CreatedAtLess    int64  `form:"created_at_less,default=0" binding:"omitempty"`
	CreatedAtGreater int64  `form:"created_at_greater,default=0" binding:"omitempty"`
}

type UrchinPeerInfo struct {
	Id              string `json:"id"`
	SecurityDomain  string `json:"domain"`
	Location        string `json:"center_id"`
	Hostname        string `json:"hostname"`
	Ip              string `json:"ip"`
	PeerPort        int    `json:"api_port"`
	StoreStrategy   string `json:"cache_strategy"`
	TaskExpireTime  int64  `json:"cache_gc_time"`
	DiskGCThreshold string `json:"cache_disk_threshold"`
}
