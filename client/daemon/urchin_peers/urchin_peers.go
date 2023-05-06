package urchin_peers

import (
	"d7y.io/api/pkg/apis/scheduler/v1"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"github.com/gin-gonic/gin"
	"net/http"
)

type UrchinPeer struct {
	PeerHost *scheduler.PeerHost
	PeerPort int
}

func NewPeer(peerHost *scheduler.PeerHost) *UrchinPeer {
	return &UrchinPeer{
		PeerHost: peerHost,
	}
}

func (up *UrchinPeer) SetPeerPort(peerPort int) {
	up.PeerPort = peerPort
}

func (up *UrchinPeer) GetPeer(ctx *gin.Context) {
	var params UrchinPeerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var (
		peerID = params.ID
	)

	//ToDo: Check peerID validation
	logger.Infof("get peer id: %s %#v", peerID)

	ctx.JSON(http.StatusOK, gin.H{
		"status_code":          0,
		"status_msg":           "",
		"id":                   peerID,
		"ip":                   up.PeerHost.Ip,
		"hostname":             up.PeerHost.Hostname,
		"domain":               up.PeerHost.Location,
		"center_id":            up.PeerHost.Idc,
		"cache_gc_time":        "",
		"cache_strategy":       "",
		"cache_disk_threshold": "",
		"api_port":             up.PeerPort,
	})
}

//ToDo
func GetUrchinPeers(ctx *gin.Context) {

	ctx.JSON(http.StatusOK, gin.H{
		"status_code": 0,
		"status_msg":  "",
		"peers":       "",
	})
}
