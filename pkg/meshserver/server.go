package meshserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/edgemesh/pkg/apis/config/defaults"
	"github.com/kubeedge/edgemesh/pkg/apis/config/v1alpha1"
)

type MeshServer struct {
	Config *v1alpha1.MeshServer
}

func NewMeshServer(c *v1alpha1.MeshServer) *MeshServer {
	ms := &MeshServer{
		Config: c,
	}
	return ms
}

// Wrapper function to start the WebSocket server
func (ms *MeshServer) startUnixsocketServer(address string) {
	uds := NewUnixDomainSocket(address)
	uds.SetContextHandler(func(context string) string {
		// receive message from SocketClient
		msg, err := ExtractMessage(context)
		if err != nil {
			klog.Errorf("Failed to extract message: %v", err)
			return feedbackError(err, msg)
		}
		klog.Infof("uds server receives msg: %+v", msg)

		// Send message to EdgeTunnel
		resp, err := beehiveContext.SendSync(defaults.EdgeTunnelModuleName, *msg, 5*time.Second)
		if err != nil {
			klog.Errorf("cannot send message to EdgeTunnel: %v", err)
			return feedbackError(err, msg)
		}
		klog.Infof("uds server send resp: %v", resp)
		return resp.String()
	})

	klog.Info("start unix domain socket server")
	if err := uds.StartServer(); err != nil {
		klog.Exitf("failed to start uds server: %v", err)
		return
	}
}

func Register(meshServer *v1alpha1.MeshServer) error {
	mesh := NewMeshServer(meshServer)
	// initDBTable(meta)
	core.Register(mesh)
	return nil
}

func (*MeshServer) Name() string {
	return defaults.MeshServerName
}

func (*MeshServer) Group() string {
	return defaults.MeshServerName
}

func (ms *MeshServer) Enable() bool {
	return ms.Config.Enable
}

func (ms *MeshServer) Start() {
	go ms.startUnixsocketServer(ms.Config.Server)
}

func (ms *MeshServer) Shutdown() {
}

// ExtractMessage extracts message from clients
func ExtractMessage(context string) (*model.Message, error) {
	var msg model.Message
	if context == "" {
		return &msg, errors.New("failed with error: context is empty")
	}
	err := json.Unmarshal([]byte(context), &msg)
	if err != nil {
		return &msg, err
	}
	return &msg, nil
}

// feedbackError sends back error message
func feedbackError(err error, request *model.Message) string {
	// Build message
	errResponse := model.NewErrorMessage(request, err.Error()).SetRoute(defaults.MeshServerName, request.GetGroup())
	// Marshal message
	data, err := json.Marshal(errResponse)
	if err != nil {
		return fmt.Sprintf("feedbackError marshal failed with error: %v", err)
	}
	return string(data)
}
