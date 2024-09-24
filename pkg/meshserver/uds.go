package meshserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"

	"github.com/kubeedge/edgemesh/pkg/apis/config/defaults"
	"github.com/kubeedge/edgemesh/pkg/tunnel"
	str "github.com/kubeedge/edgemesh/pkg/util/strings"
	"k8s.io/klog/v2"
)

const (
	// DefaultBufferSize represents default buffer size
	DefaultBufferSize = 10480
)

// UnixDomainSocket struct
type UnixDomainSocket struct {
	filename   string
	buffersize int
	handler    func(string) string
}

// NewUnixDomainSocket create new socket
func NewUnixDomainSocket(filename string, buffersize ...int) *UnixDomainSocket {
	size := DefaultBufferSize
	if len(buffersize) != 0 {
		size = buffersize[0]
	}
	return &UnixDomainSocket{filename: filename, buffersize: size}
}

// parseEndpoint parses endpoint
func parseEndpoint(ep string) (string, string, error) {
	if strings.HasPrefix(strings.ToLower(ep), "unix://") || strings.HasPrefix(strings.ToLower(ep), "tcp://") {
		s := strings.SplitN(ep, "://", 2)
		if s[1] != "" {
			return s[0], s[1], nil
		}
	}
	return "", "", fmt.Errorf("invalid endpoint: %v", ep)
}

// SetContextHandler set handler for server
func (us *UnixDomainSocket) SetContextHandler(f func(string) string) {
	us.handler = f
}

func (us *UnixDomainSocket) StartServer() error {
	proto, addr, err := parseEndpoint(us.filename)
	if err != nil {
		return fmt.Errorf("failed to parseEndpoint: %w", err)
	}

	if proto == "unix" {
		addr = "/" + addr
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove addr: %w", err)
		}
	}

	listener, err := net.Listen(proto, addr)
	if err != nil {
		return fmt.Errorf("failed to listen addr: %w", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(beehiveContext.GetContext())
	defer cancel()

	messageChan := make(chan model.Message, 100)
	go us.watchMeshServer(ctx, messageChan)

	for {
		conn, err := listener.Accept()
		if err != nil {
			klog.Errorf("accept error: %v", err)
			continue
		}

		go us.handleServerConn(ctx, conn, messageChan)
	}
}

func (us *UnixDomainSocket) watchMeshServer(ctx context.Context, messageChan chan<- model.Message) {
	for {
		select {
		case <-ctx.Done():
			klog.Warning("Stop SocketServer Receive loop")
			return
		default:
			if msg, ok := beehiveContext.Receive(defaults.MeshServerName); ok == nil {
				select {
				case messageChan <- msg:
					klog.Infoln("MeshServer received and sent msg")
				default:
					klog.Warning("messageChan is full, dropping message")
				}
			}
		}
	}
}

func (us *UnixDomainSocket) handleServerConn(ctx context.Context, c net.Conn, messageChan <-chan model.Message) {
	defer c.Close()
	klog.Infoln("start handle...")

	readChan := make(chan string)
	go us.keepAlive(ctx, c)
	go us.readFromConn(ctx, c, readChan)

	for {
		select {
		case <-ctx.Done():
			return
		case result := <-messageChan:
			klog.Infoln("ready to send message to EdgeCore")
			bytes := str.Marshal(result)
			if err := us.writeToConn(c, bytes); err != nil {
				klog.Errorf("failed to write message: %v", err)
				return
			}
		case msg := <-readChan:
			result := us.handleServerContext(msg)
			if err := us.writeToConn(c, []byte(result)); err != nil {
				klog.Errorf("failed to write result: %v", err)
				return
			}
		}
	}
}

func (us *UnixDomainSocket) readFromConn(ctx context.Context, c net.Conn, readChan chan<- string) {
	buf := make([]byte, us.buffersize)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			nr, err := c.Read(buf)
			if err != nil {
				if err != io.EOF {
					klog.Errorf("failed to read buffer: %v", err)
				}
				return
			}
			if nr > 0 {
				readChan <- string(buf[:nr])
			}
		}
	}
}

func (us *UnixDomainSocket) writeToConn(c net.Conn, bytes []byte) error {
	_, err := c.Write(bytes)
	return err
}

// HandleServerContext handler for server
func (us *UnixDomainSocket) handleServerContext(context string) string {

	if us.handler != nil {
		return us.handler(context)
	}
	return ""
}

func (us *UnixDomainSocket) keepAlive(ctx context.Context, conn net.Conn) {
	ticker := time.NewTicker(30 * time.Second) // Adjust the interval as needed
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msg := model.NewMessage("").FillBody(tunnel.NodeEvent{
				EventName:     tunnel.EventPing,
				NodeName:      "",
				NodeEventInfo: tunnel.NodeInfo{},
			})
			bytes := str.Marshal(msg)
			err := us.writeToConn(conn, bytes)
			if err != nil {
				klog.Errorf("keepAlive ping failed: %v", err)
				conn.Close()
				return
			}
		}
	}
}
