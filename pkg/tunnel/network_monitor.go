package tunnel

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"

	"github.com/xtaci/kcp-go"
	"k8s.io/klog/v2"
)

const (
	broadcastAddr = "192.168.15.255:12345" // 广播地址和端口
)

var kcpConn *kcp.UDPSession

func createKCPConnection() error {
	var err error
	kcpConn, err = kcp.DialWithOptions(broadcastAddr, nil, 0, 0)
	if err != nil {
		return fmt.Errorf("failed to create KCP connection: %v", err)
	}
	return nil
}

func broadcastMessage(b []byte) error {
	if kcpConn == nil {
		return fmt.Errorf("KCP connection not established")
	}
	_, err := kcpConn.Write(b)
	return err
}

func currentTimestamp() string {
	return time.Now().Format("2006-01-02 15:04:05.000") // 毫秒级时间戳
}

func checkConnection() (bool, error) {
	out, err := exec.Command("cat", "/sys/class/net/end0/carrier").Output()
	if err != nil {
		return false, err
	}
	return string(bytes.TrimSpace(out)) == "1", nil
}

func (t *EdgeTunnel) InterfaceMonitor() {
	var lastStatus bool = true
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			isConnected, err := checkConnection()
			if err != nil {
				klog.Errorf("Error checking connection: %v", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if isConnected && !lastStatus {
				lastStatus = true
				klog.Infof("网线接入...")
				time.Sleep(100 * time.Millisecond)

				// 创建KCP连接
				err = createKCPConnection()
				if err != nil {
					klog.Errorf("Failed to create KCP connection: %v", err)
					continue
				}

				url := "http://localhost:10550/api/v1/endpoints"
				jsonData, err := FetchFormattedJSONFromURL(url)
				if err != nil {
					klog.Errorf("Error fetching JSON data: %v", err)
					continue
				}
				filteredEndpoints, err := filterEndpointsByNodeName(jsonData, t.Config.NodeName)
				if err != nil {
					klog.Errorf("Error filtering endpoints: %v", err)
					continue
				}

				klog.Infof("[%s] %s\n", currentTimestamp(), filteredEndpoints)
				if err != nil {
					klog.Errorf("Error marshalling UDPmsg: %v", err)
					continue
				}

				err = broadcastMessage(filteredEndpoints)
				if err != nil {
					klog.Errorf("Error broadcasting message: %v", err)
				}

			} else if !isConnected && lastStatus {
				lastStatus = false
				klog.Infof("[%s] Cable unplugged, closing KCP connection.\n", currentTimestamp())
				if kcpConn != nil {
					kcpConn.Close()
					kcpConn = nil
				}
			}
		}
	}
}
