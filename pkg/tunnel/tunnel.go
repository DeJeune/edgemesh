package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	ocprom "contrib.go.opencensus.io/exporter/prometheus"
	"github.com/fsnotify/fsnotify"
	ds "github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/edgemesh/pkg/apis/config/defaults"
	"github.com/kubeedge/edgemesh/pkg/apis/config/v1alpha1"
	"github.com/kubeedge/edgemesh/pkg/messagepkg"
	discoverypb "github.com/kubeedge/edgemesh/pkg/tunnel/pb/discovery"
	proxypb "github.com/kubeedge/edgemesh/pkg/tunnel/pb/proxy"
	netutil "github.com/kubeedge/edgemesh/pkg/util/net"
	cni "github.com/kubeedge/edgemesh/pkg/util/tunutils"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	p2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/host/resource-manager/obs"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-msgio/protoio"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/xtaci/kcp-go"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
)

const (
	MaxReadSize = 4096

	DailRetryTime = 3
	DailSleepTime = 500 * time.Microsecond

	RetryTime     = 3
	RetryInterval = 2 * time.Second
)

type RelayMap map[string]*peer.AddrInfo

func (r RelayMap) ContainsPublicIP() bool {
	for _, p := range r {
		for _, addr := range p.Addrs {
			if manet.IsPublicAddr(addr) {
				return true
			}
		}
	}
	return false
}

// discoveryNotifee implement mdns interface
type discoveryNotifee struct {
	PeerChan chan peer.AddrInfo
}

// HandlePeerFound interface to be called when new peer is found
func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	n.PeerChan <- pi
}

// initMDNS initialize the MDNS service
func initMDNS(host p2phost.Host, rendezvous string) (chan peer.AddrInfo, error) {
	n := &discoveryNotifee{}
	n.PeerChan = make(chan peer.AddrInfo)

	ser := mdns.NewMdnsService(host, rendezvous, n)
	if err := ser.Start(); err != nil {
		return nil, err
	}
	klog.Infof("Starting MDNS discovery service")
	return n.PeerChan, nil
}

func (t *EdgeTunnel) runMdnsDiscovery() {
	for pi := range t.mdnsPeerChan {
		t.discovery(defaults.MdnsDiscovery, pi)
	}
}

func initDHT(ctx context.Context, ddht *dual.DHT, rendezvous string) (<-chan peer.AddrInfo, error) {
	routingDiscovery := drouting.NewRoutingDiscovery(ddht)
	dutil.Advertise(ctx, routingDiscovery, rendezvous)
	klog.Infof("Starting DHT discovery service")

	peerChan, err := routingDiscovery.FindPeers(ctx, rendezvous)
	if err != nil {
		return nil, err
	}

	return peerChan, nil
}

func (t *EdgeTunnel) runDhtDiscovery() {
	for pi := range t.dhtPeerChan {
		t.discovery(defaults.DhtDiscovery, pi)
	}
}

func (t *EdgeTunnel) isRelayPeer(id peer.ID) bool {
	for _, relay := range t.relayMap {
		if relay.ID == id {
			return true
		}
	}
	return false
}

// discovery function is used in the EdgeTunnel to establish connections with other nodes.
// It creates a new stream with the given address information (pi) and discovery type (MDNS or DHT) and performs a handshake.
// If a non-relay node is discovered in DHT discovery, it adds its address to the peerstore to avoid RESERVATION delays.
// Once the connection is established, the function adds the address information of the connection to the node-peer mapping table (t.nodePeerMap) for future communication.
func (t *EdgeTunnel) discovery(discoverType defaults.DiscoveryType, pi peer.AddrInfo) {
	if pi.ID == t.p2pHost.ID() {
		return
	}
	klog.Infof("[%s] Discovery found peer: %s", discoverType, pi)

	// If dht discovery finds a non-relay peer, add the circuit address to this peer.
	// This is done to avoid delays in RESERVATION https://github.com/libp2p/specs/blob/master/relay/circuit-v2.md.
	if discoverType == defaults.DhtDiscovery && !t.isRelayPeer(pi.ID) {
		addrInfo := peer.AddrInfo{ID: pi.ID, Addrs: []ma.Multiaddr{}}
		err := AddCircuitAddrsToPeer(&addrInfo, t.relayMap)
		if err != nil {
			klog.Errorf("Failed to add circuit addrs to peer %s", addrInfo)
			return
		}
		t.p2pHost.Peerstore().AddAddrs(pi.ID, addrInfo.Addrs, peerstore.PermanentAddrTTL)
	}

	if err := t.p2pHost.Connect(t.hostCtx, pi); err != nil {
		klog.Errorf("[%s] Failed to connect to %s, err: %v", discoverType, pi, err)
		return
	}

	stream, err := t.p2pHost.NewStream(network.WithUseTransient(t.hostCtx, "relay"), pi.ID, defaults.DiscoveryProtocol)
	if err != nil {
		klog.Errorf("[%s] New stream between peer %s err: %v", discoverType, pi, err)
		return
	}
	defer func() {
		err = stream.Reset()
		if err != nil {
			klog.Errorf("[%s] Stream between %s reset err: %v", discoverType, pi, err)
		}
	}()
	klog.Infof("[%s] New stream between peer %s success", discoverType, pi)

	streamWriter := protoio.NewDelimitedWriter(stream)
	streamReader := protoio.NewDelimitedReader(stream, MaxReadSize) // TODO get maxSize from default

	// handshake with dest peer
	protocol := string(defaults.MdnsDiscovery)
	if discoverType == defaults.DhtDiscovery {
		protocol = string(defaults.DhtDiscovery)
	}
	msg := &discoverypb.Discovery{
		Type:     discoverypb.Discovery_CONNECT.Enum(),
		Protocol: &protocol,
		NodeName: &t.Config.NodeName,
	}
	err = streamWriter.WriteMsg(msg)
	if err != nil {
		klog.Errorf("[%s] Write msg to %s err: %v", discoverType, pi, err)
		return
	}

	// read response
	msg.Reset()
	err = streamReader.ReadMsg(msg)
	if err != nil {
		klog.Errorf("[%s] Read response msg from %s err: %v", discoverType, pi, err)
		return
	}
	currentTime := time.Now().Format("2006-01-02 15:04:05.000")
	klog.Infof("receive msg in %s", currentTime)
	msgType := msg.GetType()
	if msgType != discoverypb.Discovery_SUCCESS {
		klog.Errorf("[%s] Failed to build stream between %s, Type is %s, err: %v", discoverType, pi, msg.GetType(), err)
		return
	}

	// (re)mapping nodeName and peerID
	nodeName := msg.GetNodeName()
	t.mu.Lock()
	defer t.mu.Unlock()

	nodeInfo, exists := t.nodePeerMap[nodeName]
	if !exists {
		// New node joining
		nodeInfo = &NodeInfo{
			PeerID:   pi.ID,
			Status:   NodeStatusActive,
			LastSeen: time.Now(),
		}
		t.nodePeerMap[nodeName] = nodeInfo
		t.peerIDtoNodeName[pi.ID] = nodeName
		klog.Infof("通过discovery:%s方法检测到节点%s已加入", protocol, nodeName)
		t.nodeEventChan <- NodeEvent{EventName: EventNodeJoined, NodeName: nodeName, NodeEventInfo: *nodeInfo}
	} else {
		// Existing node
		nodeInfo.LastSeen = time.Now()
		if nodeInfo.Status == NodeStatusInactive {
			// Node rejoining
			nodeInfo.Status = NodeStatusActive
			nodeInfo.LastSeen = time.Now()
			klog.Infof("通过discovery:%s方法检测到节点%s已重新连接", protocol, nodeName)
			t.nodeEventChan <- NodeEvent{EventName: EventNodeJoined, NodeName: nodeName, NodeEventInfo: *nodeInfo}
		} else {
			// Node already active, just update LastSeen
			nodeInfo.LastSeen = time.Now()
		}
		t.nodePeerMap[nodeName] = nodeInfo
	}
	klog.Infof("[%s] Discovery to %s : %s", protocol, nodeName, pi)
}

func (t *EdgeTunnel) checkNodeStatus() {
	now := time.Now()
	t.mu.Lock() // 使用互斥锁保护共享数据

	var events []NodeEvent
	klog.V(4).Infof("在时刻%s开始检查节点状态", now)
	for nodeName, nodeInfo := range t.nodePeerMap {
		if nodeInfo.Status == NodeStatusActive {
			if false { // 测试用
				nodeInfo.Status = NodeStatusInactive
				nodeInfo.LastSeen = now
				t.nodePeerMap[nodeName] = nodeInfo
				events = append(events, NodeEvent{EventName: EventNodeLeft, NodeName: nodeName, NodeEventInfo: *nodeInfo})
				klog.V(4).Infof("在时刻%s通过checkNodeStatus检测到节点%s已失联", time.Now().Format("2006-01-02 15:04:05.000"), nodeName)
			}

		} else if nodeInfo.Status == NodeStatusInactive {
			if t.isNodeReachable(nodeInfo.PeerID) {
				nodeInfo.Status = NodeStatusActive
				nodeInfo.LastSeen = now
				t.nodePeerMap[nodeName] = nodeInfo
				klog.V(4).Infof("在时刻%s通过checkNodeStatus检测到节点%s已重新连接", time.Now().Format("2006-01-02 15:04:05.000"), nodeName)
				events = append(events, NodeEvent{EventName: EventNodeJoined, NodeName: nodeName, NodeEventInfo: *nodeInfo})

			}
		}
	}

	t.mu.Unlock()

	// 异步发送事件
	go func() {
		for _, e := range events {
			klog.Infof("在时刻%s，%s传入通道", time.Now().Format("2006-01-02 15:04:05.000"), e.EventName)
			t.nodeEventChan <- e
		}
	}()
}

func (t *EdgeTunnel) isNodeReachable(peerID peer.ID) bool {
	klog.V(6).Infof("在时刻%s开始检查节点%s是否可达", time.Now().Format("2006-01-02 15:04:05.000"), peerID)
	ctx, cancel := context.WithTimeout(t.hostCtx, 3*time.Second)
	defer func() {
		klog.V(6).Infof("在时刻%s检查节点%s是否可达结束", time.Now().Format("2006-01-02 15:04:05.000"), peerID)
		cancel()
	}()

	// 使用 libp2p 的 ping 协议
	p := ping.NewPingService(t.p2pHost)
	select {
	case res := <-p.Ping(ctx, peerID):
		return res.Error == nil
	case <-ctx.Done():
		// 如果 ping 超时，尝试建立新的 libp2p 流
		streamCtx, streamCancel := context.WithTimeout(t.hostCtx, 2*time.Second)
		defer streamCancel()
		_, err := t.p2pHost.NewStream(streamCtx, peerID, "/ping/1.0.0")
		return err == nil
	}
}

// func (t *EdgeTunnel) checkTCPConnection(peerID peer.ID) bool {
// 	peerInfo := t.p2pHost.Peerstore().PeerInfo(peerID)
// 	if len(peerInfo.Addrs) == 0 {
// 		return false
// 	}

// 	for _, addr := range peerInfo.Addrs {
// 		if ipAddr, err := addr.ValueForProtocol(ma.P_IP4); err == nil {
// 			conn, err := net.DialTimeout("tcp", ipAddr+":"+strconv.Itoa(t.Config.ListenPort), 5*time.Second)
// 			if err == nil {
// 				conn.Close()
// 				return true
// 			}
// 		}
// 	}

// 	return false
// }

// discoveryStreamHandler handles incoming streams for discovery service.
// It reads the handshake message from the incoming stream and writes a response message,
// then maps the nodeName and peerID of the remote peer to the nodePeerMap of EdgeTunnel.
// This function is called when a new stream is received by the discovery service of EdgeTunnel.
func (t *EdgeTunnel) discoveryStreamHandler(stream network.Stream) {
	remotePeer := peer.AddrInfo{
		ID:    stream.Conn().RemotePeer(),
		Addrs: []ma.Multiaddr{stream.Conn().RemoteMultiaddr()},
	}
	klog.Infof("Discovery service got a new stream from %s", remotePeer)

	streamWriter := protoio.NewDelimitedWriter(stream)
	streamReader := protoio.NewDelimitedReader(stream, MaxReadSize) // TODO get maxSize from default

	// read handshake
	msg := new(discoverypb.Discovery)
	err := streamReader.ReadMsg(msg)
	if err != nil {
		klog.Errorf("Read msg from %s err: %v", remotePeer, err)
		return
	}
	if msg.GetType() != discoverypb.Discovery_CONNECT {
		klog.Errorf("Stream between %s, Type should be CONNECT", remotePeer)
		return
	}

	// write response
	protocol := msg.GetProtocol()
	nodeName := msg.GetNodeName()
	msg.Type = discoverypb.Discovery_SUCCESS.Enum()
	msg.NodeName = &t.Config.NodeName
	err = streamWriter.WriteMsg(msg)
	if err != nil {
		klog.Errorf("[%s] Write msg to %s err: %v", protocol, remotePeer, err)
		return
	}

	// (re)mapping nodeName and peerID
	klog.Infof("[%s] Discovery from %s : %s", protocol, nodeName, remotePeer)
	nodeInfo, exists := t.nodePeerMap[nodeName]
	if !exists {
		nodeInfo = &NodeInfo{
			PeerID:   remotePeer.ID,
			Status:   NodeStatusActive,
			LastSeen: time.Now(),
		}
		t.nodePeerMap[nodeName] = nodeInfo
		t.peerIDtoNodeName[remotePeer.ID] = nodeName
		klog.V(2).Infof("通过discoveryStreamHandler检测到节点%s已加入", nodeName)
		t.nodeEventChan <- NodeEvent{EventName: EventNodeJoined, NodeName: nodeName, NodeEventInfo: *nodeInfo}
	} else if nodeInfo.Status == NodeStatusInactive {
		nodeInfo.Status = NodeStatusActive
		nodeInfo.LastSeen = time.Now()
		t.nodePeerMap[nodeName] = nodeInfo
		klog.V(2).Infof("通过discoveryStreamHandler检测到节点%s已重新连接", nodeName)
		t.nodeEventChan <- NodeEvent{EventName: EventNodeJoined, NodeName: nodeName, NodeEventInfo: *nodeInfo}
	} else {
		nodeInfo.LastSeen = time.Now()
		t.nodePeerMap[nodeName] = nodeInfo
	}
}

type ProxyOptions struct {
	Protocol string
	NodeName string
	IP       string
	Port     int32
}

// GetProxyStream establishes a new stream with a destination peer, either directly or through a relay node,
// by performing a handshake with the destination peer over the stream to confirm the connection.
// It first looks up the destination peer's ID in a cache, and if not found, generates the peer ID and adds circuit addresses to it.
// It then opens a new stream using the libp2p host, and performs a handshake with the destination peer over the stream.
// If the handshake is successful, it returns a new StreamConn object representing the stream.
// If any errors occur during the process, it returns an error.
func (t *EdgeTunnel) GetProxyStream(opts ProxyOptions) (*StreamConn, error) {
	var destInfo peer.AddrInfo
	var err error

	destName := opts.NodeName
	nodeInfo, exists := t.nodePeerMap[destName]
	if !exists {
		klog.Warningf("Node %s not found in nodePeerMap, attempting to generate peer ID", destName)
		destID, err := PeerIDFromString(destName)
		if err != nil {
			return nil, fmt.Errorf("failed to generate peer id for %s: %w", destName, err)
		}
		destInfo = peer.AddrInfo{ID: destID, Addrs: []ma.Multiaddr{}}
		klog.Infof("Generated peer info for %s: %s", destName, destInfo)
		nodeInfo = &NodeInfo{
			PeerID:   destID,
			Status:   NodeStatusActive,
			LastSeen: time.Now(),
		}
		t.nodePeerMap[destName] = nodeInfo
		t.nodeEventChan <- NodeEvent{
			EventName:     EventNodeJoined,
			NodeName:      destName,
			NodeEventInfo: *nodeInfo,
		}
	} else {
		nodeInfo.LastSeen = time.Now()
		destInfo = t.p2pHost.Peerstore().PeerInfo(nodeInfo.PeerID)
	}

	if err = AddCircuitAddrsToPeer(&destInfo, t.relayMap); err != nil {
		return nil, fmt.Errorf("failed to add circuit addrs to peer %s: %w", destInfo, err)
	}
	t.p2pHost.Peerstore().AddAddrs(destInfo.ID, destInfo.Addrs, peerstore.PermanentAddrTTL)

	klog.Infof("Attempting to open stream to %s (%s)", destName, destInfo.ID)
	stream, err := t.p2pHost.NewStream(network.WithUseTransient(t.hostCtx, "relay"), nodeInfo.PeerID, defaults.ProxyProtocol)
	if err != nil {
		return nil, fmt.Errorf("failed to open new stream to %s (%s): %w", destName, destInfo.ID, err)
	}
	klog.Infof("Successfully opened stream to %s (%s)", destName, destInfo.ID)

	streamWriter := protoio.NewDelimitedWriter(stream)
	streamReader := protoio.NewDelimitedReader(stream, MaxReadSize)

	// Handshake with dest peer
	msg := &proxypb.Proxy{
		Type:     proxypb.Proxy_CONNECT.Enum(),
		Protocol: &opts.Protocol,
		NodeName: &opts.NodeName,
		Ip:       &opts.IP,
		Port:     &opts.Port,
	}
	if err = streamWriter.WriteMsg(msg); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("failed to write handshake message to %s: %w", opts.NodeName, err)
	}

	// Read response
	msg.Reset()
	if err = streamReader.ReadMsg(msg); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("failed to read handshake response from %s: %w", opts.NodeName, err)
	}
	if msg.GetType() == proxypb.Proxy_FAILED {
		stream.Reset()
		return nil, fmt.Errorf("proxy connection to %s failed: remote node returned FAILED status", opts.NodeName)
	}

	klog.V(4).Infof("Successfully established proxy connection to %s (%s:%d)", opts.NodeName, opts.IP, opts.Port)
	return NewStreamConn(stream), nil
}

func (t *EdgeTunnel) proxyStreamHandler(stream network.Stream) {
	remotePeer := peer.AddrInfo{
		ID:    stream.Conn().RemotePeer(),
		Addrs: []ma.Multiaddr{stream.Conn().RemoteMultiaddr()},
	}
	klog.Infof("Proxy service got a new stream from %s", remotePeer)

	streamWriter := protoio.NewDelimitedWriter(stream)
	streamReader := protoio.NewDelimitedReader(stream, MaxReadSize) // TODO get maxSize from default

	// read handshake
	msg := new(proxypb.Proxy)
	err := streamReader.ReadMsg(msg)
	if err != nil {
		klog.Errorf("Failed to read handshake message from %s: %v", remotePeer, err)
		return
	}
	if msg.GetType() != proxypb.Proxy_CONNECT {
		klog.Errorf("Unexpected message type from %s: expected CONNECT, got %s", remotePeer, msg.GetType())
		return
	}
	targetProto := msg.GetProtocol()
	targetNode := msg.GetNodeName()
	targetIP := msg.GetIp()
	targetPort := msg.GetPort()
	targetAddr := fmt.Sprintf("%s:%d", targetIP, targetPort)

	klog.Infof("Attempting to dial endpoint: %s %s:%d", targetProto, targetIP, targetPort)
	proxyConn, err := tryDialEndpoint(targetProto, targetIP, int(targetPort))
	if err != nil {
		klog.Errorf("Failed to connect to endpoint %s %s: %v", targetProto, targetAddr, err)
		msg.Reset()
		msg.Type = proxypb.Proxy_FAILED.Enum()
		if err = streamWriter.WriteMsg(msg); err != nil {
			klog.Errorf("Write msg to %s err: %v", remotePeer, err)
			return
		}
		return
	}

	// write response
	msg.Type = proxypb.Proxy_SUCCESS.Enum()
	err = streamWriter.WriteMsg(msg)
	if err != nil {
		klog.Errorf("Write msg to %s err: %v", remotePeer, err)
		return
	}
	msg.Reset()

	streamConn := NewStreamConn(stream)
	switch targetProto {
	case TCP:
		go netutil.ProxyConn(streamConn, proxyConn)
	case UDP:
		go netutil.ProxyConnUDP(streamConn, proxyConn.(*net.UDPConn))
	}
	klog.Infof("Successfully established proxy for {%s %s %s}", targetProto, targetNode, targetAddr)
}

// tryDialEndpoint tries to dial to an endpoint with given protocol, ip and port.
// If TCP or UDP protocol is used, it retries several times and waits for DailSleepTime between each try.
// If neither TCP nor UDP is used, it returns an error with an unsupported protocol message.
// when maximum retries are reached for the given protocol, it logs the error and returns it.
func tryDialEndpoint(protocol, ip string, port int) (conn net.Conn, err error) {
	addr := fmt.Sprintf("%s:%d", ip, port)
	klog.Infof("Attempting to dial %s %s", protocol, addr)

	var dialErr error
	for i := 0; i < DailRetryTime; i++ {
		conn, dialErr = net.DialTimeout(protocol, addr, 5*time.Second)
		if dialErr == nil {
			klog.Infof("Successfully connected to %s %s", protocol, addr)
			return conn, nil
		}

		// Check for specific error types
		if opErr, ok := dialErr.(*net.OpError); ok {
			if syscallErr, ok := opErr.Err.(*os.SyscallError); ok {
				if syscallErr.Err == syscall.ECONNREFUSED {
					klog.Errorf("Connection refused to %s %s (attempt %d/%d): service may not be running or port may be blocked",
						protocol, addr, i+1, DailRetryTime)
				} else {
					klog.Warningf("Failed to connect to %s %s (attempt %d/%d): %v",
						protocol, addr, i+1, DailRetryTime, dialErr)
				}
			}
		} else {
			klog.Warningf("Failed to connect to %s %s (attempt %d/%d): %v",
				protocol, addr, i+1, DailRetryTime, dialErr)
		}

		time.Sleep(DailSleepTime)
	}

	klog.Errorf("Max retries reached for dialing %s %s", protocol, addr)
	return nil, fmt.Errorf("failed to connect to %s %s after %d attempts: %v", protocol, addr, DailRetryTime, dialErr)
}

// BootstrapConnect tries to connect to a list of bootstrap peers in a relay map.
// The function runs a loop to attempt connecting to each peer, and will retry if some peers fail to connect.
// The function returns an error if it fails to connect to all bootstrap peers after a certain period of time.
func BootstrapConnect(ctx context.Context, ph p2phost.Host, bootstrapPeers RelayMap) error {
	var lock sync.Mutex
	var badRelays []string
	err := wait.PollImmediate(10*time.Second, time.Minute, func() (bool, error) { // TODO get timeout from config
		badRelays = make([]string, 0)
		var wg sync.WaitGroup
		for n, p := range bootstrapPeers {
			if p.ID == ph.ID() {
				continue
			}

			wg.Add(1)
			go func(n string, p *peer.AddrInfo) {
				defer wg.Done()
				klog.Infof("[Bootstrap] bootstrapping to %s", p.ID)

				ph.Peerstore().AddAddrs(p.ID, p.Addrs, peerstore.PermanentAddrTTL)
				if err := ph.Connect(ctx, *p); err != nil {
					klog.Errorf("[Bootstrap] failed to bootstrap with %s: %v", p, err)
					lock.Lock()
					badRelays = append(badRelays, n)
					lock.Unlock()
					return
				}
				klog.Infof("[Bootstrap] success bootstrapped with %s", p)
			}(n, p)
		}
		wg.Wait()
		if len(badRelays) > 0 {
			klog.Errorf("[Bootstrap] Not all bootstrapDail connected, continue bootstrapDail...")
			return false, nil
		}
		return true, nil
	})

	for _, bad := range badRelays {
		klog.Warningf("[Bootstrap] bootstrapping to %s : %s timeout", bad, bootstrapPeers[bad])
	}
	return err
}

func newDHT(ctx context.Context, host p2phost.Host, relayPeers RelayMap) (*dual.DHT, error) {
	relays := make([]peer.AddrInfo, 0, len(relayPeers))
	for _, relay := range relayPeers {
		relays = append(relays, *relay)
	}
	dstore := dsync.MutexWrap(ds.NewMapDatastore())
	ddht, err := dual.New(
		ctx,
		host,
		dual.DHTOption(
			dht.Concurrency(10),
			dht.Mode(dht.ModeServer),
			dht.Datastore(dstore)),
		dual.WanDHTOption(dht.BootstrapPeers(relays...)),
	)
	if err != nil {
		return nil, err
	}
	return ddht, nil
}

func (t *EdgeTunnel) nodeNameFromPeerID(id peer.ID) (string, bool) {
	for nodeName, nodeInfo := range t.nodePeerMap {
		if nodeInfo.PeerID == id {
			return nodeName, true
		}
	}
	return "", false
}

func (t *EdgeTunnel) runRelayFinder(ddht *dual.DHT, peerSource chan peer.AddrInfo, period time.Duration) {
	klog.Infof("Starting relay finder")
	err := wait.PollUntil(period, func() (done bool, err error) {
		// ensure peers in same LAN can send [hop]RESERVE to the relay
		for _, relay := range t.relayMap {
			if relay.ID == t.p2pHost.ID() {
				continue
			}
			select {
			case peerSource <- *relay:
				klog.Infoln("[Finder] send relayMap peer:", relay)
			case <-t.hostCtx.Done():
				return
			}
		}
		closestPeers, err := ddht.WAN.GetClosestPeers(t.hostCtx, t.p2pHost.ID().String())
		if err != nil {
			if !IsNoFindPeerError(err) {
				klog.Errorf("[Finder] Failed to get closest peers: %v", err)
			}
			return false, nil
		}
		for _, p := range closestPeers {
			addrs := t.p2pHost.Peerstore().Addrs(p)
			if len(addrs) == 0 {
				continue
			}
			dhtPeer := peer.AddrInfo{ID: p, Addrs: addrs}
			klog.Infoln("[Finder] find a relay:", dhtPeer)
			select {
			case peerSource <- dhtPeer:
			case <-t.hostCtx.Done():
				return
			}
			nodeName, exists := t.nodeNameFromPeerID(dhtPeer.ID)
			if exists {
				t.refreshRelayMap(nodeName, &dhtPeer)
			}
		}
		return false, nil
	}, t.stopCh)
	if err != nil {
		klog.Errorf("[Finder] causes an error %v", err)
	}
}

func (t *EdgeTunnel) refreshRelayMap(nodeName string, dhtPeer *peer.AddrInfo) {
	// Will there be a problem when running on a private network?
	// Still need to observe for a while
	dhtPeer.Addrs = FilterPrivateMaddr(dhtPeer.Addrs)
	dhtPeer.Addrs = FilterCircuitMaddr(dhtPeer.Addrs)

	relayInfo, exists := t.relayMap[nodeName]
	if !exists {
		t.relayMap[nodeName] = dhtPeer
		return
	}

	for _, maddr := range dhtPeer.Addrs {
		relayInfo.Addrs = AppendMultiaddrs(relayInfo.Addrs, maddr)
	}
}

func (t *EdgeTunnel) runHeartbeat() {
	err := wait.PollUntil(time.Duration(t.Config.HeartbeatPeriod)*time.Second, func() (done bool, err error) {
		t.connectToRelays("Heartbeat")
		// We make the return value of ConditionFunc, such as bool to return false,
		// and err to return to nil, to ensure that we can continuously execute
		// the ConditionFunc.
		t.checkNodeStatus()
		return false, nil
	}, t.stopCh)
	if err != nil {
		klog.Errorf("[Heartbeat] causes an error %v", err)
	}
}

func (t *EdgeTunnel) connectToRelays(connectType string) {
	wg := sync.WaitGroup{}
	for _, relay := range t.relayMap {
		wg.Add(1)
		go func(relay *peer.AddrInfo) {
			defer wg.Done()
			t.connectToRelay(connectType, relay)
		}(relay)
	}
	wg.Wait()
}

func (t *EdgeTunnel) connectToRelay(connectType string, relay *peer.AddrInfo) {
	if t.p2pHost.ID() == relay.ID {
		return
	}
	if len(t.p2pHost.Network().ConnsToPeer(relay.ID)) != 0 {
		klog.Infof("[%s] Already has connection between %s and me", connectType, relay)
		return
	}

	klog.V(0).Infof("[%s] Connection between relay %s is not established, try connect", connectType, relay)
	retryTime := 0
	for retryTime < RetryTime {
		err := t.p2pHost.Connect(t.hostCtx, *relay)
		if err != nil {
			klog.Errorf("[%s] Failed to connect relay %s err: %v", connectType, relay, err)
			time.Sleep(RetryInterval)
			retryTime++
			continue
		}

		klog.Infof("[%s] Success connected to relay %s", connectType, relay)
		break
	}
}

func (t *EdgeTunnel) runConfigWatcher() {
	defer func() {
		if err := t.cfgWatcher.Close(); err != nil {
			klog.Errorf("[Watcher] Failed to close config watcher")
		}
	}()

	for {
		select {
		case event, ok := <-t.cfgWatcher.Events:
			if !ok {
				klog.Errorf("[Watcher] Failed to get events chan")
				continue
			}
			// k8s configmaps uses symlinks, we need this workaround.
			// updating k8s configmaps will delete the file inotify
			if event.Op == fsnotify.Remove {
				// re-add a new watcher pointing to the new symlink/file
				if err := t.cfgWatcher.Add(t.Config.ConfigPath); err != nil {
					klog.Errorf("[Watcher] Failed to re-add watcher in %s, err: %v", t.Config.ConfigPath, err)
					return
				}
				t.doReload(t.Config.ConfigPath)
			}
			// also allow normal files to be modified and reloaded.
			if event.Op&fsnotify.Write == fsnotify.Write {
				t.doReload(t.Config.ConfigPath)
			}
		case err, ok := <-t.cfgWatcher.Errors:
			if !ok {
				klog.Errorf("[Watcher] Failed to get errors chan")
				continue
			}
			klog.Errorf("[Watcher] Config watcher got an error:", err)
		}
	}
}

type reloadConfig struct {
	Modules *struct {
		EdgeTunnelConfig *v1alpha1.EdgeTunnelConfig `json:"edgeTunnel,omitempty"`
	} `json:"modules,omitempty"`
}

func (t *EdgeTunnel) doReload(configPath string) {
	klog.Infof("[Watcher] Reload config from %s", configPath)

	var cfg reloadConfig
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		klog.Errorf("[Watcher] Failed to read config file %s: %v", configPath, err)
		return
	}
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		klog.Errorf("[Watcher] Failed to unmarshal config file %s: %v", configPath, err)
		return
	}

	klog.Infof("[Watcher] Generate new relay map:")
	relayMap := GenerateRelayMap(cfg.Modules.EdgeTunnelConfig.RelayNodes, t.Config.Transport, t.Config.ListenPort)
	for nodeName, pi := range relayMap {
		klog.Infof("%s => %s", nodeName, pi)
	}
	t.relayMap = relayMap

	// enable or disable relayv2 service
	_, exists := t.relayMap[t.Config.NodeName]
	if exists {
		if t.relayService == nil && t.Config.Mode == defaults.ServerClientMode {
			t.relayService, err = relayv2.New(t.p2pHost, relayv2.WithLimit(nil))
			if err != nil {
				klog.Errorf("[Watcher] Failed to enable relayv2 service, err: %v", err)
			} else {
				t.isRelay = true
				klog.Infof("[Watcher] Enable relayv2 service success")
			}
		}
	} else {
		if t.relayService != nil && t.Config.Mode == defaults.ServerClientMode {
			err = t.relayService.Close()
			if err != nil {
				klog.Errorf("[Watcher] Failed to close relayv2 service, err: %v", err)
			} else {
				t.isRelay = false
				t.relayService = nil
				klog.Infof("[Watcher] Disable relayv2 service success")
			}
		}
	}

	t.connectToRelays("Watcher")
}

func (t *EdgeTunnel) Run() {
	go t.runMetricsServer()
	go t.runMdnsDiscovery()
	go t.runDhtDiscovery()
	go t.runConfigWatcher()
	go t.broadcastMessage()
	go t.handleIncomingMessages()
	go t.handleNodeEvents()
	go t.monitorKCPPort(t.hostCtx)
	go t.InterfaceMonitor()
	t.setupConnectionManager()
	t.runHeartbeat()
	t.detectCloudNode()
}

func (t *EdgeTunnel) runMetricsServer() {
	if !t.Config.MetricConfig.Enable {
		klog.Infof("not relay, skip metrics server!")
		return
	}

	klog.Infof("Starting Metrics service")
	obs.MustRegisterWith(prometheus.DefaultRegisterer)
	exporter, err := ocprom.NewExporter(ocprom.Options{
		Registry: prometheus.DefaultRegisterer.(*prometheus.Registry),
	})
	if err != nil {
		klog.Errorf("Failed to create exporter error: %v", err)
		return
	}
	http.Handle("/metrics", exporter)
	port := fmt.Sprintf(":%d", t.Config.MetricConfig.Port)
	klog.Error(http.ListenAndServe(port, nil))
}

// GetCNIAdapterStream establishes a new stream with a destination peer, either directly or through a relay node,
// use net.conn to get income data
func (t *EdgeTunnel) GetCNIAdapterStream(opts ProxyOptions) (*StreamConn, error) {
	var destInfo peer.AddrInfo
	var err error

	destName := opts.NodeName
	nodeInfo, exists := t.nodePeerMap[destName]
	if !exists {
		destID, err := PeerIDFromString(destName)
		if err != nil {
			return nil, fmt.Errorf("failed to generate peer id for %s err: %w", destName, err)
		}
		destInfo = peer.AddrInfo{ID: destID, Addrs: []ma.Multiaddr{}}
		// mapping nodeName and peerID
		klog.Infof("[CNI]Could not find peer %s in cache, auto generate peer info: %s", destName, destInfo)
		nodeInfo = &NodeInfo{
			PeerID:   destID,
			Status:   NodeStatusActive,
			LastSeen: time.Now(),
		}
		t.nodePeerMap[destName] = nodeInfo
		t.nodeEventChan <- NodeEvent{EventName: EventNodeJoined, NodeName: destName, NodeEventInfo: *nodeInfo}
	} else {
		destInfo = t.p2pHost.Peerstore().PeerInfo(nodeInfo.PeerID)
	}
	if err = AddCircuitAddrsToPeer(&destInfo, t.relayMap); err != nil {
		return nil, fmt.Errorf("failed to add circuit addrs to peer %s", destInfo)
	}
	t.p2pHost.Peerstore().AddAddrs(destInfo.ID, destInfo.Addrs, peerstore.PermanentAddrTTL)

	stream, err := t.p2pHost.NewStream(network.WithUseTransient(t.hostCtx, "relay"), nodeInfo.PeerID, defaults.CNIProtocol)
	if err != nil {
		return nil, fmt.Errorf("new stream between %s: %s err: %w", destName, destInfo, err)
	}
	klog.Infof("【CNI】New stream between peer %s: %s success", destName, destInfo)
	// defer stream.Close() // will close the stream elsewhere

	streamWriter := protoio.NewDelimitedWriter(stream)
	streamReader := protoio.NewDelimitedReader(stream, MaxReadSize)

	// handshake with dest peer
	opt := &ProxyOptions{
		Protocol: opts.Protocol,
		NodeName: opts.NodeName,
	}
	msg := &proxypb.Proxy{
		Type:     proxypb.Proxy_CONNECT.Enum(),
		Protocol: &opt.Protocol,
		NodeName: &opt.NodeName,
		Ip:       &opt.IP,
		Port:     &opt.Port,
	}
	if err = streamWriter.WriteMsg(msg); err != nil {
		resetErr := stream.Reset()
		if resetErr != nil {
			return nil, fmt.Errorf("stream between %s reset err: %w", opts.NodeName, resetErr)
		}
		return nil, fmt.Errorf("write conn msg to %s err: %w", opts.NodeName, err)
	}

	// read response
	msg.Reset()
	if err = streamReader.ReadMsg(msg); err != nil {
		resetErr := stream.Reset()
		if resetErr != nil {
			return nil, fmt.Errorf("stream between %s reset err: %w", opts.NodeName, resetErr)
		}
		return nil, fmt.Errorf("read conn result msg from %s err: %w", opts.NodeName, err)
	}
	if msg.GetType() == proxypb.Proxy_FAILED {
		resetErr := stream.Reset()
		if resetErr != nil {
			return nil, fmt.Errorf("stream between %s reset err: %w", opts.NodeName, err)
		}
		return nil, fmt.Errorf("libp2p dial %v err: Proxy.type is %s", opts, msg.GetType())
	}

	msg.Reset()
	klog.V(4).Infof("libp2p dial %v success", opts)

	return NewStreamConn(stream), nil
}

func (t *EdgeTunnel) CNIAdapterStreamHandler(stream network.Stream) {
	remotePeer := peer.AddrInfo{
		ID:    stream.Conn().RemotePeer(),
		Addrs: []ma.Multiaddr{stream.Conn().RemoteMultiaddr()},
	}
	klog.Infof("[CNI]Adapter service got a new stream from %s", remotePeer)

	streamWriter := protoio.NewDelimitedWriter(stream)
	streamReader := protoio.NewDelimitedReader(stream, MaxReadSize) // TODO get maxSize from default

	// read handshake
	// tempUse
	msg := new(proxypb.Proxy)
	err := streamReader.ReadMsg(msg)
	if err != nil {
		klog.Errorf("Read msg from %s err: %v", remotePeer, err)
		return
	}
	if msg.GetType() != proxypb.Proxy_CONNECT {
		klog.Errorf("Read msg from %s type should be CONNECT", remotePeer)
		return
	}
	targetProto := msg.GetProtocol()
	targetNode := msg.GetNodeName()
	targetIP := msg.GetIp()
	targetPort := msg.GetPort()
	targetAddr := fmt.Sprintf("%s:%d", targetIP, targetPort)

	klog.Infof("[CNI] calls cni Dial")
	if err != nil {
		klog.Errorf("l3 CNI proxy connect to %v err: %v", msg, err)
		msg.Reset()
		msg.Type = proxypb.Proxy_FAILED.Enum()
		if err = streamWriter.WriteMsg(msg); err != nil {
			klog.Errorf("Write msg to %s err: %v", remotePeer, err)
			return
		}
		return
	}

	// write response
	msg.Type = proxypb.Proxy_SUCCESS.Enum()
	err = streamWriter.WriteMsg(msg)
	if err != nil {
		klog.Errorf("Write msg to %s err: %v", remotePeer, err)
		return
	}
	msg.Reset()

	streamConn := NewStreamConn(stream)
	klog.Infof("Get Tunnel data")
	go cni.DialTun(streamConn, defaults.TunDeviceName)
	klog.Infof("[CNI]Success proxy CNI data for {%s %s %s}", targetProto, targetNode, targetAddr)
}

func (t *EdgeTunnel) setupConnectionManager() {
	klog.Infof("setupConnectionManager started")
	t.p2pHost.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(n network.Network, conn network.Conn) {
			klog.V(2).Infof("在时刻%s，%s断开连接", time.Now().Format("2006-01-02 15:04:05.000"), conn.RemotePeer())
			t.handleDisconnection(conn.RemotePeer())
		},
		ConnectedF: func(n network.Network, conn network.Conn) {
			// klog.V(2).Infof("在时刻%s，%s连接到我", time.Now().Format("2006-01-02 15:04:05.000"), conn.RemotePeer())
			// t.handleConnection(conn.RemotePeer())
		},
	})
}

func (t *EdgeTunnel) handleConnection(peerID peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	nodeName, ok1 := t.peerIDtoNodeName[peerID]
	nodeInfo, ok2 := t.nodePeerMap[nodeName]
	if ok1 && ok2 {
		if nodeInfo.Status == NodeStatusInactive {
			nodeInfo.Status = NodeStatusActive
			nodeInfo.LastSeen = time.Now()
			t.nodePeerMap[nodeName] = nodeInfo
			go func(name string, info NodeInfo) {
				klog.Infof("在时刻%s，通过回调函数将节点%s的状态设置为活跃", time.Now().Format("2006-01-02 15:04:05.000"), name)
				t.nodeEventChan <- NodeEvent{EventName: EventNodeJoined, NodeName: name, NodeEventInfo: info}
			}(nodeName, *nodeInfo)
		}
	}
}

func (t *EdgeTunnel) handleDisconnection(peerID peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	nodeName, ok1 := t.peerIDtoNodeName[peerID]
	nodeInfo, ok2 := t.nodePeerMap[nodeName]
	if ok1 && ok2 {
		if nodeInfo.Status == NodeStatusActive {
			nodeInfo.Status = NodeStatusInactive
			nodeInfo.LastSeen = time.Now()
			t.nodePeerMap[nodeName] = nodeInfo
			go func(name string, info NodeInfo) {
				klog.V(2).Infof("在时刻%s，通过回调函数将节点%s的状态设置为不活跃", time.Now().Format("2006-01-02 15:04:05.000"), name)
				t.nodeEventChan <- NodeEvent{EventName: EventNodeLeft, NodeName: name, NodeEventInfo: info}
			}(nodeName, *nodeInfo)
		}
	}
}

// func (t *EdgeTunnel) GetDirectStream(destName string, message []byte) ([]byte, error) {
// 	var destInfo peer.AddrInfo
// 	var err error

// 	nodeInfo := t.nodePeerMap[destName]
// 	_, exists := t.activeNodes[destName]
// 	if !exists {
// 		destID, err := PeerIDFromString(destName)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to generate peer id for %s err: %w", destName, err)
// 		}
// 		destInfo = peer.AddrInfo{ID: destID, Addrs: []ma.Multiaddr{}}
// 		// mapping nodeName and peerID
// 		klog.Infof("Could not find peer %s in cache, auto generate peer info: %s", destName, destInfo)
// 		nodeInfo = &NodeInfo{
// 			PeerID:   destID,
// 			Status:   NodeStatusActive,
// 			LastSeen: time.Now(),
// 		}
// 		t.nodePeerMap[destName] = nodeInfo
// 		t.activeNodes[destName] = true
// 		t.nodeEventChan <- NodeEvent{
// 			EventName:     EventNodeJoined,
// 			NodeName:      destName,
// 			NodeEventInfo: *nodeInfo,
// 		}
// 	} else {
// 		destInfo = t.p2pHost.Peerstore().PeerInfo(nodeInfo.PeerID)
// 	}
// 	if err = AddCircuitAddrsToPeer(&destInfo, t.relayMap); err != nil {
// 		return nil, fmt.Errorf("failed to add circuit addrs to peer %s", destInfo)
// 	}
// 	t.p2pHost.Peerstore().AddAddrs(destInfo.ID, destInfo.Addrs, peerstore.PermanentAddrTTL)

// 	stream, err := t.p2pHost.NewStream(network.WithUseTransient(t.hostCtx, "relay"), nodeInfo.PeerID, defaults.DirectProtocol)
// 	if err != nil {
// 		return nil, fmt.Errorf("new stream between %s: %s err: %w", destName, destInfo, err)
// 	}
// 	klog.Infof("New stream between peer %s: %s success", destName, destInfo)

// 	streamWriter := bufio.NewWriter(stream)
// 	streamReader := bufio.NewReader(stream)

// 	// Handshake: Send a ready signal
// 	_, err = streamWriter.Write([]byte("READY"))
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to send ready signal to %s: %w", destName, err)
// 	}
// 	err = streamWriter.Flush()
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to flush ready signal to %s: %w", destName, err)
// 	}

// 	// Wait for the receiver's ready signal
// 	readySignal, err := streamReader.ReadString('\n')
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read ready signal from %s: %w", destName, err)
// 	}
// 	if readySignal != "READY\n" {
// 		return nil, fmt.Errorf("unexpected ready signal from %s: %s", destName, readySignal)
// 	}

// 	// Send message
// 	_, err = streamWriter.Write(message)
// 	if err != nil {
// 		return nil, fmt.Errorf("write message to %s err: %w", destName, err)
// 	} else {
// 		klog.Infof("[Direct] successfully send message to %s", destName)
// 	}
// 	err = streamWriter.Flush()
// 	if err != nil {
// 		return nil, fmt.Errorf("flush message to %s err: %w", destName, err)
// 	}

// 	// Read reply
// 	readChan := make(chan []byte)
// 	errorChan := make(chan error)
// 	go func() {
// 		data, err := io.ReadAll(streamReader)
// 		if err != nil {
// 			errorChan <- err
// 			return
// 		}
// 		readChan <- data
// 	}()

// 	select {
// 	case reply := <-readChan:
// 		klog.Infof("[Direct] successfully received reply from %s", destName)
// 		return reply, nil
// 	case err := <-errorChan:
// 		return nil, fmt.Errorf("read reply from %s err: %w", destName, err)
// 	}
// }

// func (t *EdgeTunnel) directConnectionHandler(stream network.Stream) {
// 	remotePeer := peer.AddrInfo{
// 		ID:    stream.Conn().RemotePeer(),
// 		Addrs: []ma.Multiaddr{stream.Conn().RemoteMultiaddr()},
// 	}
// 	klog.Infof("Direct connection service got a new stream from %s", remotePeer)
// 	defer stream.Close()

// 	streamWriter := bufio.NewWriter(stream)
// 	streamReader := bufio.NewReader(stream)

// 	// Handshake: Send a ready signal
// 	_, err := streamWriter.Write([]byte("READY"))
// 	if err != nil {
// 		klog.Errorf("Failed to send ready signal to %s: %v", remotePeer, err)
// 		return
// 	}
// 	err = streamWriter.Flush()
// 	if err != nil {
// 		klog.Errorf("Failed to flush ready signal to %s: %v", remotePeer, err)
// 		return
// 	}

// 	// Read message with retries and timeout
// 	retries := 3
// 	retryInterval := 1 * time.Second
// 	var data []byte
// 	for i := 0; i < retries; i++ {
// 		readChan := make(chan []byte)
// 		errorChan := make(chan error)
// 		go func() {
// 			data, err := io.ReadAll(streamReader)
// 			if err != nil {
// 				errorChan <- err
// 				return
// 			}
// 			readChan <- data
// 		}()

// 		select {
// 		case data = <-readChan:
// 			break
// 		case err := <-errorChan:
// 			klog.Errorf("Error reading message from %s: %v", remotePeer, err)
// 			time.Sleep(retryInterval)
// 			continue
// 		case <-time.After(5 * time.Second): // Adjust the timeout as needed
// 			klog.Errorf("Timeout reading message from %s", remotePeer)
// 			time.Sleep(retryInterval)
// 			continue
// 		}
// 	}

// 	if data == nil {
// 		klog.Errorf("Failed to read message from %s after retries", remotePeer)
// 		return
// 	}

// 	var msg model.Message
// 	err = json.Unmarshal(data, &msg)
// 	if err != nil {
// 		klog.Errorf("Failed to unmarshal message: %v", err)
// 		return
// 	}
// 	klog.Infof("[Direct] successfully received message %s from %s", msg.String(), remotePeer.ID)
// 	beehiveContext.Send(defaults.MeshServerName, msg)

// 	// Send reply
// 	reply := []byte("REPLY: ")
// 	_, err = streamWriter.Write(reply)
// 	if err != nil {
// 		klog.Errorf("Write reply to %s err: %v", remotePeer, err)
// 		return
// 	}
// 	err = streamWriter.Flush()
// 	if err != nil {
// 		klog.Errorf("Flush reply to %s err: %v", remotePeer, err)
// 		return
// 	}

// 	klog.Infof("Sent reply to %s", remotePeer)
// }

func (t *EdgeTunnel) broadcastMessage() error {
	klog.Infof("broadcastMessage started")
	err := wait.PollUntil(time.Second, func() (bool, error) {
		msg, err := beehiveContext.Receive(defaults.EdgeTunnelModuleName)
		if err != nil {
			klog.Errorf("Failed to receive message to %s: %v", defaults.EdgeTunnelModuleName, err)
			return false, nil // continue polling
		}

		//klog.Infof("Received message to %s: %+v", defaults.EdgeTunnelModuleName, msg)
		data, err := json.Marshal(msg)
		if err != nil {
			klog.Errorf("Failed to marshal message: %v", err)
			return false, nil // continue polling
		}

		err = t.Topic.Publish(t.hostCtx, data)
		if err != nil {
			klog.Errorf("Failed to publish message: %v", err)
			return false, nil // continue polling
		}

		klog.Infof("Successfully broadcasted metadata for node %s", t.Config.NodeName)
		return false, nil // continue polling
	}, t.stopCh)
	return err
}

func (t *EdgeTunnel) handleIncomingMessages() error {
	klog.Infof("handleIncomingMessages started")
	err := wait.PollUntil(time.Second, func() (bool, error) {
		msg, err := t.Sub.Next(t.hostCtx)
		if err != nil {
			klog.Errorf("Error receiving message: %v", err)
			return false, nil // continue polling
		}

		var receivedMsg model.Message
		err = json.Unmarshal(msg.Data, &receivedMsg)
		if err != nil {
			klog.Errorf("Error unmarshalling message: %v", err)
			return false, nil // continue polling
		}

		// Skip messages from self
		if receivedMsg.GetOperation() == messagepkg.NodeJoined {
			if msg.ReceivedFrom == t.p2pHost.ID() {
				klog.V(4).Infof("Skipping message from self")
				return false, nil // continue polling
			}
		}

		klog.Infof("Received message from %s: %+v", msg.ReceivedFrom, receivedMsg.String())
		// Add your logic to handle the received message here
		if receivedMsg.GetOperation() == messagepkg.DetectCloudNode {
			t.CloudNode = receivedMsg.GetContent().(string)
		} else {
			beehiveContext.Send(defaults.EdgeProxyModuleName, receivedMsg)
		}

		return false, nil // continue polling
	}, t.stopCh)
	return err
}

func (t *EdgeTunnel) handleNodeEvents() {
	processedEvents := make(map[string]time.Time)
	deduplicationWindow := 5 * time.Second

	for e := range t.nodeEventChan {
		go func(event NodeEvent) {
			eventKey := fmt.Sprintf("%s-%s", event.EventName, event.NodeName)
			t.mu2.Lock()
			lastProcessed, exists := processedEvents[eventKey]
			currentTime := time.Now()
			if !exists || currentTime.Sub(lastProcessed) > deduplicationWindow {
				processedEvents[eventKey] = currentTime
				t.mu2.Unlock()

				// Process the event
				//currentTimeStr := currentTime.Format("2006-01-02 15:04:05.000")
				switch event.EventName {
				case EventNodeJoined:
					//klog.Infof("Node joined event received at %s", currentTimeStr)
					klog.Infof("Node joined: %s", event.NodeName)

					if event.NodeName == t.CloudNode {
						msg := model.NewMessage("").
							BuildRouter(t.Config.NodeName, "edgemesh", "config", messagepkg.IsSync).
							FillBody(map[string]interface{}{
								"isSync": true,
							})
						beehiveContext.Send(defaults.EdgeProxyModuleName, *msg)
						t.isCloudNodeOnline = true
					}

					if !t.isCloudNodeOnline {
						if v1alpha1.DetectRunningMode() != defaults.CloudMode {
							msg, err := t.BuildselfEndpointsMsg()
							if err != nil {
								klog.Errorf("Failed to build msg: %v", err)
							} else {
								//klog.Infof("[%s]: 成功创建本机的Endpoints信息%s", defaults.EdgeTunnelModuleName, msg.Header)
								beehiveContext.Send(defaults.EdgeTunnelModuleName, *msg)
							}
						}
					}
				case EventNodeLeft:
					//klog.Infof("Node left event received at %s", currentTimeStr)
					klog.Infof("Node left: %s", event.NodeName)

					if event.NodeName == t.CloudNode {
						klog.Infof("云节点%s离线", event.NodeName)
						t.isCloudNodeOnline = false
					}
					if !t.isCloudNodeOnline {
						if v1alpha1.DetectRunningMode() != defaults.CloudMode {
							msg := model.NewMessage("").
								BuildRouter(t.Config.NodeName, "edgemesh", "service", messagepkg.NodeLeft).
								FillBody(event.NodeName)
							beehiveContext.Send(defaults.EdgeProxyModuleName, *msg)
						}
					}
				}
			} else {
				t.mu.Unlock()
				klog.V(4).Infof("Skipping duplicate event: %s for node %s", event.EventName, event.NodeName)
			}
		}(e)
	}
}

func (t *EdgeTunnel) detectCloudNode() {
	if v1alpha1.DetectRunningMode() == defaults.CloudMode {
		t.CloudNode = t.Config.NodeName
		msg := model.NewMessage("").
			BuildRouter(t.Config.NodeName, "edgemesh", "service", messagepkg.DetectCloudNode).
			FillBody(t.Config.NodeName)
		beehiveContext.Send(defaults.EdgeTunnelModuleName, *msg)
		klog.Infof("云节点是%s", t.Config.NodeName)
	}
}

func (t *EdgeTunnel) handleKCPConnection(ctx context.Context, conn *kcp.UDPSession) {
	defer conn.Close()

	// 设置一些KCP的参数
	conn.SetStreamMode(true)
	conn.SetWriteDelay(false)
	conn.SetNoDelay(1, 10, 2, 1)
	conn.SetMtu(1350)
	conn.SetWindowSize(128, 128)
	conn.SetACKNoDelay(true)

	buf := make([]byte, 65507)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 设置读取超时
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				//klog.ErrorS(err, "Error reading from KCP connection")
				return
			}

			t.mu.Lock()
			t.udpBuffer.Write(buf[:n])
			data := t.udpBuffer.Bytes()
			if isValidJSON(data) {
				klog.V(1).InfoS("Received complete KCP packet", "from", conn.RemoteAddr().String())

				msg := model.NewMessage("").
					BuildRouter("quickupdate", "edgemesh", "service", messagepkg.NodeJoined).
					FillBody(string(data))

				klog.Infof("通过UDP收到消息%s", msg.String())
				beehiveContext.Send(defaults.EdgeProxyModuleName, *msg)

				// Clear the buffer after processing
				t.udpBuffer.Reset()
			} else {
				klog.V(1).Infof("Received incomplete KCP packet, from %s", conn.RemoteAddr().String())
			}
			t.mu.Unlock()
		}
	}
}

func (t *EdgeTunnel) monitorKCPPort(ctx context.Context) {
	// 创建KCP监听器
	listener, err := kcp.ListenWithOptions(broadcastAddr, nil, 0, 0)
	if err != nil {
		klog.ErrorS(err, "Failed to create KCP listener", "address", broadcastAddr)
		return
	}
	t.kcpListener = listener
	defer listener.Close()

	klog.InfoS("Started listening on KCP", "address", broadcastAddr)

	for {
		select {
		case <-ctx.Done():
			klog.InfoS("Stopping KCP monitor")
			return
		default:
			conn, err := listener.AcceptKCP()
			if err != nil {
				klog.ErrorS(err, "Error accepting KCP connection")
				continue
			}

			go t.handleKCPConnection(ctx, conn)
		}
	}
}
