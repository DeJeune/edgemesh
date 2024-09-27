package tunnel

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	ipfslog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	p2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/host/resource-manager/obs"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	ma "github.com/multiformats/go-multiaddr"
	"k8s.io/klog/v2"

	"github.com/kubeedge/beehive/pkg/core"

	"github.com/kubeedge/edgemesh/pkg/apis/config/defaults"
	"github.com/kubeedge/edgemesh/pkg/apis/config/v1alpha1"
	"github.com/kubeedge/edgemesh/pkg/clients"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/xtaci/kcp-go"
)

// Agent expose the tunnel ability.  TODO convert var to func
var Agent *EdgeTunnel

type NodeStatus int

const (
	NodeStatusActive NodeStatus = iota
	NodeStatusInactive
)

type NodeInfo struct {
	PeerID   peer.ID
	Status   NodeStatus
	LastSeen time.Time
}

type NodeEvent struct {
	EventName     string   `json:"eventName"`
	NodeName      string   `json:"nodeName"`
	NodeEventInfo NodeInfo `json:"nodeEventInfo,omitempty"`
}

const (
	EventPing       = "Ping"
	EventNodeJoined = "NodeJoined"
	EventNodeLeft   = "NodeLeft"
)

// EdgeTunnel is used for solving cross subset communication
type EdgeTunnel struct {
	Config            *v1alpha1.EdgeTunnelConfig
	CloudNode         string
	p2pHost           p2phost.Host         // libp2p host
	hostCtx           context.Context      // ctx governs the lifetime of the libp2p host
	nodePeerMap       map[string]*NodeInfo // map of Kubernetes node name and peer.ID
	peerIDtoNodeName  map[peer.ID]string
	mdnsPeerChan      chan peer.AddrInfo
	dhtPeerChan       <-chan peer.AddrInfo
	isRelay           bool
	relayMap          RelayMap
	relayService      *relayv2.Relay
	holepunchService  *holepunch.Service
	stopCh            chan struct{}
	cfgWatcher        *fsnotify.Watcher
	pubSub            *pubsub.PubSub
	Topic             *pubsub.Topic
	Sub               *pubsub.Subscription
	nodeEventChan     chan NodeEvent
	disconnectChan    chan struct{}
	mu                sync.RWMutex
	mu2               sync.Mutex
	Clients           *clients.Clients
	udpBuffer         bytes.Buffer
	kcpListener       *kcp.Listener
	isCloudNodeOnline bool
}

// Name of EdgeTunnel
func (t *EdgeTunnel) Name() string {
	return defaults.EdgeTunnelModuleName
}

// Group of EdgeTunnel
func (t *EdgeTunnel) Group() string {
	return defaults.EdgeTunnelModuleName
}

// Enable indicates whether enable this module
func (t *EdgeTunnel) Enable() bool {
	return t.Config.Enable
}

// Start EdgeTunnel
func (t *EdgeTunnel) Start() {
	t.Run()
}

func (t *EdgeTunnel) Shutdown() {
	close(t.stopCh)
}

// Register edgetunnel to beehive modules
func Register(c *v1alpha1.EdgeTunnelConfig, cli *clients.Clients) error {
	agent, err := newEdgeTunnel(c, cli)
	if err != nil {
		return fmt.Errorf("register module EdgeTunnel error: %v", err)
	}
	core.Register(agent)
	return nil
}

func newEdgeTunnel(c *v1alpha1.EdgeTunnelConfig, cli *clients.Clients) (*EdgeTunnel, error) {
	if !c.Enable {
		return &EdgeTunnel{Config: c}, nil
	}

	if c.EnableIpfsLog {
		ipfslog.SetAllLoggers(ipfslog.LevelDebug)
	}

	ctx := context.Background()
	opts := make([]libp2p.Option, 0) // libp2p options
	peerSource := make(chan peer.AddrInfo, c.MaxCandidates)

	privKey, err := GenerateKeyPairWithString(c.NodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	connMgr, err := connmgr.NewConnManager(
		100, // LowWater
		400, // HighWater,
		connmgr.WithGracePeriod(time.Minute))
	if err != nil {
		return nil, fmt.Errorf("failed to new conn manager: %w", err)
	}

	listenAddr, err := generateListenAddr(c)
	if err != nil {
		return nil, fmt.Errorf("failed to generate listenAddr: %w", err)
	}

	// If this host is a relay node, we need to add its advertiseAddress
	relayMap := GenerateRelayMap(c.RelayNodes, c.Transport, c.ListenPort)
	myInfo, isRelay := relayMap[c.NodeName]
	if isRelay && c.Mode == defaults.ServerClientMode {
		opts = append(opts, libp2p.AddrsFactory(func(maddrs []ma.Multiaddr) []ma.Multiaddr {
			maddrs = append(maddrs, myInfo.Addrs...)
			return maddrs
		}))
	}

	// If the relayMap does not contain any public IP, NATService will not be able to assist this non-relay node to
	// identify its own network(public, private or unknown), so it needs to configure libp2p.ForceReachabilityPrivate()
	if !isRelay && !relayMap.ContainsPublicIP() {
		klog.Infof("Configure libp2p.ForceReachabilityPrivate()")
		opts = append(opts, libp2p.ForceReachabilityPrivate())
	}

	relayNums := len(relayMap)
	if c.MaxCandidates < relayNums {
		klog.Infof("MaxCandidates=%d is less than len(relayMap)=%d, set MaxCandidates to len(relayMap)",
			c.MaxCandidates, relayNums)
		c.MaxCandidates = relayNums
	}

	// configures libp2p to use the given private network protector
	if c.PSK.Enable {
		pskReader, err := GeneratePSKReader(c.PSK.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to generate psk reader: %w", err)
		}
		psk, err := pnet.DecodeV1PSK(pskReader)
		if err != nil {
			return nil, fmt.Errorf("failed to decode v1 psk: %w", err)
		}
		opts = append(opts, libp2p.PrivateNetwork(psk))
	}

	var ddht *dual.DHT
	opts = append(opts, []libp2p.Option{
		libp2p.Identity(privKey),
		listenAddr,
		libp2p.DefaultSecurity,
		GenerateTransportOption(c.Transport),
		libp2p.ConnectionManager(connMgr),
		libp2p.NATPortMap(),
		libp2p.Routing(func(h p2phost.Host) (routing.PeerRouting, error) {
			ddht, err = newDHT(ctx, h, relayMap)
			return ddht, err
		}),
		libp2p.EnableAutoRelayWithPeerSource(
			func(ctx context.Context, numPeers int) <-chan peer.AddrInfo {
				return peerSource
			},
			autorelay.WithMinCandidates(0),
			autorelay.WithMaxCandidates(c.MaxCandidates),
			autorelay.WithBackoff(30*time.Second),
		),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
	}...)

	rcMgrOpts := make([]rcmgr.Option, 0)
	if c.MetricConfig.Enable {
		reporter, _ := obs.NewStatsTraceReporter()
		rcMgrOpts = append(rcMgrOpts, rcmgr.WithTraceReporter(reporter))
	}
	//Adjust stream limit
	if limitOpt, err := CreateLimitOpt(c.TunnelLimitConfig, rcMgrOpts...); err == nil {
		opts = append(opts, limitOpt)
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to new p2p host: %w", err)
	}
	klog.V(0).Infof("I'm %s\n", fmt.Sprintf("{%v: %v}", h.ID(), h.Addrs()))

	// If this host is a relay node, we need to run libp2p relayv2 service
	var relayService *relayv2.Relay
	if isRelay && c.Mode == defaults.ServerClientMode {
		relayService, err = relayv2.New(h, relayv2.WithLimit(nil)) // TODO close relayService
		if err != nil {
			return nil, fmt.Errorf("run libp2p relayv2 service error: %w", err)
		}
		klog.Infof("Run as a relay node")
	}

	// new hole punching service TODO fix hole punch not working
	ids, err := identify.NewIDService(h)
	if err != nil {
		return nil, fmt.Errorf("new id service error: %w", err)
	}
	holepunchService, err := holepunch.NewService(h, ids)
	if err != nil {
		return nil, fmt.Errorf("run libp2p holepunch service error: %w", err)
	}

	klog.Infof("Bootstrapping the DHT")
	if err = ddht.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("failed to bootstrap dht: %w", err)
	}

	// connect to bootstrap
	err = BootstrapConnect(ctx, h, relayMap)
	if err != nil {
		// We don't want to return error here, so that some
		// edge region that don't have access to the external
		// network can still work
		klog.Warningf("Failed to connect bootstrap: %v", err)
	}

	// init discovery services
	mdnsPeerChan, err := initMDNS(h, c.Rendezvous)
	if err != nil {
		return nil, fmt.Errorf("init mdns discovery error: %w", err)
	}
	dhtPeerChan, err := initDHT(ctx, ddht, c.Rendezvous)
	if err != nil {
		return nil, fmt.Errorf("init dht discovery error: %w", err)
	}

	// init config watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("init config watcher errror: %w", err)
	}
	err = watcher.Add(c.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to add watch in %s, err: %w", c.ConfigPath, err)
	}
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("dial error: %v", err)
	}

	edgeTunnel := &EdgeTunnel{
		Config:            c,
		p2pHost:           h,
		hostCtx:           ctx,
		nodePeerMap:       make(map[string]*NodeInfo),
		peerIDtoNodeName:  make(map[peer.ID]string),
		mdnsPeerChan:      mdnsPeerChan,
		dhtPeerChan:       dhtPeerChan,
		isRelay:           isRelay,
		relayMap:          relayMap,
		relayService:      relayService,
		holepunchService:  holepunchService,
		stopCh:            make(chan struct{}),
		cfgWatcher:        watcher,
		pubSub:            ps,
		nodeEventChan:     make(chan NodeEvent, 100),
		disconnectChan:    make(chan struct{}, 1),
		Clients:           cli,
		udpBuffer:         bytes.Buffer{},
		isCloudNodeOnline: true,
		CloudNode:         "cloud",
	}

	// run relay finder
	go edgeTunnel.runRelayFinder(ddht, peerSource, time.Duration(c.FinderPeriod)*time.Second)

	// register stream handlers
	if c.Mode == defaults.ServerClientMode {
		h.SetStreamHandler(defaults.DiscoveryProtocol, edgeTunnel.discoveryStreamHandler)
		h.SetStreamHandler(defaults.ProxyProtocol, edgeTunnel.proxyStreamHandler)
		h.SetStreamHandler(defaults.CNIProtocol, edgeTunnel.CNIAdapterStreamHandler)
		// h.SetStreamHandler(defaults.DirectProtocol, edgeTunnel.directConnectionHandler)
	}
	topic, sub := edgeTunnel.subscribeMetadataTopic()
	edgeTunnel.Topic = topic
	edgeTunnel.Sub = sub

	Agent = edgeTunnel
	return edgeTunnel, nil
}

func generateListenAddr(c *v1alpha1.EdgeTunnelConfig) (libp2p.Option, error) {
	ips, err := GetIPsFromInterfaces(c.ListenInterfaces, c.ExtraFilteredInterfaces)
	if err != nil {
		return nil, fmt.Errorf("failed to get ips from listen interfaces: %w", err)
	}

	multiAddrStrings := make([]string, 0)
	if c.Mode == defaults.ServerClientMode {
		for _, ip := range ips {
			multiAddrStrings = append(multiAddrStrings, GenerateMultiAddrString(c.Transport, ip, c.ListenPort))
		}
	} else {
		for _, ip := range ips {
			multiAddrStrings = append(multiAddrStrings, GenerateMultiAddrString(c.Transport, ip, c.ListenPort+1))
		}
	}

	listenAddr := libp2p.ListenAddrStrings(multiAddrStrings...)
	return listenAddr, nil
}

func (t *EdgeTunnel) subscribeMetadataTopic() (*pubsub.Topic, *pubsub.Subscription) {
	topicName := "edgemesh-metadata"
	topic, err := t.pubSub.Join(topicName)
	if err != nil {
		klog.Errorf("订阅主题 %s 失败: %v", topic, err)
		return nil, nil
	}
	klog.Infof("成功订阅主题: %s", topic)
	sub, err := topic.Subscribe()
	if err != nil {
		klog.Errorf("订阅主题 %s 失败: %v", topic, err)
		return nil, nil
	}
	return topic, sub
}
