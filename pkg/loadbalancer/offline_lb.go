package loadbalancer

import (
	"encoding/json"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
	utilproxy "k8s.io/kubernetes/pkg/proxy/util"
)

// ParseJSONToEndpoints 解析 JSON 字符串为 v1.Endpoints 集合
func ParseJSONToEndpoints(jsonData string) ([]v1.Endpoints, error) {
	var endpointsList v1.EndpointsList
	err := json.Unmarshal([]byte(jsonData), &endpointsList)
	if err != nil {
		return nil, fmt.Errorf("error decoding JSON: %w", err)
	}
	return endpointsList.Items, nil
}

// 删除与离线node相关的endpoints信息
func (lb *LoadBalancer) removeNodeByName(state *balancerState, nodeNameToRemove string) {
	var updatedEndpoints []string

	for _, endpoint := range state.endpoints {
		parts := strings.Split(endpoint, ":")
		if len(parts) < 4 {
			// 端点格式不正确，跳过
			updatedEndpoints = append(updatedEndpoints, endpoint)
			continue
		}

		nodeName := parts[0]
		if nodeName != nodeNameToRemove {
			// 保留不是要删除的节点
			updatedEndpoints = append(updatedEndpoints, endpoint)
		}
	}
	klog.Infof("保留的下的Endpoints:%v", updatedEndpoints)

	state.endpoints = updatedEndpoints
}

// 删除 endpoints 为空的服务
func (lb *LoadBalancer) RemoveEmptyEndpointsServices() {
	for servicePort, balancer := range lb.services {
		if len(balancer.endpoints) == 0 {
			klog.Infof("Removing service: Namespace=%s, Name=%s, Port=%s", servicePort.Namespace, servicePort.Name, servicePort.Port)
			delete(lb.services, servicePort)
		}
	}

	svcPort := proxy.ServicePortName{
		NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "hostname-lb-svc",
		},
		Port: "http-0",
	}
	klog.Infof("lb-svc:%v", lb.services[svcPort])
}

// mergeEndpoints 用于合并新旧 endpoints
func (lb *LoadBalancer) mergeEndpoints(endpoints *v1.Endpoints) {
	// 根据 endpoints 生成端口到 endpoint 的映射
	portsToEndpoints := buildPortsToEndpointsMap(endpoints)

	// 加锁，保证线程安全
	lb.lock.Lock()
	defer lb.lock.Unlock()

	// 获取服务名称
	svcName := types.NamespacedName{Namespace: endpoints.Namespace, Name: endpoints.Name}

	// 遍历所有的端口
	for portname := range portsToEndpoints {
		// 构造服务端口名称
		svcPort := proxy.ServicePortName{NamespacedName: svcName, Port: portname}
		// 获取该端口对应的新的 endpoints
		newEndpoints := portsToEndpoints[portname]
		// 获取当前服务的状态
		state, exists := lb.services[svcPort]

		if !exists || state == nil {
			// 如果服务不存在，则创建一个新的服务并设置 endpoints
			klog.V(1).InfoS("Setting new service and endpoints", "servicePortName", svcPort, "endpoints", newEndpoints)
			state = lb.newServiceInternal(svcPort, v1.ServiceAffinity(""), 0)
			state.endpoints = utilproxy.ShuffleStrings(newEndpoints)
		} else {
			// 如果服务存在，则合并新的和旧的 endpoints
			klog.V(1).InfoS("Merging new endpoints with existing service", "servicePortName", svcPort, "newEndpoints", newEndpoints)
			// 将旧的和新的 endpoints 合并并去重
			endpointSet := make(map[string]struct{})
			for _, ep := range state.endpoints {
				endpointSet[ep] = struct{}{}
			}
			for _, ep := range newEndpoints {
				if _, exists := endpointSet[ep]; !exists {
					state.endpoints = append(state.endpoints, ep)
				}
			}
			// 打乱合并后的 endpoints 列表
			state.endpoints = utilproxy.ShuffleStrings(state.endpoints)
		}

		// 重置 round-robin 的索引
		state.index = 0

		// 同步后端负载均衡策略
		lb.policyMutex.Lock()
		if policy, exists := lb.policyMap[svcPort]; exists {
			policy.Sync(state.endpoints)
		}
		lb.policyMutex.Unlock()
	}
}

func (lb *LoadBalancer) removePodByName(state *balancerState, podNameToRemove string) {
	var updatedEndpoints []string

	for _, endpoint := range state.endpoints {
		parts := strings.Split(endpoint, ":")
		if len(parts) < 4 {
			// 端点格式不正确，跳过
			updatedEndpoints = append(updatedEndpoints, endpoint)
			continue
		}

		podName := parts[1]
		if podName != podNameToRemove {
			// 保留不是要删除的 pod
			updatedEndpoints = append(updatedEndpoints, endpoint)
		}
	}

	state.endpoints = updatedEndpoints
}
