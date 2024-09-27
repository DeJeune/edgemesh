package tunnel

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/kubeedge/beehive/pkg/core/model"
	v1 "k8s.io/api/core/v1"
)

func FetchFormattedJSONFromURL(url string) (string, error) {
	cacheData, timestamp, found := GetCache(url)
	if found && time.Since(timestamp) < 30*time.Minute {
		return cacheData, nil
	}
	// 发送 GET 请求到指定的 URL
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("读取响应数据失败: %v", err)
	}

	// 将响应数据解析为 JSON 格式
	var jsonData interface{}
	err = json.Unmarshal(body, &jsonData)
	if err != nil {
		return "", fmt.Errorf("JSON 解码失败: %v", err)
	}

	// 将解析后的 JSON 数据格式化为字符串
	formattedJSON, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return "", fmt.Errorf("格式化 JSON 数据失败: %v", err)
	}

	// 更新缓存
	SetCache(url, string(formattedJSON))

	return string(formattedJSON), nil
}

func (t *EdgeTunnel) BuildselfEndpointsMsg() (*model.Message, error) {
	url := "http://localhost:10550/api/v1/endpoints"
	jsonData, err := FetchFormattedJSONFromURL(url)
	if err != nil {
		return nil, fmt.Errorf("发生错误: %w", err)
	}

	// 过滤出特定 nodeName 的 endpoints
	filteredEndpoints, err := filterEndpointsByNodeName(jsonData, t.Config.NodeName)
	if err != nil {
		return nil, fmt.Errorf("过滤发生错误: %w", err)
	}

	// 构建消息对象
	message := model.NewMessage("").
		BuildRouter(t.Config.NodeName, "edgemesh", "service", "internalJoin").
		FillBody(string(filteredEndpoints))

	return message, nil
}

// filterEndpointsByNodeName 过滤 Endpoints，保留指定 nodeName 的 address。
// 如果 address 为空，则删除整个 Endpoints 对象。
func filterEndpointsByNodeName(jsonStr string, targetNodeName string) ([]byte, error) {
	var endpointsList v1.EndpointsList
	err := json.Unmarshal([]byte(jsonStr), &endpointsList)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	filteredItems := []v1.Endpoints{}

	for _, endpoint := range endpointsList.Items {
		filteredSubsets := []v1.EndpointSubset{}

		for _, subset := range endpoint.Subsets {
			filteredAddresses := []v1.EndpointAddress{}

			for _, address := range subset.Addresses {
				// 只保留指定 nodeName 的 address
				if address.NodeName != nil && *address.NodeName == targetNodeName {
					filteredAddresses = append(filteredAddresses, address)
				}
			}

			// 如果 filteredAddresses 不为空，将 subset 添加到 filteredSubsets
			if len(filteredAddresses) > 0 {
				subset.Addresses = filteredAddresses
				filteredSubsets = append(filteredSubsets, subset)
			}
		}

		// 如果 filteredSubsets 不为空，将 endpoint 添加到 filteredItems
		if len(filteredSubsets) > 0 {
			endpoint.Subsets = filteredSubsets
			filteredItems = append(filteredItems, endpoint)
		}
	}

	// 更新过滤后的 EndpointsList
	endpointsList.Items = filteredItems

	// 将过滤后的 EndpointsList 序列化回 JSON
	filteredJSON, err := json.MarshalIndent(endpointsList, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("error marshalling filtered JSON: %v", err)
	}

	return filteredJSON, nil
}
