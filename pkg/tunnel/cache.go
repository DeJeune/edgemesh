package tunnel

import (
	// ... existing imports ...
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

var (
	cache      = make(map[string]cacheEntry)
	cacheMutex sync.RWMutex
)

type cacheEntry struct {
	data      string
	timestamp time.Time
}

func SetCache(key string, data string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	cache[key] = cacheEntry{data: data, timestamp: time.Now()}
}

func GetCache(key string) (string, bool) {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	entry, found := cache[key]
	if !found {
		return "", false
	}
	return entry.data, true
}

func RefreshCache(url string) error {
	// 发送 GET 请求到指定的 URL
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应数据失败: %v", err)
	}

	// 将响应数据解析为 JSON 格式
	var jsonData interface{}
	err = json.Unmarshal(body, &jsonData)
	if err != nil {
		return fmt.Errorf("JSON 解码失败: %v", err)
	}

	// 将解析后的 JSON 数据格式化为字符串
	formattedJSON, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return fmt.Errorf("格式化 JSON 数据失败: %v", err)
	}

	// 更新缓存
	cacheMutex.Lock()
	SetCache(url, string(formattedJSON))
	cacheMutex.Unlock()

	return nil
}

// StartCacheRefresher 启动一个后台 goroutine 来定期刷新缓存
func (t *EdgeTunnel) StartCacheRefresher(url string) {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
			err := RefreshCache(url)
			if err != nil {
				klog.ErrorS(err, "Failed to refresh cache")
			}
		}
	}()
}
