package tunnel

import (
	// ... existing imports ...

	"sync"
	"time"
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

func GetCache(key string) (string, time.Time, bool) {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	entry, found := cache[key]
	if !found {
		return "", time.Time{}, false
	}
	return entry.data, entry.timestamp, true
}
