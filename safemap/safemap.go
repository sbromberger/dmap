package safemap

import (
	"fmt"
	"sync"
)

// KeyType is anything that is comparable that has a Hash() function.
// The Hash() function returns an int; the rank is determined by modulus.
type KeyType interface {
	comparable
	Hash() int
}

// ValType is anything that implements an Empty() function.
// The Empty() function is designed to return true if the value doesn't exist.
type ValType interface {
	Empty() bool
}

// SafeMap is a thread-safe map protected by a mutex.
type SafeMap[K KeyType, V ValType] struct {
	mu sync.RWMutex
	mp map[K]V
}

func (m *SafeMap[K, V]) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fmt.Sprintf("%v", m.mp)
}

// New creates a new SafeMap
func New[K KeyType, V ValType]() *SafeMap[K, V] {
	return &SafeMap[K, V]{mp: make(map[K]V)}
}

// Set sets a key/value pair on a SafeMap.
func (sm *SafeMap[K, V]) Set(k K, v V) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mp[k] = v
}

// Get retrieves a value from a SafeMap along with a boolean indicating
// whether the key existed.
func (sm *SafeMap[K, V]) Get(k K) (V, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, found := sm.mp[k]
	return v, found
}

// Size returns the number of entries in the SafeMap.
func (sm *SafeMap[K, V]) Size() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return uint64(len(sm.mp))
}
