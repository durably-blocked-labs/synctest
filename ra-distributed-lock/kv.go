package lock

import "sync"

// KVStore is a simple in-memory key-value store. Individual Get and Put
// calls are safe for concurrent use, but read-modify-write sequences are
// NOT atomic — callers must use external synchronization (e.g. the RA
// distributed lock) to prevent lost updates.
type KVStore struct {
	mu   sync.Mutex
	data map[string]int
}

func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]int)}
}

func (s *KVStore) Get(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[key]
}

func (s *KVStore) Put(key string, value int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}
