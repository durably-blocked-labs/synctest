package raduplicaterequest

import "sync"

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
