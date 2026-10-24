package rafteval

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"sync"

	"github.com/hashicorp/raft"
)

// AppliedEntry is one command observed by an application FSM.
type AppliedEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
}

// RecordingFSM records applied commands with their Raft log positions.
type RecordingFSM struct {
	mu      sync.Mutex
	history []AppliedEntry
}

// NewRecordingFSM creates an empty recording FSM.
func NewRecordingFSM() *RecordingFSM {
	return &RecordingFSM{}
}

// Apply implements raft.FSM.
func (f *RecordingFSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := AppliedEntry{Index: log.Index, Term: log.Term, Data: bytes.Clone(log.Data)}
	f.history = append(f.history, entry)
	return entry.Index
}

// History returns a deep copy of the applied command history.
func (f *RecordingFSM) History() []AppliedEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneHistory(f.history)
}

// Snapshot implements raft.FSM.
func (f *RecordingFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &recordingSnapshot{history: f.History()}, nil
}

// Restore implements raft.FSM.
func (f *RecordingFSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	var history []AppliedEntry
	if err := json.NewDecoder(snapshot).Decode(&history); err != nil {
		return err
	}
	f.mu.Lock()
	f.history = cloneHistory(history)
	f.mu.Unlock()
	return nil
}

type recordingSnapshot struct {
	history []AppliedEntry
}

func (s *recordingSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.history); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *recordingSnapshot) Release() {}

func cloneHistory(history []AppliedEntry) []AppliedEntry {
	copyOfHistory := slices.Clone(history)
	for i := range copyOfHistory {
		copyOfHistory[i].Data = bytes.Clone(copyOfHistory[i].Data)
	}
	return copyOfHistory
}
