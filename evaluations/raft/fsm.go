package rafteval

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	entry := AppliedEntry{Index: log.Index, Term: log.Term, Data: append([]byte(nil), log.Data...)}
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
	copyOfHistory := make([]AppliedEntry, len(history))
	for i, entry := range history {
		copyOfHistory[i] = AppliedEntry{
			Index: entry.Index,
			Term:  entry.Term,
			Data:  append([]byte(nil), entry.Data...),
		}
	}
	return copyOfHistory
}

// CheckAgreement verifies that histories never contain different commands at
// the same applied log index. Histories may end at different indexes.
func CheckAgreement(histories map[raft.ServerID][]AppliedEntry) error {
	commands := make(map[uint64][]byte)
	owners := make(map[uint64]raft.ServerID)
	for id, history := range histories {
		for _, entry := range history {
			if command, ok := commands[entry.Index]; ok {
				if !bytes.Equal(command, entry.Data) {
					return fmt.Errorf(
						"log index %d differs: node %s applied %q, node %s applied %q",
						entry.Index,
						owners[entry.Index],
						command,
						id,
						entry.Data,
					)
				}
				continue
			}
			commands[entry.Index] = append([]byte(nil), entry.Data...)
			owners[entry.Index] = id
		}
	}
	return nil
}
