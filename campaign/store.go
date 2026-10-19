// Package campaign provides bounded, checked artifacts for independent workers.
// It does not control application inputs or make external work deterministic.
package campaign

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const SchemaVersion = 1

// TaskBinding is mandatory for the independent-process runner, optional for
// standalone stores. It pins the dispatched task, not just artifact consistency.
type TaskBinding struct {
	ManifestDigest string `json:"manifest_digest"`
	TaskID         string `json:"task_id"`
	Seed           int64  `json:"seed"`
	CommandDigest  string `json:"command_digest"`
	InputDigest    string `json:"input_digest"`
}

type Manifest struct {
	Task             *TaskBinding      `json:"task,omitempty"`
	Version          int               `json:"version"`
	Workload         string            `json:"workload"`
	InputDigest      string            `json:"input_digest"`
	ExecutableDigest string            `json:"executable_digest"`
	RuntimeIdentity  string            `json:"runtime_identity"`
	SourceIdentity   string            `json:"source_identity"` // include dirty-state evidence/digest
	ResetPolicy      string            `json:"reset_policy"`
	Policy           string            `json:"policy"` // e.g. delivery-before-time/v1
	Seed             int64             `json:"seed"`
	Environment      map[string]string `json:"environment"`
	MaxBytes         int64             `json:"max_bytes"`
	MaxRecordBytes   int               `json:"max_record_bytes"`
	MaxDecisions     int               `json:"max_decisions"`
}

func (m Manifest) Validate() error {
	if m.Version != SchemaVersion || m.Workload == "" || m.InputDigest == "" || m.ExecutableDigest == "" || m.RuntimeIdentity == "" || m.SourceIdentity == "" || m.ResetPolicy == "" || m.Policy == "" || len(m.Environment) == 0 || m.MaxBytes <= 0 || m.MaxRecordBytes <= 0 || m.MaxDecisions <= 0 {
		return fmt.Errorf("incomplete/unsupported campaign manifest")
	}
	if m.Task != nil && (m.Task.ManifestDigest == "" || m.Task.TaskID == "" || m.Task.CommandDigest == "" || m.Task.InputDigest == "" || m.Task.InputDigest != m.InputDigest || m.Task.Seed != m.Seed) {
		return fmt.Errorf("incomplete/inconsistent task binding")
	}
	if m.MaxRecordBytes > 4<<20 || m.MaxBytes > 64<<20 {
		return fmt.Errorf("artifact limits exceed supported reader bounds")
	}
	return nil
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

type durableWriter interface {
	io.Writer
	Sync() error
	Close() error
}

// Store retains its first persistence failure. There is no silent disable path.
type Store struct {
	mu        sync.Mutex
	writer    durableWriter
	manifest  string
	sequence  uint64
	bytes     int64
	maxBytes  int64
	maxRecord int
	err       error
	closed    bool
	active    int
	lastTrial int
}

type Record struct {
	Version  int             `json:"version"`
	Sequence uint64          `json:"sequence"`
	Manifest string          `json:"manifest"`
	Kind     string          `json:"kind"`
	Trial    int             `json:"trial"`
	Payload  json.RawMessage `json:"payload"`
	Checksum string          `json:"checksum"`
}

func Create(dir string, m Manifest) (*Store, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	} // exclusive; never overwrite
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	b = append(b, '\n')
	if len(b) > 1<<20 {
		return nil, fmt.Errorf("manifest too large")
	}
	if err = writeExclusive(filepath.Join(dir, "manifest.json"), b); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	if err = syncDir(dir); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if err = syncDir(filepath.Dir(dir)); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &Store{writer: f, manifest: digest(b), maxBytes: m.MaxBytes, maxRecord: m.MaxRecordBytes}, nil
}
func writeExclusive(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, err := f.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}
func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (s *Store) Append(kind string, trial int, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.closed {
		s.err = fmt.Errorf("artifact store closed")
		return s.err
	}
	if kind != "start" && kind != "checkpoint" && kind != "terminal" {
		s.err = fmt.Errorf("invalid record kind %q", kind)
		return s.err
	}
	if trial <= 0 || (kind == "start" && (s.active != 0 || trial != s.lastTrial+1)) || (kind != "start" && (s.active == 0 || trial != s.active)) {
		s.err = fmt.Errorf("invalid trial record order")
		return s.err
	}
	p, err := json.Marshal(payload)
	if err != nil {
		s.err = err
		return err
	}
	r := Record{Version: SchemaVersion, Sequence: s.sequence + 1, Manifest: s.manifest, Kind: kind, Trial: trial, Payload: p, Checksum: digest(p)}
	b, err := json.Marshal(r)
	if err == nil {
		b = append(b, '\n')
		if len(b) > s.maxRecord || s.bytes+int64(len(b)) > s.maxBytes {
			err = fmt.Errorf("artifact byte limit exceeded")
		}
	}
	if err == nil {
		var n int
		n, err = s.writer.Write(b)
		if err == nil && n != len(b) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = s.writer.Sync()
	}
	if err != nil {
		s.err = err
		return err
	}
	s.sequence++
	if kind == "start" {
		s.active = trial
		s.lastTrial = trial
	}
	if kind == "terminal" {
		s.active = 0
	}
	s.bytes += int64(len(b))
	return nil
}
func (s *Store) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.err
	}
	s.closed = true
	if s.active != 0 && s.err == nil {
		s.err = fmt.Errorf("artifact closed with incomplete trial %d", s.active)
	}
	err := errors.Join(s.writer.Sync(), s.writer.Close())
	if s.err == nil {
		s.err = err
	}
	return s.err
}
