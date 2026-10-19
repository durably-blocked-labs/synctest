package campaign

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type brokenWriter struct {
	writes                          int
	short                           bool
	writeErr, errorSync, errorClose error
}

func (w *brokenWriter) Write(b []byte) (int, error) {
	w.writes++
	if w.short {
		return len(b) - 1, w.writeErr
	}
	return len(b), w.writeErr
}
func (w *brokenWriter) Sync() error  { return w.errorSync }
func (w *brokenWriter) Close() error { return w.errorClose }
func testManifest() Manifest {
	return Manifest{Version: 1, Workload: "tiny", InputDigest: "input", ExecutableDigest: "exe", RuntimeIdentity: "runtime", SourceIdentity: "dirty-digest", ResetPolicy: "fresh-process", Policy: "delivery-before-time/v1", Environment: map[string]string{"GODEBUG": "asyncpreemptoff=1"}, MaxBytes: 8192, MaxRecordBytes: 2048, MaxDecisions: 32}
}

func TestStoreStickyFailures(t *testing.T) {
	cause := errors.New("disk failed")
	for _, tc := range []struct {
		name string
		w    brokenWriter
		want error
	}{{"write", brokenWriter{writeErr: cause}, cause}, {"partial", brokenWriter{short: true, writeErr: cause}, cause}, {"short", brokenWriter{short: true}, io.ErrShortWrite}, {"sync", brokenWriter{errorSync: cause}, cause}} {
		t.Run(tc.name, func(t *testing.T) {
			w := tc.w
			s := Store{writer: &w, maxBytes: 4096, maxRecord: 2048}
			if err := s.Append("start", 1, nil); !errors.Is(err, tc.want) {
				t.Fatalf("error %v", err)
			}
			if err := s.Append("terminal", 1, nil); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
			if w.writes != 1 {
				t.Fatalf("writes %d", w.writes)
			}
			if err := s.Close(); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
	s := Store{writer: &brokenWriter{errorClose: cause}}
	if !errors.Is(s.Close(), cause) || !errors.Is(s.Err(), cause) {
		t.Fatal("close error lost")
	}
}
func TestStoreExclusiveAndInspect(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	s, err := Create(dir, testManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append("start", 1, map[string]int{"version": 1}); err != nil {
		t.Fatal(err)
	}
	if err = s.Append("terminal", 1, map[string]string{"outcome": "passed"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if _, err = Create(dir, testManifest()); err == nil {
		t.Fatal("collision accepted")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("collision overwrote manifest")
	}
	if err = CheckCompatibility(dir, testManifest()); err != nil {
		t.Fatal(err)
	}
	changed := testManifest()
	changed.ExecutableDigest = "different"
	if err = CheckCompatibility(dir, changed); err == nil {
		t.Fatal("changed executable accepted")
	}
	got, err := Inspect(dir, false)
	if err != nil || len(got.Records) != 2 || got.Incomplete {
		t.Fatalf("%+v %v", got, err)
	}
	events, _ := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	manifest := digest(before)
	truncated := events[:len(events)-3]
	prefix, err := ReadRecords(bytes.NewReader(truncated), manifest, 2048, 8192, true)
	if err != nil || len(prefix.Records) != 1 || !prefix.Incomplete {
		t.Fatalf("salvage %+v %v", prefix, err)
	}
	if _, err = ReadRecords(bytes.NewReader(truncated), manifest, 2048, 8192, false); err == nil {
		t.Fatal("truncation accepted")
	}
	corrupt := bytes.Replace(events, []byte(`"version":1`), []byte(`"version":9`), 1)
	if _, err = ReadRecords(bytes.NewReader(corrupt), manifest, 2048, 8192, true); err == nil {
		t.Fatal("middle corruption accepted")
	}
	checksum := bytes.Replace(events, []byte(`"outcome":"passed"`), []byte(`"outcome":"failed"`), 1)
	if _, err = ReadRecords(bytes.NewReader(checksum), manifest, 2048, 8192, false); err == nil {
		t.Fatal("checksum corruption accepted")
	}
	if _, err = ReadRecords(bytes.NewReader(events), manifest, 10, 8192, false); err == nil {
		t.Fatal("record limit ignored")
	}
	if _, err = ReadRecords(bytes.NewReader([]byte{0xff, '\n'}), manifest, 2048, 8192, true); err == nil {
		t.Fatal("invalid UTF8 accepted")
	}
}
func TestStoreIncompleteClose(t *testing.T) {
	w := &brokenWriter{}
	s := Store{writer: w, maxBytes: 4096, maxRecord: 2048}
	if err := s.Append("start", 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err == nil {
		t.Fatal("incomplete trial close accepted")
	}
}

func TestStoreBudgetAndManifest(t *testing.T) {
	m := testManifest()
	m.RuntimeIdentity = ""
	if _, err := Create(filepath.Join(t.TempDir(), "bad"), m); err == nil {
		t.Fatal("missing provenance accepted")
	}
	w := &brokenWriter{}
	s := Store{writer: w, maxBytes: 1, maxRecord: 1024}
	if err := s.Append("start", 1, nil); err == nil || w.writes != 0 {
		t.Fatal("byte limit failed")
	}
}
