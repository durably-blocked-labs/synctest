package campaign

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode/utf8"
)

type Inspection struct {
	Records        []Record
	VerifiedOffset int64
	Incomplete     bool
}

// Inspect validates an immutable manifest and a bounded record stream. Salvage
// accepts only a non-newline-terminated final record, never malformed middle data.
func Inspect(dir string, salvageTail bool) (Inspection, error) {
	b, err := readManifestBytes(dir)
	if err != nil {
		return Inspection{}, err
	}
	var m Manifest
	if err = strictJSON(b, &m); err != nil {
		return Inspection{}, err
	}
	if err = m.Validate(); err != nil {
		return Inspection{}, err
	}
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		return Inspection{}, err
	}
	defer f.Close()
	return ReadRecords(f, digest(b), m.MaxRecordBytes, m.MaxBytes, salvageTail)
}
func readManifestBytes(dir string) ([]byte, error) {
	f, err := os.Open(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(b) > 1<<20 {
		return nil, fmt.Errorf("manifest too large")
	}
	return b, nil
}

// CheckCompatibility rejects any changed effective input, binary, environment,
// policy or limit before a worker is allowed to replay.
func CheckCompatibility(dir string, expected Manifest) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	b, err := readManifestBytes(dir)
	if err != nil {
		return err
	}
	var actual Manifest
	if err = strictJSON(b, &actual); err != nil {
		return err
	}
	if err = actual.Validate(); err != nil {
		return err
	}
	want, _ := json.Marshal(expected)
	got, _ := json.Marshal(actual)
	if !bytes.Equal(want, got) {
		return fmt.Errorf("incompatible replay manifest")
	}
	return nil
}

func strictJSON(b []byte, v any) error {
	if !utf8.Valid(b) {
		return fmt.Errorf("invalid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("extra JSON data")
	}
	return nil
}
func ReadRecords(input io.Reader, manifest string, maxRecord int, maxBytes int64, salvageTail bool) (Inspection, error) {
	var out Inspection
	if maxRecord <= 0 || maxBytes <= 0 {
		return out, fmt.Errorf("invalid reader limits")
	}
	r := bufio.NewReaderSize(input, maxRecord+1)
	active := 0
	lastTrial := 0
	for {
		b, err := r.ReadSlice('\n')
		if len(b) > maxRecord || err == bufio.ErrBufferFull {
			return out, fmt.Errorf("record length limit exceeded at %d", out.VerifiedOffset)
		}
		if out.VerifiedOffset+int64(len(b)) > maxBytes {
			return out, fmt.Errorf("artifact byte limit exceeded")
		}
		if err == io.EOF {
			if len(b) > 0 {
				if !salvageTail {
					return out, fmt.Errorf("truncated tail at %d", out.VerifiedOffset)
				}
				out.Incomplete = true
			}
			if active != 0 {
				out.Incomplete = true
			}
			return out, nil
		}
		if err != nil {
			return out, err
		}
		var rec Record
		if err = strictJSON(b, &rec); err != nil {
			return out, fmt.Errorf("corruption at %d: %w", out.VerifiedOffset, err)
		}
		if rec.Version != SchemaVersion || rec.Sequence != uint64(len(out.Records)+1) || rec.Manifest != manifest || rec.Checksum != digest(rec.Payload) {
			return out, fmt.Errorf("record identity/checksum mismatch at %d", out.VerifiedOffset)
		}
		switch rec.Kind {
		case "start":
			if active != 0 || rec.Trial != lastTrial+1 {
				return out, fmt.Errorf("out-of-order trial start")
			}
			active = rec.Trial
			lastTrial = rec.Trial
		case "checkpoint":
			if active == 0 || rec.Trial != active {
				return out, fmt.Errorf("checkpoint without active trial")
			}
		case "terminal":
			if active == 0 || rec.Trial != active {
				return out, fmt.Errorf("terminal without active trial")
			}
			active = 0
		default:
			return out, fmt.Errorf("unknown record kind")
		}
		out.Records = append(out.Records, rec)
		out.VerifiedOffset += int64(len(b))
	}
}
