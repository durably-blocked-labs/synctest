package framework

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type JSONLWriter struct {
	mu   sync.Mutex
	file *os.File
}

func OpenJSONLWriter(path string) (*JSONLWriter, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open JSONL file: %w", err)
	}
	return &JSONLWriter{file: file}, nil
}

func (w *JSONLWriter) Append(record any) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode JSONL record: %w", err)
	}
	data = append(data, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("append JSONL record: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync JSONL file: %w", err)
	}
	return nil
}

func (w *JSONLWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func WriteJSONAtomic(path string, value any) error {
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary JSON file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		temp.Close()
		return fmt.Errorf("encode JSON file: %w", err)
	}
	if err := temp.Chmod(0o644); err != nil {
		temp.Close()
		return fmt.Errorf("set JSON file permissions: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync temporary JSON file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary JSON file: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace JSON file: %w", err)
	}

	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open JSON directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync JSON directory: %w", err)
	}
	return nil
}
