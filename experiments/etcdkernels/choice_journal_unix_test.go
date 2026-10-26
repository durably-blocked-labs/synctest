//go:build darwin || linux

package etcdkernels

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

const (
	choiceSlots = 1024
	choiceSize  = 72 // selected index, runnable count, and 16 uint32 goroutine IDs
	headerSize  = 8
)

// choiceJournal is a pre-faulted shared mapping. The scheduling hook only
// writes memory; it must not perform file I/O or a syscall while choosing an
// actor, because an external wait there could let application peers run.
type choiceJournal struct {
	file *os.File
	data []byte
}

func openChoiceJournal(path string) (*choiceJournal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	size := headerSize + choiceSlots*choiceSize
	if err := file.Truncate(int64(size)); err != nil {
		file.Close()
		return nil, err
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}
	copy(data, "BKC1")
	for page := 4096; page < len(data); page += 4096 {
		data[page] = 0
	}
	return &choiceJournal{file: file, data: data}, nil
}

func (j *choiceJournal) append(choice kernelChoice) error {
	step := binary.LittleEndian.Uint32(j.data[4:8])
	if step >= choiceSlots || len(choice.BGIDs) == 0 || len(choice.BGIDs) > 16 {
		return fmt.Errorf("decision journal capacity or runnable limit exceeded")
	}
	offset := headerSize + int(step)*choiceSize
	binary.LittleEndian.PutUint32(j.data[offset:offset+4], uint32(choice.Chosen))
	binary.LittleEndian.PutUint32(j.data[offset+4:offset+8], uint32(len(choice.BGIDs)))
	for i, id := range choice.BGIDs {
		start := offset + 8 + i*4
		binary.LittleEndian.PutUint32(j.data[start:start+4], id)
	}
	binary.LittleEndian.PutUint32(j.data[4:8], step+1)
	return nil
}

func (j *choiceJournal) close() {
	syscall.Munmap(j.data)
	j.file.Close()
}
