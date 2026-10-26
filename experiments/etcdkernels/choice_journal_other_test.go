//go:build !darwin && !linux

package etcdkernels

import "errors"

type choiceJournal struct{}

func openChoiceJournal(string) (*choiceJournal, error) {
	return nil, errors.New("kernel worker requires a shared decision journal on Linux or macOS")
}

func (*choiceJournal) append(kernelChoice) error { return errors.New("unsupported platform") }
func (*choiceJournal) close()                    {}
