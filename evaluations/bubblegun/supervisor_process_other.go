//go:build !unix

package bubblegun

import (
	"errors"
	"os"
	"os/exec"
)

func configureWorkerProcess(*exec.Cmd) error {
	return errors.New("BubbleGun worker process-group containment requires Unix")
}

func signalWorkerProcess(*exec.Cmd, os.Signal) error {
	return errors.New("BubbleGun worker process-group containment requires Unix")
}

func workerAborted(error) bool { return true }
