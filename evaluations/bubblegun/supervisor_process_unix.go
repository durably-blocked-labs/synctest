//go:build unix

package bubblegun

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func configureWorkerProcess(command *exec.Cmd) error {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

func signalWorkerProcess(command *exec.Cmd, signal os.Signal) error {
	if command.Process == nil {
		return nil
	}
	sig, ok := signal.(syscall.Signal)
	if !ok {
		return errors.New("unsupported worker process-group signal")
	}
	// The worker is the leader of its own group. Descendants that deliberately
	// create new sessions/groups are outside this containment boundary.
	err := syscall.Kill(-command.Process.Pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func workerAborted(err error) bool {
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return false
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	return exit.ExitCode() == 124 || (ok && status.Signaled())
}
