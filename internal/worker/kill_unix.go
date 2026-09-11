//go:build !windows

package worker

import (
	"os"
	"os/exec"
	"syscall"
)

func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
}

func terminateProcessGroup(pid int, sig syscall.Signal) error {
	// Negative pid sends signal to entire process group in POSIX
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		pgid = pid
	}
	return syscall.Kill(-pgid, sig)
}

func sendSigterm(pid int) error {
	return terminateProcessGroup(pid, syscall.SIGTERM)
}

func sendSigkill(pid int) error {
	return terminateProcessGroup(pid, syscall.SIGKILL)
}

func checkProcessAliveOS(proc *os.Process, pid int) bool {
	// In POSIX, Signal(0) tests whether process exists and is alive
	err := proc.Signal(syscall.Signal(0))
	return err == nil
}
