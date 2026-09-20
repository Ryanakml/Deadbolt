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

func processGroupID(pid int) int {
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		return pid
	}
	return pgid
}

// sendProcessGroupSignal uses a captured pgid so a surviving child is still
// observed and killed after its runner parent has exited.
func sendProcessGroupSignal(pgid int, force bool) error {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	return syscall.Kill(-pgid, sig)
}

func isProcessGroupAlive(pgid int) bool {
	// EPERM from signal 0 means the group exists but is not signalable by
	// us: that is still alive. Only ESRCH (no such process) means gone.
	// Treating EPERM as dead would report a hung group stopped without
	// ever escalating to SIGKILL.
	err := syscall.Kill(-pgid, syscall.Signal(0))
	return err == nil || err == syscall.EPERM
}

func checkProcessAliveOS(proc *os.Process, pid int) bool {
	// In POSIX, Signal(0) tests whether process exists and is alive
	err := proc.Signal(syscall.Signal(0))
	return err == nil
}
