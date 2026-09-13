//go:build windows

package worker

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func configureProcessGroup(cmd *exec.Cmd) {
	// Windows manages process hierarchies without Setpgid
}

func processGroupID(pid int) int { return pid }

func sendProcessGroupSignal(pid int, force bool) error {
	if force {
		cmd := exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid))
		return cmd.Run()
	}
	// On Windows, initiate graceful termination via taskkill without /F first
	cmd := exec.Command("taskkill", "/PID", strconv.Itoa(pid))
	_ = cmd.Run()

	// Also signal process if accessible
	proc, err := os.FindProcess(pid)
	if err == nil {
		_ = proc.Signal(os.Interrupt)
	}
	return nil
}

func isProcessGroupAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	return err == nil && checkProcessAliveOS(proc, pid)
}

func checkProcessAliveOS(proc *os.Process, pid int) bool {
	cmd := exec.Command("cmd", "/c", fmt.Sprintf("tasklist /FI \"PID eq %d\" /NH", pid))
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), strconv.Itoa(pid))
}
