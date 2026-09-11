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

func sendSigterm(pid int) error {
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

func sendSigkill(pid int) error {
	// On Windows, taskkill /F /T kills the process and all child/grandchild processes tree
	cmd := exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid))
	if err := cmd.Run(); err != nil {
		// Fallback to direct process kill
		if proc, pErr := os.FindProcess(pid); pErr == nil {
			return proc.Kill()
		}
		return fmt.Errorf("taskkill failed: %w", err)
	}
	return nil
}

func checkProcessAliveOS(proc *os.Process, pid int) bool {
	cmd := exec.Command("cmd", "/c", fmt.Sprintf("tasklist /FI \"PID eq %d\" /NH", pid))
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), strconv.Itoa(pid))
}
