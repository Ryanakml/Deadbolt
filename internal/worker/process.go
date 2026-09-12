package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

var (
	ErrStartAckRejected  = errors.New("START_ACK_REJECTED: Control plane rejected Start request; handler aborted")
	ErrExecutionTimedOut = errors.New("EXECUTION_TIMED_OUT: Task exceeded allowed execution deadline")
	ErrProcessTerminated = errors.New("PROCESS_TERMINATED: Process was stopped before completion")
)

// ExecutionLogs captures stdout and stderr emitted by the child process.
type ExecutionLogs struct {
	Stdout string
	Stderr string
}

// ProcessSupervisor coordinates the Node.js runner child process per Blueprint §12.3.
type ProcessSupervisor struct {
	NodePath      string
	RunnerPath    string
	GracePeriod   time.Duration
	LeaseTracker  *LeaseTracker
	StartAckFn    StartAckFunc
	AllowlistKeys []string
}

// NewProcessSupervisor creates a configured supervisor.
func NewProcessSupervisor(nodePath, runnerPath string) *ProcessSupervisor {
	if nodePath == "" {
		nodePath = "node"
	}
	return &ProcessSupervisor{
		NodePath:      nodePath,
		RunnerPath:    runnerPath,
		GracePeriod:   10 * time.Second,
		AllowlistKeys: []string{"NODE_ENV", "DEADBOLT_ENV"},
	}
}

// TerminateProcessGroup terminates the target process and its child tree
// using SIGTERM followed by SIGKILL after the 10-second grace period.
func (s *ProcessSupervisor) TerminateProcessGroup(pid int) *StopResult {
	startTime := time.Now()
	res := &StopResult{
		PID:      pid,
		Duration: 0,
	}

	// 1. Send SIGTERM / initial abort signal
	_ = sendSigterm(pid)

	// 2. Poll for termination within grace period
	graceTimer := time.NewTimer(s.GracePeriod)
	defer graceTimer.Stop()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-graceTimer.C:
			// Grace period expired without clean exit; send SIGKILL
			res.GraceExceeded = true
			_ = sendSigkill(pid)
			res.Stopped = true
			res.Duration = time.Since(startTime)
			return res

		case <-ticker.C:
			// Check if process has already exited
			if !isProcessAlive(pid) {
				res.Stopped = true
				res.Duration = time.Since(startTime)
				return res
			}
		}
	}
}

// ExecuteAttempt runs the task attempt lifecycle with Start ACK gating,
// lease monitoring, environment allowlisting, and channel isolation.
func (s *ProcessSupervisor) ExecuteAttempt(
	ctx context.Context,
	input *TaskInput,
	epoch int64,
) (*TaskCompletion, *ExecutionLogs, error) {
	// Rule 1: Lease verification before Start
	if s.LeaseTracker != nil {
		canStart, err := s.LeaseTracker.CanStart(time.Now())
		if !canStart || err != nil {
			return nil, nil, ErrInsufficientLeaseTTL
		}
	}

	// Rule 2: Start ACK gating - Handler NEVER starts without verified ACK from control plane
	if s.StartAckFn != nil {
		if err := s.StartAckFn(ctx, input.AttemptID, epoch); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrStartAckRejected, err)
		}
	}

	// Rule 3: Structured result channel (dedicated file/pipe isolation)
	tempDir := os.TempDir()
	resultFile := filepath.Join(tempDir, fmt.Sprintf("deadbolt_res_%s_%d.json", input.AttemptID, time.Now().UnixNano()))
	defer os.Remove(resultFile)

	// Prepare Node runner command
	args := []string{}
	if s.RunnerPath != "" {
		args = append(args, s.RunnerPath)
	}
	cmd := exec.Command(s.NodePath, args...)

	// Configure OS process group
	configureProcessGroup(cmd)

	// Rule 4: Child environment allowlisting
	taskEnv := input.Env
	if taskEnv == nil {
		taskEnv = make(map[string]string)
	}
	taskEnv["DEADBOLT_RESULT_FILE"] = resultFile
	cmd.Env = SanitizeEnvironment(os.Environ(), taskEnv, s.AllowlistKeys)

	// Separate stdout/stderr log buffers - Rule: arbitrary stdout never becomes result
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	// Stdin pipe for input payload
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open stdin pipe: %w", err)
	}

	// Start runner process
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("failed to start runner process: %w", err)
	}

	pid := cmd.Process.Pid

	// Write input to stdin and close pipe
	inputBytes, err := json.Marshal(input)
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, nil, fmt.Errorf("failed to encode task input: %w", err)
	}
	if _, err := stdinPipe.Write(inputBytes); err != nil {
		_ = cmd.Process.Kill()
		return nil, nil, fmt.Errorf("failed to write input to stdin: %w", err)
	}
	_ = stdinPipe.Close()

	// Wait channel for process exit
	type exitResult struct {
		err error
	}
	exitChan := make(chan exitResult, 1)
	go func() {
		err := cmd.Wait()
		exitChan <- exitResult{err: err}
	}()

	var finalErr error

	// Wait for completion, abort, or context expiration
	select {
	case res := <-exitChan:
		finalErr = res.err

	case <-ctx.Done():
		// Abort triggered: initiate graceful shutdown -> 10s grace -> SIGKILL
		_ = s.TerminateProcessGroup(pid)
		<-exitChan // wait for exit
		finalErr = ErrProcessTerminated
	}

	logs := &ExecutionLogs{
		Stdout: stdoutBuf.String(),
		Stderr: stderrBuf.String(),
	}

	// Read result strictly from structured channel (result file)
	rawResult, readErr := os.ReadFile(resultFile)
	if readErr != nil {
		// If result file does not exist, return failure envelope
		completion := &TaskCompletion{
			AttemptID: input.AttemptID,
			Status:    "FAILED",
			Error: &TaskError{
				Code:      "NO_RESULT_DELIVERED",
				Message:   fmt.Sprintf("Process exited without writing to result channel: %v", finalErr),
				Retryable: false,
			},
			Metrics: TaskMetrics{DurationMs: 0},
		}
		return completion, logs, finalErr
	}

	var completion TaskCompletion
	if err := json.Unmarshal(rawResult, &completion); err != nil {
		completion = TaskCompletion{
			AttemptID: input.AttemptID,
			Status:    "FAILED",
			Error: &TaskError{
				Code:      "MALFORMED_RESULT_PAYLOAD",
				Message:   fmt.Sprintf("Structured result payload was not valid JSON: %v", err),
				Retryable: false,
			},
			Metrics: TaskMetrics{DurationMs: 0},
		}
		return &completion, logs, err
	}

	return &completion, logs, finalErr
}

func isProcessAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Windows proc.FindProcess always succeeds.
	// We can check with os.FindProcess or Signal(0) on Unix.
	return checkProcessAliveOS(proc, pid)
}
