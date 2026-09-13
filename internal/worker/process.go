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
	ErrStartAckRejected       = errors.New("START_ACK_REJECTED: Control plane rejected Start request; handler aborted")
	ErrStartAckAmbiguous      = errors.New("START_ACK_AMBIGUOUS: Start decision was not recovered before lease boundary")
	ErrExecutionTimedOut      = errors.New("EXECUTION_TIMED_OUT: Task exceeded allowed execution deadline")
	ErrProcessTerminated      = errors.New("PROCESS_TERMINATED: Process was stopped before completion")
	ErrBundleVerificationNeed = errors.New("BUNDLE_VERIFICATION_REQUIRED: customer code cannot run without verified bundle identity")
)

type ExecutionLogs struct{ Stdout, Stderr string }

// ProcessSupervisor is deliberately a bounded lifecycle harness, not a worker
// service. Gateway callbacks stand in for the future authenticated transport.
type ProcessSupervisor struct {
	NodePath, RunnerPath            string
	GracePeriod, LeaseCheckInterval time.Duration
	LeaseTracker                    *LeaseTracker
	StartAckFn                      StartAckFunc // compatibility adapter: any error is authoritative rejection
	StartFn                         StartDecisionFunc
	RenewLeaseFn                    RenewLeaseFunc
	OnProcessStart                  func(pid int) // test-only observation hook
	AllowlistKeys, TaskEnvAllowlist []string
}

func NewProcessSupervisor(nodePath, runnerPath string) *ProcessSupervisor {
	if nodePath == "" {
		nodePath = "node"
	}
	return &ProcessSupervisor{
		NodePath: nodePath, RunnerPath: runnerPath, GracePeriod: 10 * time.Second,
		LeaseCheckInterval: 100 * time.Millisecond, AllowlistKeys: []string{"NODE_ENV", "DEADBOLT_ENV"},
	}
}

func (s *ProcessSupervisor) TerminateProcessGroup(pid int) *StopResult {
	started := time.Now()
	res := &StopResult{PID: pid}
	_ = sendSigterm(pid)
	deadline := time.NewTimer(s.GracePeriod)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline.C:
			res.GraceExceeded = true
			_ = sendSigkill(pid)
			res.Stopped = true
			res.Duration = time.Since(started)
			return res
		case <-tick.C:
			if !isProcessAlive(pid) {
				res.Stopped = true
				res.Duration = time.Since(started)
				return res
			}
		}
	}
}

func (s *ProcessSupervisor) verifyStart(ctx context.Context, attemptID string, epoch int64) error {
	if s.StartFn == nil {
		if s.StartAckFn == nil {
			return nil
		}
		if err := s.StartAckFn(ctx, attemptID, epoch); err != nil {
			return fmt.Errorf("%w: %v", ErrStartAckRejected, err)
		}
		return nil
	}
	// At most one retry: it is the identical Start request/identity and reads the
	// durable gateway decision; it does not create a second start.
	for tries := 0; tries < 2; tries++ {
		decision, err := s.StartFn(ctx, attemptID, epoch)
		if err != nil || decision == StartAmbiguous {
			if tries == 0 {
				continue
			}
			return ErrStartAckAmbiguous
		}
		if decision != StartAccepted {
			return ErrStartAckRejected
		}
		return nil
	}
	return ErrStartAckAmbiguous
}

func (s *ProcessSupervisor) verifyBundle(input *TaskInput) error {
	if input.Bundle == nil {
		return ErrBundleVerificationNeed
	}
	if err := VerifyArchitecture(input.Bundle.TargetArch, ""); err != nil {
		return err
	}
	file, err := os.Open(input.Bundle.Path)
	if err != nil {
		return fmt.Errorf("open bundle: %w", err)
	}
	defer file.Close()
	_, err = VerifyBundleDigest(file, input.Bundle.SHA256)
	return err
}

func (s *ProcessSupervisor) ExecuteAttempt(ctx context.Context, input *TaskInput, epoch int64) (*TaskCompletion, *ExecutionLogs, error) {
	if s.LeaseTracker != nil {
		if ok, err := s.LeaseTracker.CanStart(time.Time{}); !ok || err != nil {
			return nil, nil, ErrInsufficientLeaseTTL
		}
	}
	if err := s.verifyStart(ctx, input.AttemptID, epoch); err != nil {
		return nil, nil, err
	}
	// Start ACK can consume the entire budget; never launch based on the old check.
	if s.LeaseTracker != nil {
		if ok, err := s.LeaseTracker.CanStart(time.Time{}); !ok || err != nil {
			return nil, nil, ErrInsufficientLeaseTTL
		}
	}
	if err := s.verifyBundle(input); err != nil {
		return nil, nil, err
	}
	if err := ValidateTaskEnvironment(input.Env, s.TaskEnvAllowlist); err != nil {
		return nil, nil, err
	}

	if input.TimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(input.TimeoutMs)*time.Millisecond)
		defer cancel()
	}
	resultFile := filepath.Join(os.TempDir(), fmt.Sprintf("deadbolt_res_%s_%d.json", input.AttemptID, time.Now().UnixNano()))
	defer os.Remove(resultFile)
	cmd := exec.Command(s.NodePath, s.RunnerPath)
	configureProcessGroup(cmd)
	taskEnv := make(map[string]string, len(input.Env)+1)
	for k, v := range input.Env {
		taskEnv[k] = v
	}
	taskEnv["DEADBOLT_RESULT_FILE"] = resultFile
	cmd.Env = SanitizeEnvironment(os.Environ(), taskEnv, append(s.AllowlistKeys, "DEADBOLT_RESULT_FILE"))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("stdin pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("start runner: %w", err)
	}
	pid := cmd.Process.Pid
	if s.OnProcessStart != nil {
		s.OnProcessStart(pid)
	}
	inputBytes, err := json.Marshal(input)
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, nil, fmt.Errorf("encode input: %w", err)
	}
	if _, err := stdin.Write(inputBytes); err != nil {
		_ = cmd.Process.Kill()
		return nil, nil, fmt.Errorf("write input: %w", err)
	}
	_ = stdin.Close()
	exit := make(chan error, 1)
	go func() { exit <- cmd.Wait() }()
	interval := s.LeaseCheckInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	var finalErr error
	for {
		select {
		case finalErr = <-exit:
			goto done
		case <-ctx.Done():
			_ = s.TerminateProcessGroup(pid)
			<-exit
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && input.TimeoutMs > 0 {
				finalErr = ErrExecutionTimedOut
			} else {
				finalErr = ErrProcessTerminated
			}
			goto done
		case <-tick.C:
			if s.LeaseTracker == nil {
				continue
			}
			if s.RenewLeaseFn != nil {
				renewal, err := s.RenewLeaseFn(ctx, input.AttemptID, epoch)
				if err == nil {
					s.LeaseTracker.Renew(renewal.ExpiresAt, renewal.RTT)
				}
			}
			if s.LeaseTracker.IsExpired(time.Time{}) {
				_ = s.TerminateProcessGroup(pid)
				<-exit
				finalErr = ErrLeaseExpired
				goto done
			}
		}
	}
done:
	logs := &ExecutionLogs{Stdout: stdout.String(), Stderr: stderr.String()}
	raw, readErr := os.ReadFile(resultFile)
	if readErr != nil {
		return &TaskCompletion{AttemptID: input.AttemptID, Status: "FAILED", Error: &TaskError{Code: "NO_RESULT_DELIVERED", Message: fmt.Sprintf("Process exited without writing to result channel: %v", finalErr)}, Metrics: TaskMetrics{}}, logs, finalErr
	}
	var completion TaskCompletion
	if err := json.Unmarshal(raw, &completion); err != nil {
		return &TaskCompletion{AttemptID: input.AttemptID, Status: "FAILED", Error: &TaskError{Code: "MALFORMED_RESULT_PAYLOAD", Message: err.Error()}, Metrics: TaskMetrics{}}, logs, err
	}
	return &completion, logs, finalErr
}

func isProcessAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	return err == nil && checkProcessAliveOS(proc, pid)
}
