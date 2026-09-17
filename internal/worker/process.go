package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	ErrStartAckRejected       = errors.New("START_ACK_REJECTED: Control plane rejected Start request; handler aborted")
	ErrStartAckAmbiguous      = errors.New("START_ACK_AMBIGUOUS: Start decision was not recovered before lease boundary")
	ErrExecutionTimedOut      = errors.New("EXECUTION_TIMED_OUT: Task exceeded allowed execution deadline")
	ErrProcessTerminated      = errors.New("PROCESS_TERMINATED: Process was stopped before completion")
	ErrBundleVerificationNeed = errors.New("BUNDLE_VERIFICATION_REQUIRED: customer code cannot run without verified bundle identity")
	ErrStartAuthorityMissing  = errors.New("START_AUTHORITY_REQUIRED: customer code cannot run without a Start verifier")
	ErrLeaseAuthorityMissing  = errors.New("LEASE_AUTHORITY_REQUIRED: customer code cannot run without a lease tracker")
	ErrBundleEntrypoint       = errors.New("BUNDLE_ENTRYPOINT_MISMATCH: verified artifact is not the requested entrypoint")
	ErrRunnerInputTooLarge    = errors.New("RUNNER_INPUT_TOO_LARGE: serialized runner input exceeds the configured limit")
)

type ExecutionLogs struct{ Stdout, Stderr string }

// MaxSerializedRunnerInputBytes bounds the JSON payload sent over a runner's
// stdin. It applies before the child process exists, so an oversized payload
// cannot consume a runner slot or block on a pipe.
const MaxSerializedRunnerInputBytes = 1 << 20

// ProcessSupervisor is deliberately a bounded lifecycle harness, not a worker
// service. Gateway callbacks stand in for the future authenticated transport.
type ProcessSupervisor struct {
	NodePath, RunnerPath            string
	GracePeriod, LeaseCheckInterval time.Duration
	LeaseTracker                    *LeaseTracker
	StartAckFn                      StartAckFunc // compatibility adapter: any error is authoritative rejection
	StartFn                         StartDecisionFunc
	StartAuthorization              *StartAuthorization
	RenewLeaseFn                    RenewLeaseFunc
	OnProcessStart                  func(pid int) // test-only observation hook
	ResultDir                       string        // optional test-owned directory for result cleanup assertions
	AllowlistKeys, TaskEnvAllowlist []string
	MaxRunnerInputBytes             int
}

func NewProcessSupervisor(nodePath, runnerPath string) *ProcessSupervisor {
	if nodePath == "" {
		nodePath = "node"
	}
	return &ProcessSupervisor{
		NodePath: nodePath, RunnerPath: runnerPath, GracePeriod: 10 * time.Second,
		LeaseCheckInterval: 100 * time.Millisecond, AllowlistKeys: []string{"NODE_ENV", "DEADBOLT_ENV"},
		MaxRunnerInputBytes: MaxSerializedRunnerInputBytes,
	}
}

func (s *ProcessSupervisor) TerminateProcessGroup(pid int) *StopResult {
	started := time.Now()
	res := &StopResult{PID: pid}
	pgid := processGroupID(pid)
	_ = sendProcessGroupSignal(pgid, false)
	deadline := time.NewTimer(s.GracePeriod)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline.C:
			res.GraceExceeded = true
			_ = sendProcessGroupSignal(pgid, true)
			goneBy := time.NewTimer(time.Second)
			for isProcessGroupAlive(pgid) {
				select {
				case <-goneBy.C:
					res.Duration = time.Since(started)
					return res
				default:
					time.Sleep(10 * time.Millisecond)
				}
			}
			goneBy.Stop()
			res.Stopped = true
			res.Duration = time.Since(started)
			return res
		case <-tick.C:
			if !isProcessGroupAlive(pgid) {
				res.Stopped = true
				res.Duration = time.Since(started)
				return res
			}
		}
	}
}

func (s *ProcessSupervisor) verifyStart(ctx context.Context, attemptID string, epoch int64) error {
	if authorization := s.StartAuthorization; authorization != nil {
		if authorization.AttemptID != attemptID || authorization.Epoch != epoch || authorization.Decision != StartAccepted {
			return ErrStartAckRejected
		}
		return nil
	}
	if s.StartFn == nil {
		if s.StartAckFn == nil {
			return ErrStartAuthorityMissing
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

func (s *ProcessSupervisor) verifyBundle(input *TaskInput) (string, func(), error) {
	if input.Bundle == nil {
		return "", nil, ErrBundleVerificationNeed
	}
	if err := VerifyArchitecture(input.Bundle.TargetArch, ""); err != nil {
		return "", nil, err
	}
	verifiedPath, err := filepath.Abs(input.Bundle.Path)
	if err != nil {
		return "", nil, err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(verifiedPath); resolveErr == nil {
		verifiedPath = resolved
	}
	// Legacy single-file fixture: the verified file is the entrypoint itself.
	if input.Bundle.Entrypoint == "" {
		entrypoint := input.Entrypoint
		if entrypoint == "" {
			entrypoint = verifiedPath
		}
		requestedPath, err := filepath.Abs(entrypoint)
		if err != nil {
			return "", nil, err
		}
		if resolved, resolveErr := filepath.EvalSymlinks(requestedPath); resolveErr == nil {
			requestedPath = resolved
		}
		if requestedPath != verifiedPath {
			return "", nil, ErrBundleEntrypoint
		}
		file, err := os.Open(verifiedPath)
		if err != nil {
			return "", nil, fmt.Errorf("open bundle: %w", err)
		}
		defer file.Close()
		_, err = VerifyBundleDigest(file, input.Bundle.SHA256)
		return verifiedPath, func() {}, err
	}
	file, err := os.Open(verifiedPath)
	if err != nil {
		return "", nil, fmt.Errorf("open bundle: %w", err)
	}
	_, err = VerifyBundleDigest(file, input.Bundle.SHA256)
	_ = file.Close()
	if err != nil {
		return "", nil, err
	}
	if input.Entrypoint != input.Bundle.Entrypoint || filepath.IsAbs(input.Bundle.Entrypoint) || strings.HasPrefix(filepath.Clean(input.Bundle.Entrypoint), "..") {
		return "", nil, ErrBundleEntrypoint
	}
	dir, err := os.MkdirTemp("", "deadbolt_bundle_")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	file, err = os.Open(verifiedPath)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	defer file.Close()
	reader := tar.NewReader(file)
	for {
		header, readErr := reader.Next()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			cleanup()
			return "", nil, readErr
		}
		name := filepath.Clean(header.Name)
		if filepath.IsAbs(name) || strings.HasPrefix(name, "..") {
			cleanup()
			return "", nil, ErrBundleEntrypoint
		}
		target := filepath.Join(dir, name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				cleanup()
				return "", nil, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				cleanup()
				return "", nil, err
			}
			out, createErr := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if createErr != nil {
				cleanup()
				return "", nil, createErr
			}
			_, copyErr := io.Copy(out, reader)
			closeErr := out.Close()
			if copyErr != nil || closeErr != nil {
				cleanup()
				return "", nil, fmt.Errorf("extract bundle: %w", firstErr(copyErr, closeErr))
			}
		default:
			cleanup()
			return "", nil, ErrBundleEntrypoint
		}
	}
	entrypoint := filepath.Join(dir, filepath.Clean(input.Bundle.Entrypoint))
	if _, err := os.Stat(entrypoint); err != nil {
		cleanup()
		return "", nil, ErrBundleEntrypoint
	}
	platformFile := filepath.Join(dir, BundlePlatformPath)
	platformData, err := os.ReadFile(platformFile)
	if err != nil {
		cleanup()
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, ErrNoBundlePlatform
		}
		return "", nil, fmt.Errorf("read bundle platform metadata: %w", err)
	}
	var plat BundlePlatform
	if err := json.Unmarshal(platformData, &plat); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("%w: %v", ErrBundlePlatformInvalid, err)
	}
	if normalizeBundleOS(plat.TargetOS) == "" || normalizeBundleArch(plat.TargetArchitecture) == "" {
		cleanup()
		return "", nil, ErrBundlePlatformInvalid
	}
	// Identity equality: the embedded target must exactly match the assigned
	// execution target, not merely be independently host-compatible.
	specArch := normalizeBundleArch(input.Bundle.TargetArch)
	if specArch == "" || normalizeBundleArch(plat.TargetArchitecture) != specArch {
		cleanup()
		return "", nil, ErrBundlePlatformMismatch
	}
	if specOS := normalizeBundleOS(input.Bundle.TargetOS); specOS != "" && normalizeBundleOS(plat.TargetOS) != specOS {
		cleanup()
		return "", nil, ErrBundlePlatformMismatch
	}
	// Identity proven; host compatibility is still enforced.
	if err := VerifyArchitecture(plat.TargetArchitecture, ""); err != nil {
		cleanup()
		return "", nil, err
	}
	return entrypoint, cleanup, nil
}

func firstErr(first, second error) error {
	if first != nil {
		return first
	}
	return second
}

func (s *ProcessSupervisor) ExecuteAttempt(ctx context.Context, input *TaskInput, epoch int64) (*TaskCompletion, *ExecutionLogs, error) {
	if s.LeaseTracker == nil {
		return nil, nil, ErrLeaseAuthorityMissing
	}
	if ok, err := s.LeaseTracker.CanStart(time.Time{}); !ok || err != nil {
		return nil, nil, ErrInsufficientLeaseTTL
	}
	if err := s.verifyStart(ctx, input.AttemptID, epoch); err != nil {
		return nil, nil, err
	}
	// Start ACK can consume the entire budget; never launch based on the old check.
	if ok, err := s.LeaseTracker.CanStart(time.Time{}); !ok || err != nil {
		return nil, nil, ErrInsufficientLeaseTTL
	}
	verifiedEntrypoint, bundleCleanup, err := s.verifyBundle(input)
	if err != nil {
		return nil, nil, err
	}
	defer bundleCleanup()
	if err := ValidateTaskEnvironment(input.Env, s.TaskEnvAllowlist); err != nil {
		return nil, nil, err
	}
	runnerInput := *input
	runnerInput.Entrypoint = verifiedEntrypoint
	inputBytes, err := json.Marshal(&runnerInput)
	if err != nil {
		return nil, nil, fmt.Errorf("encode input: %w", err)
	}
	maxInputBytes := s.MaxRunnerInputBytes
	if maxInputBytes <= 0 {
		maxInputBytes = MaxSerializedRunnerInputBytes
	}
	if len(inputBytes) > maxInputBytes {
		return nil, nil, fmt.Errorf("%w: got %d bytes, limit %d", ErrRunnerInputTooLarge, len(inputBytes), maxInputBytes)
	}

	if input.TimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(input.TimeoutMs)*time.Millisecond)
		defer cancel()
	}
	resultDir := s.ResultDir
	if resultDir == "" {
		resultDir = os.TempDir()
	}
	resultFile := filepath.Join(resultDir, fmt.Sprintf("deadbolt_res_%s_%d.json", input.AttemptID, time.Now().UnixNano()))
	defer os.Remove(resultFile)
	cmd := exec.Command(s.NodePath, s.RunnerPath)
	configureProcessGroup(cmd)
	taskEnv := make(map[string]string, len(input.Env)+1)
	for k, v := range input.Env {
		taskEnv[k] = v
	}
	taskEnv["DEADBOLT_RESULT_FILE"] = resultFile
	cmd.Env = SanitizeEnvironment(os.Environ(), taskEnv, append(s.AllowlistKeys, "DEADBOLT_RESULT_FILE"))
	var stdout, stderr cappedBuffer
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
	safeLease, _ := s.LeaseTracker.SafeRemainingTTL(time.Time{})
	leaseTimer := time.NewTimer(safeLease)
	defer leaseTimer.Stop()
	type renewalResult struct {
		renewal LeaseRenewal
		err     error
	}
	renewalDone := make(chan renewalResult, 1)
	renewing := false
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
		case <-leaseTimer.C:
			_ = s.TerminateProcessGroup(pid)
			<-exit
			finalErr = ErrLeaseExpired
			goto done
		case result := <-renewalDone:
			renewing = false
			if result.err == nil {
				s.LeaseTracker.Renew(result.renewal.ExpiresAt, result.renewal.RTT)
				safe, safeErr := s.LeaseTracker.SafeRemainingTTL(time.Time{})
				if safeErr != nil {
					_ = s.TerminateProcessGroup(pid)
					<-exit
					finalErr = ErrLeaseExpired
					goto done
				}
				if !leaseTimer.Stop() {
					select {
					case <-leaseTimer.C:
					default:
					}
				}
				leaseTimer.Reset(safe)
			}
		case <-tick.C:
			if s.RenewLeaseFn != nil && !renewing {
				renewing = true
				go func() {
					renewal, renewErr := s.RenewLeaseFn(ctx, input.AttemptID, epoch)
					renewalDone <- renewalResult{renewal, renewErr}
				}()
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

const maxCapturedOutput = 1 << 20

// RedactExecutionLogs removes resolved task-secret values before logs leave a
// customer worker. Empty values are intentionally ignored: replacing an empty
// string would corrupt every log line without protecting a secret.
func RedactExecutionLogs(logs *ExecutionLogs, secrets map[string]string) *ExecutionLogs {
	if logs == nil || len(secrets) == 0 {
		return logs
	}
	values := make([]string, 0, len(secrets))
	seen := make(map[string]struct{}, len(secrets))
	for _, value := range secrets {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	// Redact longer values first so a shorter secret cannot leave a suffix of a
	// longer one exposed.
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	redact := func(value string) string {
		for _, secret := range values {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
		return value
	}
	return &ExecutionLogs{Stdout: redact(logs.Stdout), Stderr: redact(logs.Stderr)}
}

type cappedBuffer struct {
	bytes.Buffer
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.Len() >= maxCapturedOutput {
		b.truncated = true
		return len(p), nil
	}
	remaining := maxCapturedOutput - b.Len()
	if len(p) > remaining {
		_, _ = b.Buffer.Write(p[:remaining])
		b.truncated = true
		return len(p), nil
	}
	return b.Buffer.Write(p)
}

func isProcessAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	return err == nil && checkProcessAliveOS(proc, pid)
}
