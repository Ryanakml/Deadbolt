package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// HandleWorker processes `runtime worker <subcommand> [flags]`
func HandleWorker(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: runtime worker <enroll|start|drain|list> [flags]")
	}

	switch args[0] {
	case "enroll":
		return handleWorkerEnroll(args[1:])
	case "start":
		return handleWorkerStart(args[1:])
	case "drain":
		return handleWorkerDrain(args[1:])
	case "list":
		return handleWorkerList(args[1:])
	default:
		return fmt.Errorf("unknown worker subcommand: %s (supported: enroll, start, drain, list)", args[0])
	}
}

// HandleWorkerEnroll runs the worker enrollment flow
func HandleWorkerEnroll(args []string) error {
	return handleWorkerEnroll(args)
}

func handleWorkerEnroll(args []string) error {
	fs := flag.NewFlagSet("runtime worker enroll", flag.ContinueOnError)
	tokenFlag := fs.String("token", "", "Single-use enrollment token")
	keyPathFlag := fs.String("key-path", "", "Path to worker private key file (default ~/.deadbolt/worker.key)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	poolFlag := fs.String("pool", "default", "Worker pool name")
	envFlag := fs.String("env", "", "Environment ID/name (required if using --create-token)")
	createTokenFlag := fs.Bool("create-token", false, "Generate an enrollment token from control plane API first")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	cpURL := *cpURLFlag
	if cpURL == "" {
		cpURL = cfg.APIURL
	}
	cpURL = strings.TrimRight(cpURL, "/")
	cfg.APIURL = cpURL

	keyPath := *keyPathFlag
	if keyPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("resolve user home dir: %w", err)
		}
		keyPath = filepath.Join(home, ".deadbolt", "worker.key")
	}

	enrollToken := strings.TrimSpace(*tokenFlag)

	// If --create-token requested, call control plane admin API to issue enrollment token
	if *createTokenFlag {
		// Pass the raw flag: only the resolver decides fallback precedence,
		// so stored canonical EnvID wins when --env is omitted.
		sel, err := resolveEnvSelection(cfg, *envFlag)
		if err != nil {
			return err
		}
		envID := sel.EnvID
		envDisplay := *envFlag
		if envDisplay == "" {
			envDisplay = sel.EnvName
		}
		fmt.Printf("Requesting worker enrollment token for environment %s (pool: %s)...\n", envDisplay, *poolFlag)
		inPayload, _ := json.Marshal(map[string]string{"pool": *poolFlag})
		req, err := cfg.NewRequest(http.MethodPost, fmt.Sprintf("/v1/environments/%s/worker-enrollments", envID), bytes.NewReader(inPayload))
		if err != nil {
			return fmt.Errorf("create enrollment token request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("enroll-token-%d", time.Now().UnixNano()))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("request enrollment token: %w", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("failed to create enrollment token: %s", FormatAPIError(resp.StatusCode, body))
		}

		var tokenResp struct {
			Token           string `json:"token"`
			EnrollmentToken string `json:"enrollmentToken"`
			WorkerPool      string `json:"workerPool"`
			ExpiresAt       string `json:"expiresAt"`
		}
		if err := json.Unmarshal(body, &tokenResp); err != nil {
			return fmt.Errorf("parse enrollment token response: %w", err)
		}
		enrollToken = tokenResp.Token
		if enrollToken == "" {
			enrollToken = tokenResp.EnrollmentToken
		}
		fmt.Printf("Issued single-use enrollment token (expires at %s).\n", tokenResp.ExpiresAt)
	}

	if enrollToken == "" {
		return fmt.Errorf("--token is required (or pass --create-token if authenticated with admin role)")
	}

	// 1. Prepare keypair
	var privKey ed25519.PrivateKey
	var pubKey ed25519.PublicKey

	if _, err := os.Stat(keyPath); err == nil {
		p, err := worker.LoadPrivateKey(keyPath)
		if err != nil {
			return fmt.Errorf("load existing private key: %w", err)
		}
		privKey = p
		pubKey = p.Public().(ed25519.PublicKey)
	} else {
		pub, priv, err := worker.GenerateWorkerKeyPair()
		if err != nil {
			return fmt.Errorf("generate ed25519 keypair: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
			return fmt.Errorf("create key directory: %w", err)
		}
		if err := worker.SavePrivateKey(keyPath, priv); err != nil {
			return fmt.Errorf("save private key: %w", err)
		}
		privKey = priv
		pubKey = pub
	}

	// 2. Request challenge nonce from control plane
	client := &http.Client{Timeout: 10 * time.Second}
	chalPayload, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       fmt.Sprintf("req_chal_%d", time.Now().UnixNano()),
		PublicKey:       worker.EncodePublicKey(pubKey),
	})

	chalResp, err := client.Post(cpURL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalPayload))
	if err != nil {
		return fmt.Errorf("challenge request failed: %w", err)
	}
	defer chalResp.Body.Close()

	chalBody, _ := io.ReadAll(chalResp.Body)
	if chalResp.StatusCode != http.StatusOK {
		return fmt.Errorf("challenge rejected (%d): %s", chalResp.StatusCode, string(chalBody))
	}

	var chalRes worker.ChallengeResponseDTO
	if err := json.Unmarshal(chalBody, &chalRes); err != nil {
		return fmt.Errorf("parse challenge response: %w", err)
	}

	// 3. Sign challenge nonce and execute enrollment request
	sig := worker.SignChallenge(privKey, chalRes.Nonce)
	enrollPayload, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       fmt.Sprintf("req_enr_%d", time.Now().UnixNano()),
		EnrollmentToken: enrollToken,
		PublicKey:       worker.EncodePublicKey(pubKey),
		Nonce:           chalRes.Nonce,
		Signature:       sig,
	})

	enrollResp, err := client.Post(cpURL+"/worker/v1/enroll", "application/json", bytes.NewReader(enrollPayload))
	if err != nil {
		return fmt.Errorf("enroll request failed: %w", err)
	}
	defer enrollResp.Body.Close()

	enrollBody, _ := io.ReadAll(enrollResp.Body)
	if enrollResp.StatusCode != http.StatusOK {
		return fmt.Errorf("enrollment rejected (%d): %s", enrollResp.StatusCode, string(enrollBody))
	}

	var sessionRes worker.SessionResponseDTO
	if err := json.Unmarshal(enrollBody, &sessionRes); err != nil {
		return fmt.Errorf("parse enroll response: %w", err)
	}

	// 4. Persist worker identity
	if err := worker.SaveWorkerIdentity(keyPath, sessionRes.WorkerID); err != nil {
		return fmt.Errorf("save worker identity: %w", err)
	}

	fmt.Println("Worker Enrolled Successfully!")
	fmt.Printf("Worker ID:         %s\n", sessionRes.WorkerID)
	fmt.Printf("Private Key Path:  %s (mode 0600)\n", keyPath)
	fmt.Printf("Identity File:     %s (mode 0600)\n", worker.IdentityPath(keyPath))
	fmt.Printf("Control Plane:     %s\n", cpURL)
	fmt.Println("\nTo start this worker agent:")
	fmt.Printf("  runtime worker start --key-path %s --control-plane-url %s\n", keyPath, cpURL)

	return nil
}

func handleWorkerStart(args []string) error {
	fs := flag.NewFlagSet("runtime worker start", flag.ContinueOnError)
	keyPathFlag := fs.String("key-path", "", "Path to worker private key file (mode 0600)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane URL")
	bundleDirFlag := fs.String("bundle-dir", "./bundles", "Local bundle storage directory")
	poolFlag := fs.String("pool", "default", "Worker pool name")
	slotsFlag := fs.Int("slots", 2, "Concurrency slot capacity")
	nodePathFlag := fs.String("node-path", "node", "Path to Node.js executable")
	runnerPathFlag := fs.String("runner-path", "", "Path to installed Node runner script")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	cpURL := *cpURLFlag
	if cpURL == "" {
		cpURL = cfg.APIURL
	}
	keyPath := *keyPathFlag
	if keyPath == "" {
		home, _ := os.UserHomeDir()
		keyPath = filepath.Join(home, ".deadbolt", "worker.key")
	}

	runnerPath, err := resolveRunnerPath(*runnerPathFlag)
	if err != nil {
		return err
	}

	logger := log.New(os.Stdout, "[WORKER] ", log.LstdFlags|log.Lmsgprefix)

	agentCfg := worker.AgentConfig{
		ControlPlaneURL: cpURL,
		KeyPath:         keyPath,
		Pool:            *poolFlag,
		Slots:           *slotsFlag,
		NodePath:        *nodePathFlag,
		RunnerPath:      runnerPath,
		BundleDir:       *bundleDirFlag,
		Logger:          logger,
	}

	agent, err := worker.NewAgent(agentCfg)
	if err != nil {
		return fmt.Errorf("initialize worker agent: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Printf("Received termination signal; draining running tasks...")
		cancel()
	}()

	logger.Printf("Starting worker agent connected to %s (slots: %d, pool: %s, bundleDir: %s)", cpURL, *slotsFlag, *poolFlag, *bundleDirFlag)
	if err := agent.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("worker agent stopped with error: %w", err)
	}

	logger.Println("Worker agent terminated cleanly.")
	return nil
}

func handleWorkerDrain(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: runtime worker drain <worker_id>")
	}
	workerID := args[0]
	cfg := LoadConfig()

	req, err := cfg.NewRequest(http.MethodPost, fmt.Sprintf("/v1/workers/%s/drain", workerID), nil)
	if err != nil {
		return fmt.Errorf("create drain request: %w", err)
	}
	req.Header.Set("Idempotency-Key", fmt.Sprintf("worker-drain-%s-%d", workerID, time.Now().UnixNano()))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("drain request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to drain worker: %s", FormatAPIError(resp.StatusCode, body))
	}

	fmt.Printf("Drain signal accepted for worker %s.\n", workerID)
	return nil
}

// resolveRunnerPath locates the Node runner companion asset in precedence
// order: explicit flag, DEADBOLT_RUNNER_PATH, then the packaged layout
// relative to the running executable (<prefix>/share/deadbolt/runner).
// Repository source paths are never consulted. A missing asset fails fast
// with an actionable error instead of failing later inside worker startup.
func resolveRunnerPath(flagVal string) (string, error) {
	runnerPath := flagVal
	if runnerPath == "" {
		runnerPath = os.Getenv("DEADBOLT_RUNNER_PATH")
	}
	if runnerPath == "" {
		if executable, err := os.Executable(); err == nil {
			candidate := filepath.Join(filepath.Dir(executable), "..", "share", "deadbolt", "runner", "index.js")
			if fileExists(candidate) {
				runnerPath = candidate
			}
		}
	}
	if runnerPath == "" || !fileExists(runnerPath) {
		return "", fmt.Errorf("Node runner is not installed. Install the Deadbolt runner companion asset or pass --runner-path /path/to/index.js (DEADBOLT_RUNNER_PATH is also supported)")
	}
	return runnerPath, nil
}

// EnvSelection is the canonical environment context for one CLI command.
// Commands operate on EnvID; names are display context and scope.
type EnvSelection struct {
	EnvID       string
	EnvName     string
	ProjectID   string
	ProjectName string
}

// resolveEnvSelection determines the canonical environment context using
// only authenticated public tenant discovery routes. Precedence:
//
//	explicit UUID (exact, no scope filter)
//	→ explicit name + project scope
//	→ stored env UUID
//	→ stored name + stored project scope
//	→ unique org-wide name
//	→ explicit ambiguity error
//
// It never silently picks the first match.
func resolveEnvSelection(cfg Config, explicit string) (EnvSelection, error) {
	if explicit != "" {
		if isUUIDString(explicit) {
			return EnvSelection{EnvID: explicit}, nil
		}
		return selectEnvByName(cfg, explicit, resolveProjectScope(cfg))
	}
	if cfg.EnvID != "" {
		return EnvSelection{EnvID: cfg.EnvID, EnvName: cfg.Env, ProjectID: cfg.ProjectID, ProjectName: cfg.Project}, nil
	}
	if cfg.Env == "" {
		return EnvSelection{}, fmt.Errorf("no environment selected: pass --env <name-or-UUID> or run `runtime bootstrap`")
	}
	return selectEnvByName(cfg, cfg.Env, resolveProjectScope(cfg))
}

// isUUIDString reports whether s has canonical UUID text shape. It gates
// exact-identity handling only; existence and authorization stay server-side.
func isUUIDString(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' {
				continue
			}
			return false
		}
	}
	return true
}

// resolveProjectScope returns the currently selected project context, if
// any: DEADBOLT_PROJECT first, then the local deadbolt.config.json project,
// then the stored bootstrap selection.
func resolveProjectScope(cfg Config) string {
	if v := strings.TrimSpace(os.Getenv("DEADBOLT_PROJECT")); v != "" {
		return v
	}
	if prj, err := LoadProjectConfig("."); err == nil && prj != nil && strings.TrimSpace(prj.Project) != "" {
		return strings.TrimSpace(prj.Project)
	}
	return strings.TrimSpace(cfg.Project)
}

// selectEnvByName collects every environment matching nameOrID across the
// scoped projects: zero matches fail, exactly one wins, and several fail
// with an explicit ambiguity error.
func selectEnvByName(cfg Config, nameOrID, scope string) (EnvSelection, error) {
	projects, err := listBootstrapProjects(cfg)
	if err != nil {
		return EnvSelection{}, err
	}
	candidates := projects
	if scope != "" {
		candidates = nil
		for _, p := range projects {
			if p.Name == scope || p.ID == scope {
				candidates = append(candidates, p)
			}
		}
		if len(candidates) == 0 {
			return EnvSelection{}, fmt.Errorf("project %q was not found in the authenticated organization", scope)
		}
	}
	type match struct {
		project bootstrapProjectRef
		envID   string
		envName string
	}
	var matches []match
	for _, project := range candidates {
		envs, err := listProjectEnvironments(cfg, project.ID)
		if err != nil {
			return EnvSelection{}, err
		}
		for _, environment := range envs {
			if environment.ID == nameOrID || environment.Name == nameOrID {
				matches = append(matches, match{project: project, envID: environment.ID, envName: environment.Name})
			}
		}
	}
	switch len(matches) {
	case 0:
		return EnvSelection{}, fmt.Errorf("environment %q was not found in the authenticated organization", nameOrID)
	case 1:
		m := matches[0]
		return EnvSelection{EnvID: m.envID, EnvName: m.envName, ProjectID: m.project.ID, ProjectName: m.project.Name}, nil
	default:
		var lines []string
		for _, m := range matches {
			lines = append(lines, fmt.Sprintf("  %s (project %q, id %s)", m.envName, m.project.Name, m.envID))
		}
		return EnvSelection{}, fmt.Errorf("environment %q matches %d environments; rerun with an exact environment UUID:\n%s", nameOrID, len(matches), strings.Join(lines, "\n"))
	}
}

type bootstrapProjectRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type bootstrapEnvRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func listBootstrapProjects(cfg Config) ([]bootstrapProjectRef, error) {
	req, err := cfg.NewRequest(http.MethodGet, "/v1/projects", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list projects: %s", FormatAPIError(resp.StatusCode, body))
	}
	var list struct {
		Projects []bootstrapProjectRef `json:"projects"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("parse projects: %w", err)
	}
	return list.Projects, nil
}

func listProjectEnvironments(cfg Config, projectID string) ([]bootstrapEnvRef, error) {
	req, err := cfg.NewRequest(http.MethodGet, fmt.Sprintf("/v1/projects/%s/environments", projectID), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list environments: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list environments: %s", FormatAPIError(resp.StatusCode, body))
	}
	var list struct {
		Environments []bootstrapEnvRef `json:"environments"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("parse environments: %w", err)
	}
	return list.Environments, nil
}

func handleWorkerList(args []string) error {
	fs := flag.NewFlagSet("runtime worker list", flag.ContinueOnError)
	envFlag := fs.String("env", "", "Environment name or ID")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	cursorFlag := fs.String("cursor", "", "Pagination cursor")
	limitFlag := fs.Int("limit", 25, "Maximum number of items")
	jsonFlag := fs.Bool("json", false, "Output as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}
	env := *envFlag
	if env == "" {
		env = cfg.Env
	}
	if env == "" {
		return fmt.Errorf("--env is required")
	}

	// Operate on the canonical environment identity.
	sel, err := resolveEnvSelection(cfg, *envFlag)
	if err != nil {
		return err
	}

	q := url.Values{}
	q.Set("environment", sel.EnvID)
	if *cursorFlag != "" {
		q.Set("cursor", *cursorFlag)
	}
	if *limitFlag > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limitFlag))
	}

	req, err := cfg.NewRequest(http.MethodGet, "/v1/workers?"+q.Encode(), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("worker list request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker list failed: %s", FormatAPIError(resp.StatusCode, body))
	}

	if *jsonFlag {
		fmt.Println(string(body))
		return nil
	}

	var result struct {
		Items []struct {
			ID                string   `json:"id"`
			Pool              string   `json:"pool"`
			Status            string   `json:"status"`
			DeploymentDigests []string `json:"deploymentDigests"`
		} `json:"items"`
		NextCursor *string `json:"nextCursor"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse workers response: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "WORKER ID\tPOOL\tSTATUS\tDEPLOYMENTS")
	for _, item := range result.Items {
		deployments := strings.Join(item.DeploymentDigests, ", ")
		if deployments == "" {
			deployments = "(none)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", item.ID, item.Pool, item.Status, deployments)
	}
	w.Flush()

	if result.NextCursor != nil && *result.NextCursor != "" {
		fmt.Printf("\nNext cursor: %s\n", *result.NextCursor)
	}

	return nil
}
