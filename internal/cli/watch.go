package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// RunWatch implements the local immutable file-watch loop. Every successful
// build creates a new deployment; existing bundles are never deleted, so old
// workers and active runs keep their pinned artifact.
func RunWatch(args []string) error {
	fsFlags := flag.NewFlagSet("runtime watch", flag.ContinueOnError)
	dirFlag := fsFlags.String("dir", ".", "Project root to watch and build")
	workflowFlag := fsFlags.String("workflow", "", "Workflow to activate for new runs")
	envFlag := fsFlags.String("env", "", "Target environment name or UUID")
	bundleDirFlag := fsFlags.String("bundle-dir", "", "Directory for immutable bundle archives")
	outDirFlag := fsFlags.String("out-dir", "", "Directory for generated manifests")
	intervalFlag := fsFlags.Duration("interval", time.Second, "Polling interval for file changes")
	onceFlag := fsFlags.Bool("once", false, "Build and register once, then exit")
	noActivateFlag := fsFlags.Bool("no-activate", false, "Register deployments without changing the workflow active pointer")
	cpURLFlag := fsFlags.String("control-plane-url", "", "Control plane base URL")
	if err := fsFlags.Parse(args); err != nil {
		return err
	}
	if *intervalFlag <= 0 {
		return fmt.Errorf("--interval must be greater than zero")
	}
	root, err := filepath.Abs(*dirFlag)
	if err != nil {
		return fmt.Errorf("resolve watch directory: %w", err)
	}
	if _, err := os.Stat(root); err != nil {
		return fmt.Errorf("watch directory %s: %w", root, err)
	}

	cfg := LoadConfig()
	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}
	if !*noActivateFlag && strings.TrimSpace(*workflowFlag) == "" {
		return fmt.Errorf("--workflow is required unless --no-activate is set")
	}

	var previous string
	for {
		fingerprint, err := watchFingerprint(root)
		if err != nil {
			return err
		}
		if previous == "" || fingerprint != previous {
			if err := buildAndRegisterWatchDeployment(root, cfg, *workflowFlag, *envFlag, *bundleDirFlag, *outDirFlag, !*noActivateFlag); err != nil {
				if *onceFlag {
					return err
				}
				fmt.Fprintf(os.Stderr, "watch: deployment update failed: %v\n", err)
			} else {
				previous = fingerprint
			}
			if *onceFlag {
				return nil
			}
		}
		time.Sleep(*intervalFlag)
	}
}

func buildAndRegisterWatchDeployment(root string, cfg Config, workflow, env, bundleDir, outDir string, activate bool) error {
	result, err := BuildDeployment(BuildOptions{
		ProjectDir:   root,
		WorkflowPath: "",
		BundleDir:    bundleDir,
		OutputDir:    outDir,
	})
	if err != nil {
		return fmt.Errorf("build immutable deployment: %w", err)
	}
	manifest, err := os.ReadFile(result.ManifestPath)
	if err != nil {
		return fmt.Errorf("read generated manifest: %w", err)
	}
	sel, err := resolveEnvSelection(cfg, env)
	if err != nil {
		return fmt.Errorf("resolve watch environment: %w", err)
	}
	dep, _, err := RegisterDeployment(cfg, sel.EnvID, manifest)
	if err != nil {
		return fmt.Errorf("register immutable deployment: %w", err)
	}
	fmt.Printf("Watch deployment registered: %s (bundle %s)\n", dep.ID, dep.BundleDigest)
	if activate {
		if err := activateDeployment(cfg, dep.ID, workflow, sel.EnvID); err != nil {
			return err
		}
	}
	return nil
}

func activateDeployment(cfg Config, deploymentID, workflow, envID string) error {
	return activateDeploymentWithRevision(cfg, deploymentID, workflow, envID, 0)
}

func activateDeploymentWithRevision(cfg Config, deploymentID, workflow, envID string, expectedRevision int64) error {
	payload, err := json.Marshal(map[string]any{
		"deploymentId":      deploymentID,
		"expectedRevision":  expectedRevision,
		"allowSingleWorker": false,
	})
	if err != nil {
		return fmt.Errorf("encode activation request: %w", err)
	}
	path := fmt.Sprintf("/v1/workflows/%s/activate?environment=%s", url.PathEscape(workflow), url.QueryEscape(envID))
	req, err := cfg.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create activation request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", fmt.Sprintf("watch-activate-%s-%d", deploymentID, time.Now().UnixNano()))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("activate deployment: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("activate deployment: %s", FormatAPIError(resp.StatusCode, body))
	}
	fmt.Printf("Watch deployment activated for new runs: workflow=%s deployment=%s\n", workflow, deploymentID)
	return nil
}

func watchFingerprint(root string) (string, error) {
	files := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if path != root && (name == ".git" || name == "node_modules" || name == "bundles" || name == "dist") {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type().IsRegular() {
			ext := filepath.Ext(path)
			base := filepath.Base(path)
			if ext == ".js" || ext == ".json" || ext == ".ts" || ext == ".mjs" || base == "package-lock.json" || base == "pnpm-lock.yaml" {
				files = append(files, path)
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("scan watch directory: %w", err)
	}
	sort.Strings(files)
	h := sha256.New()
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read watched file %s: %w", path, err)
		}
		h.Write([]byte(strings.TrimPrefix(path, root)))
		h.Write(data)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
