package cli

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

type BuildOptions struct {
	ProjectDir   string
	WorkflowPath string
	BundleDir    string
	OutputDir    string
	TargetArch   string
	TargetOS     string
}

type BuildResult struct {
	BundleDigest         string   `json:"bundleDigest"`
	DependencyLockDigest string   `json:"dependencyLockDigest"`
	TargetArchitecture   string   `json:"targetArchitecture"`
	TargetOS             string   `json:"targetOS"`
	BundlePath           string   `json:"bundlePath"`
	ManifestPath         string   `json:"manifestPath"`
	SecretNames          []string `json:"secretNames"`
	Tasks                []string `json:"tasks"`
	Workflows            []string `json:"workflows"`
}

// HandleBuild compiles local tasks, creates deterministic .tar bundle, and produces validated deployment manifest
func HandleBuild(args []string) error {
	fs := flag.NewFlagSet("runtime build", flag.ContinueOnError)
	dirFlag := fs.String("dir", ".", "Project root directory")
	workflowFlag := fs.String("workflow", "", "Path to workflow definition JSON")
	bundleDirFlag := fs.String("bundle-dir", "", "Directory to store compiled bundle .tar files")
	outDirFlag := fs.String("out-dir", "", "Directory to write output manifest.json")
	archFlag := fs.String("arch", "amd64", "Target architecture (amd64 or arm64)")
	osFlag := fs.String("os", "linux", "Target operating system (linux)")
	jsonFlag := fs.Bool("json", false, "Output build result as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	opts := BuildOptions{
		ProjectDir:   *dirFlag,
		WorkflowPath: *workflowFlag,
		BundleDir:    *bundleDirFlag,
		OutputDir:    *outDirFlag,
		TargetArch:   *archFlag,
		TargetOS:     *osFlag,
	}

	result, err := BuildDeployment(opts)
	if err != nil {
		return fmt.Errorf("build failed: %w", err)
	}

	if *jsonFlag {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Println("Immutable Deployment Bundle Built Successfully!")
		fmt.Printf("Bundle Digest (SHA-256):         %s\n", result.BundleDigest)
		fmt.Printf("Dependency Lock Digest (SHA-256): %s\n", result.DependencyLockDigest)
		fmt.Printf("Target Architecture:             %s\n", result.TargetArchitecture)
		fmt.Printf("Target OS:                       %s\n", result.TargetOS)
		fmt.Printf("Bundle Artifact:                 %s\n", result.BundlePath)
		fmt.Printf("Deployment Manifest:             %s\n", result.ManifestPath)
		fmt.Printf("Registered Workflows:            %s\n", strings.Join(result.Workflows, ", "))
		fmt.Printf("Registered Tasks:                %s\n", strings.Join(result.Tasks, ", "))
		fmt.Println("\nTo register this deployment on the control plane, run:")
		fmt.Printf("  runtime deploy --env staging --manifest %s\n", result.ManifestPath)
	}

	return nil
}

// BuildDeployment assembles the deterministic bundle and validated deployment manifest
func BuildDeployment(opts BuildOptions) (*BuildResult, error) {
	if opts.ProjectDir == "" {
		opts.ProjectDir = "."
	}
	if opts.TargetArch == "" {
		opts.TargetArch = "amd64"
	}
	if opts.TargetOS == "" {
		opts.TargetOS = "linux"
	}
	if opts.BundleDir == "" {
		opts.BundleDir = filepath.Join(opts.ProjectDir, "bundles")
	}
	if opts.OutputDir == "" {
		opts.OutputDir = filepath.Join(opts.ProjectDir, "dist")
	}

	if err := os.MkdirAll(opts.BundleDir, 0o755); err != nil {
		return nil, fmt.Errorf("create bundle dir: %w", err)
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	// 1. Read deadbolt.config.json if present
	var secretNames []string
	if prj, err := LoadProjectConfig(opts.ProjectDir); err == nil && prj != nil {
		if opts.WorkflowPath == "" && prj.Workflow != "" {
			opts.WorkflowPath = filepath.Join(opts.ProjectDir, "workflow.json")
		}
		if prj.TargetArch != "" {
			opts.TargetArch = prj.TargetArch
		}
		secretNames = append(secretNames, prj.Secrets...)
	}

	// 2. Discover task files in tasks/
	tasksDir := filepath.Join(opts.ProjectDir, "tasks")
	taskFiles := make(map[string][]byte)

	if info, err := os.Stat(tasksDir); err == nil && info.IsDir() {
		entries, err := os.ReadDir(tasksDir)
		if err != nil {
			return nil, fmt.Errorf("read tasks dir: %w", err)
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".ts") {
				return nil, fmt.Errorf("task %s is TypeScript; M1 bundles execute JavaScript only. Compile it to .js before runtime build", e.Name())
			}
			if !e.IsDir() && (strings.HasSuffix(e.Name(), ".js") || strings.HasSuffix(e.Name(), ".mjs")) {
				p := filepath.Join(tasksDir, e.Name())
				data, err := os.ReadFile(p)
				if err != nil {
					return nil, fmt.Errorf("read task %s: %w", e.Name(), err)
				}
				taskFiles["tasks/"+e.Name()] = data
			}
		}
	}

	// Also check if src/tasks exists
	srcTasksDir := filepath.Join(opts.ProjectDir, "src", "tasks")
	if info, err := os.Stat(srcTasksDir); err == nil && info.IsDir() {
		entries, _ := os.ReadDir(srcTasksDir)
		for _, e := range entries {
			if !e.IsDir() && (strings.HasSuffix(e.Name(), ".js") || strings.HasSuffix(e.Name(), ".mjs")) {
				data, _ := os.ReadFile(filepath.Join(srcTasksDir, e.Name()))
				taskFiles["tasks/"+e.Name()] = data
			}
		}
	}

	if len(taskFiles) == 0 {
		return nil, fmt.Errorf("no JavaScript task entrypoints found in %s; run runtime init or add tasks/*.js", tasksDir)
	}

	for relPath, content := range taskFiles {
		if err := validateTaskDependencies(relPath, content); err != nil {
			return nil, err
		}
	}

	// 3. Build deterministic tarball: sorted file keys, fixed modtime
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	sortedPaths := make([]string, 0, len(taskFiles))
	for p := range taskFiles {
		sortedPaths = append(sortedPaths, p)
	}
	sort.Strings(sortedPaths)

	// Fixed epoch timestamp for deterministic reproducible builds
	deterministicTime := time.Unix(1700000000, 0).UTC()

	for _, relPath := range sortedPaths {
		content := taskFiles[relPath]
		hdr := &tar.Header{
			Name:     relPath,
			Mode:     0o644,
			Size:     int64(len(content)),
			ModTime:  deterministicTime,
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, fmt.Errorf("write tar header: %w", err)
		}
		if _, err := tw.Write(content); err != nil {
			return nil, fmt.Errorf("write tar content: %w", err)
		}
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("close tar archive: %w", err)
	}

	tarBytes := tarBuf.Bytes()
	bundleSHA := sha256.Sum256(tarBytes)
	bundleDigest := hex.EncodeToString(bundleSHA[:])

	// Write bundle to <bundleDir>/<bundleDigest>.tar
	bundlePath := filepath.Join(opts.BundleDir, bundleDigest+".tar")
	if err := os.WriteFile(bundlePath, tarBytes, 0o644); err != nil {
		return nil, fmt.Errorf("write bundle archive: %w", err)
	}

	// 4. Calculate dependencyLockDigest
	lockDigest, err := calculateLockDigest(opts.ProjectDir)
	if err != nil {
		return nil, err
	}

	// 5. Load or generate workflow definition
	var workflows []any
	var tasks []any

	if opts.WorkflowPath == "" {
		candidates := []string{
			filepath.Join(opts.ProjectDir, "workflow.json"),
			filepath.Join(opts.ProjectDir, "dist", "workflow.json"),
		}
		for _, c := range candidates {
			if fileExists(c) {
				opts.WorkflowPath = c
				break
			}
		}
	}

	if opts.WorkflowPath != "" && fileExists(opts.WorkflowPath) {
		wfData, err := os.ReadFile(opts.WorkflowPath)
		if err != nil {
			return nil, fmt.Errorf("read workflow %s: %w", opts.WorkflowPath, err)
		}
		var parsedWF map[string]any
		if err := json.Unmarshal(wfData, &parsedWF); err != nil {
			return nil, fmt.Errorf("parse workflow: %w", err)
		}
		workflows = append(workflows, parsedWF)
	}

	if len(workflows) == 0 {
		return nil, fmt.Errorf("workflow definition is required; add workflow.json or pass --workflow")
	}

	// Check if tasks.json exists in project dir
	tasksPath := filepath.Join(opts.ProjectDir, "tasks.json")
	if !fileExists(tasksPath) {
		return nil, fmt.Errorf("task definition file is required: %s", tasksPath)
	}
	tData, err := os.ReadFile(tasksPath)
	if err != nil {
		return nil, fmt.Errorf("read task definitions: %w", err)
	}
	var parsedTasks []any
	if err := json.Unmarshal(tData, &parsedTasks); err != nil {
		return nil, fmt.Errorf("parse task definitions: %w", err)
	}
	tasks = append(tasks, parsedTasks...)

	// Task definitions are authored by the project; never infer recovery or
	// schemas from names or workflow wiring.
	if len(tasks) == 0 {
		return nil, fmt.Errorf("tasks.json must define at least one task")
	}

	for _, t := range tasks {
		tm, ok := t.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid task definition in tasks.json")
		}
		tName, _ := tm["name"].(string)
		entrypoint, _ := tm["entrypoint"].(string)
		if entrypoint == "" {
			return nil, fmt.Errorf("task %q missing entrypoint in tasks.json", tName)
		}
		relEntry := entrypoint
		if !strings.HasPrefix(relEntry, "tasks/") && !strings.HasPrefix(relEntry, "src/tasks/") {
			relEntry = "tasks/" + relEntry
		}
		if _, exists := taskFiles[entrypoint]; !exists {
			if _, existsRel := taskFiles[relEntry]; !existsRel {
				return nil, fmt.Errorf("task %q entrypoint %q was not found in project", tName, entrypoint)
			}
		}
	}

	// Deduplicate secret names
	uniqueSecrets := make([]string, 0)
	secMap := make(map[string]bool)
	for _, s := range secretNames {
		s = strings.TrimSpace(s)
		if s != "" && !secMap[s] {
			secMap[s] = true
			uniqueSecrets = append(uniqueSecrets, s)
		}
	}

	// 7. Assemble deployment manifest
	manifest := map[string]any{
		"manifestVersion":      1,
		"sdkVersion":           "0.1.0",
		"protocolMajor":        1,
		"nodeRuntimeMajor":     24,
		"targetOS":             opts.TargetOS,
		"targetArchitecture":   opts.TargetArch,
		"dependencyLockDigest": lockDigest,
		"bundleDigest":         bundleDigest,
		"secretNames":          uniqueSecrets,
		"tasks":                tasks,
		"workflows":            workflows,
	}

	// 8. Marshal and validate against canonical deployment.schema.json
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	canonicalParsed, err := contracts.ParseJSON(manifestJSON)
	if err != nil {
		return nil, fmt.Errorf("canonical parse error: %w", err)
	}

	if err := contracts.ValidateDeployment(canonicalParsed); err != nil {
		return nil, fmt.Errorf("contract validation error: %w", err)
	}

	// 9. Write output manifests
	distManifestPath := filepath.Join(opts.OutputDir, "manifest.json")
	if err := os.WriteFile(distManifestPath, manifestJSON, 0o644); err != nil {
		return nil, fmt.Errorf("write dist manifest: %w", err)
	}

	bundleManifestPath := filepath.Join(opts.BundleDir, bundleDigest+"-manifest.json")
	if err := os.WriteFile(bundleManifestPath, manifestJSON, 0o644); err != nil {
		return nil, fmt.Errorf("write bundle manifest: %w", err)
	}

	var taskNames []string
	for _, t := range tasks {
		if tm, ok := t.(map[string]any); ok {
			taskNames = append(taskNames, tm["name"].(string))
		}
	}
	var wfNames []string
	for _, w := range workflows {
		if wm, ok := w.(map[string]any); ok {
			wfNames = append(wfNames, wm["name"].(string))
		}
	}

	return &BuildResult{
		BundleDigest:         bundleDigest,
		DependencyLockDigest: lockDigest,
		TargetArchitecture:   opts.TargetArch,
		TargetOS:             opts.TargetOS,
		BundlePath:           bundlePath,
		ManifestPath:         distManifestPath,
		SecretNames:          uniqueSecrets,
		Tasks:                taskNames,
		Workflows:            wfNames,
	}, nil
}

func calculateLockDigest(projectDir string) (string, error) {
	candidates := []string{
		filepath.Join(projectDir, "pnpm-lock.yaml"),
		filepath.Join(projectDir, "package-lock.json"),
	}

	for _, c := range candidates {
		if data, err := os.ReadFile(c); err == nil && len(data) > 0 {
			h := sha256.Sum256(data)
			return hex.EncodeToString(h[:]), nil
		}
	}

	return "", fmt.Errorf("dependency lockfile is required (package-lock.json or pnpm-lock.yaml); run npm install or pnpm install")
}

var (
	importRegex        = regexp.MustCompile(`(?m)\bimport\s+(?:(?:(?:\*\s+as\s+[\w$]+|[\w$]+|\{[^}]*\})\s+from\s+)|(?:\s*))['"]([^'"]+)['"]`)
	exportRegex        = regexp.MustCompile(`(?m)\bexport\s+(?:(?:\*|[\w$]+|\{[^}]*\})\s+from\s+)['"]([^'"]+)['"]`)
	requireRegex       = regexp.MustCompile(`\brequire\s*\(\s*['"]([^'"]+)['"]\s*\)`)
	dynamicImportRegex = regexp.MustCompile(`\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)`)

	nodeBuiltinModules = map[string]struct{}{
		"assert":              {},
		"async_hooks":         {},
		"buffer":              {},
		"child_process":       {},
		"cluster":             {},
		"console":             {},
		"constants":           {},
		"crypto":              {},
		"dgram":               {},
		"diagnostics_channel": {},
		"dns":                 {},
		"domain":              {},
		"events":              {},
		"fs":                  {},
		"http":                {},
		"http2":               {},
		"https":               {},
		"inspector":           {},
		"module":              {},
		"net":                 {},
		"os":                  {},
		"path":                {},
		"perf_hooks":          {},
		"process":             {},
		"punycode":            {},
		"querystring":         {},
		"readline":            {},
		"repl":                {},
		"stream":              {},
		"string_decoder":      {},
		"test":                {},
		"timers":              {},
		"tls":                 {},
		"trace_events":        {},
		"tty":                 {},
		"url":                 {},
		"util":                {},
		"v8":                  {},
		"vm":                  {},
		"wasi":                {},
		"worker_threads":      {},
		"zlib":                {},
	}
)

func isNodeBuiltin(moduleName string) bool {
	if strings.HasPrefix(moduleName, "node:") {
		return true
	}
	root := strings.Split(moduleName, "/")[0]
	_, ok := nodeBuiltinModules[root]
	return ok
}

func validateTaskDependencies(taskPath string, content []byte) error {
	matches := append(importRegex.FindAllSubmatch(content, -1), exportRegex.FindAllSubmatch(content, -1)...)
	matches = append(matches, requireRegex.FindAllSubmatch(content, -1)...)
	matches = append(matches, dynamicImportRegex.FindAllSubmatch(content, -1)...)

	for _, m := range matches {
		if len(m) > 1 {
			specifier := strings.TrimSpace(string(m[1]))
			if strings.HasPrefix(specifier, ".") || strings.HasPrefix(specifier, "/") {
				continue
			}
			if isNodeBuiltin(specifier) {
				continue
			}
			return fmt.Errorf("task %s imports external dependency %q: M1 tasks must be self-contained or use supported packaged dependencies", taskPath, specifier)
		}
	}
	return nil
}
