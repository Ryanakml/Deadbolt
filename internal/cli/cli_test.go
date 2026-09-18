package cli

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitProject(t *testing.T) {
	tmpDir := t.TempDir()

	// Initial init should succeed
	err := InitProject(tmpDir, "sample-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	expectedFiles := []string{
		"deadbolt.config.json",
		"package.json",
		"package-lock.json",
		"workflow.json",
		"tasks/validate.js",
		"tasks/provision.js",
		"tasks/notify.js",
		".env.example",
		".gitignore",
	}

	for _, f := range expectedFiles {
		p := filepath.Join(tmpDir, f)
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("expected file %s does not exist", f)
		}
	}

	// Verify deadbolt.config.json content
	cfg, err := LoadProjectConfig(tmpDir)
	if err != nil {
		t.Fatalf("LoadProjectConfig failed: %v", err)
	}
	if cfg.Project != "sample-service" {
		t.Errorf("expected project name sample-service, got %s", cfg.Project)
	}
	if cfg.Workflow != "customer-onboarding" {
		t.Errorf("expected workflow name customer-onboarding, got %s", cfg.Workflow)
	}

	// Running init again without force should fail
	err = InitProject(tmpDir, "sample-service", false)
	if err == nil {
		t.Errorf("expected error when re-running InitProject without force, got nil")
	}

	// Running init with force should succeed
	err = InitProject(tmpDir, "sample-service", true)
	if err != nil {
		t.Errorf("expected force init to succeed, got %v", err)
	}
}

func TestBuildProjectDeterminism(t *testing.T) {
	tmpDir := t.TempDir()

	err := InitProject(tmpDir, "build-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	buildOpts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}

	// First build
	result1, err := BuildDeployment(buildOpts)
	if err != nil {
		t.Fatalf("First BuildDeployment failed: %v", err)
	}

	if len(result1.BundleDigest) != 64 {
		t.Errorf("expected 64-char sha256 bundleDigest, got %s", result1.BundleDigest)
	}

	// Verify bundle file on disk
	expectedBundlePath := filepath.Join(tmpDir, "bundles", result1.BundleDigest+".tar")
	bundleBytes, err := os.ReadFile(expectedBundlePath)
	if err != nil {
		t.Fatalf("failed to read bundle archive at %s: %v", expectedBundlePath, err)
	}

	// Verify digest computation matches raw bytes sha256
	h := sha256.Sum256(bundleBytes)
	actualDigest := hex.EncodeToString(h[:])
	if actualDigest != result1.BundleDigest {
		t.Errorf("manifest BundleDigest %s does not match bundle file sha256 %s", result1.BundleDigest, actualDigest)
	}

	// Second build should produce byte-for-byte identical output (determinism)
	result2, err := BuildDeployment(buildOpts)
	if err != nil {
		t.Fatalf("Second BuildDeployment failed: %v", err)
	}

	if result1.BundleDigest != result2.BundleDigest {
		t.Errorf("build is not deterministic: digest1=%s, digest2=%s", result1.BundleDigest, result2.BundleDigest)
	}
	if result1.DependencyLockDigest != result2.DependencyLockDigest {
		t.Errorf("dependencyLockDigest mismatch: %s vs %s", result1.DependencyLockDigest, result2.DependencyLockDigest)
	}
}

func TestKeychainStorage(t *testing.T) {
	tmpDir := t.TempDir()
	SetCustomCredentialsDir(tmpDir)

	service := "deadbolt-test"
	account := "api_key"
	secret := "test-credential-value-not-a-token"

	// Store credential
	err := StoreCredential(service, account, secret)
	if err != nil {
		t.Fatalf("StoreCredential failed: %v", err)
	}

	// Verify file permissions (0600)
	credFile := filepath.Join(tmpDir, "credentials.json")
	info, err := os.Stat(credFile)
	if err != nil {
		t.Fatalf("credentials.json not found: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("expected credentials.json permissions 0600, got %#o", info.Mode().Perm())
	}

	// Retrieve credential
	retrieved, err := GetCredential(service, account)
	if err != nil {
		t.Fatalf("GetCredential failed: %v", err)
	}
	if retrieved != secret {
		t.Errorf("expected secret %q, got %q", secret, retrieved)
	}

	// Delete credential
	err = DeleteCredential(service, account)
	if err != nil {
		t.Fatalf("DeleteCredential failed: %v", err)
	}

	// Retrieve after delete should be empty
	retrievedAfter, err := GetCredential(service, account)
	if err == nil && retrievedAfter != "" {
		t.Errorf("expected deleted credential to be empty, got %q", retrievedAfter)
	}
}

func TestDoctorSecretMasking(t *testing.T) {
	tmpDir := t.TempDir()

	// Create project with .env having secrets
	err := InitProject(tmpDir, "doctor-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	superSecretValue := "super_secret_token_never_expose_998877"
	envContent := "API_SECRET=" + superSecretValue + "\nSTRIPE_KEY=sk_test_12345\n"
	_ = os.WriteFile(filepath.Join(tmpDir, ".env"), []byte(envContent), 0600)

	// Change working dir to tmpDir
	origWd, _ := os.Getwd()
	defer os.Chdir(origWd)
	_ = os.Chdir(tmpDir)

	// Build to have manifest and bundles
	buildOpts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}
	result, err := BuildDeployment(buildOpts)
	if err != nil {
		t.Fatalf("BuildDeployment failed: %v", err)
	}

	// Run doctor checks
	cfg := Config{
		APIURL: "http://127.0.0.1:8080",
		Env:    "development",
	}
	report := RunDoctorChecks(cfg, filepath.Join(tmpDir, "dist", "manifest.json"), filepath.Join(tmpDir, "bundles"))

	// Verify doctor report:
	// 1. Bundle digest check should pass
	foundBundleCheck := false
	for _, c := range report.Checks {
		if strings.Contains(c.Name, "Bundle Integrity") {
			foundBundleCheck = true
			if c.Status != StatusOK {
				t.Errorf("expected bundle integrity to be OK, got %s: %s", c.Status, c.Message)
			}
		}
	}
	if !foundBundleCheck {
		t.Errorf("doctor report missing Bundle Integrity check")
	}

	// 2. Critical security requirement: secret values must NEVER appear in report output
	reportJSON, _ := json.Marshal(report)
	reportStr := string(reportJSON)

	if strings.Contains(reportStr, superSecretValue) {
		t.Fatalf("CRITICAL SECURITY VIOLATION: Doctor report leaked secret value %q in output!", superSecretValue)
	}
	if strings.Contains(reportStr, "sk_test_12345") {
		t.Fatalf("CRITICAL SECURITY VIOLATION: Doctor report leaked secret value 'sk_test_12345' in output!")
	}

	_ = result
}

func TestBuildArchiveStructure(t *testing.T) {
	tmpDir := t.TempDir()

	err := InitProject(tmpDir, "archive-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	buildOpts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}
	result, err := BuildDeployment(buildOpts)
	if err != nil {
		t.Fatalf("BuildDeployment failed: %v", err)
	}

	// Read tar archive entries
	bundleFile := filepath.Join(tmpDir, "bundles", result.BundleDigest+".tar")
	f, err := os.Open(bundleFile)
	if err != nil {
		t.Fatalf("open bundle failed: %v", err)
	}
	defer f.Close()

	tr := tar.NewReader(f)
	foundTasks := map[string]bool{}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar reading error: %v", err)
		}
		foundTasks[hdr.Name] = true
	}

	expectedEntries := []string{"tasks/validate.js", "tasks/provision.js", "tasks/notify.js"}
	for _, entry := range expectedEntries {
		if !foundTasks[entry] {
			t.Errorf("expected bundle to contain entry %s, found: %v", entry, foundTasks)
		}
	}
}

func TestLoginCIEnvironmentDetection(t *testing.T) {
	// Set CI environment variable
	origCI := os.Getenv("CI")
	defer os.Setenv("CI", origCI)
	os.Setenv("CI", "true")

	// In CI, login without --api-key and without loopback control plane must fail with actionable guidance
	err := RunLogin([]string{"--control-plane-url", "https://api.deadbolt.cloud"})
	if err == nil {
		t.Fatalf("expected login to fail in CI without api key")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "not supported in non-interactive / CI environments") {
		t.Errorf("expected error to explain CI non-interactive limitation, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "DEADBOLT_API_KEY") {
		t.Errorf("expected error to suggest DEADBOLT_API_KEY, got: %s", errMsg)
	}
}

func TestDoctorNodeToolchainDiagnostic(t *testing.T) {
	res := checkNodeToolchain()
	if res.Name == "" {
		t.Errorf("expected check name to be non-empty")
	}
	// Verify that if node is mismatched or missing, actionable remediation is provided
	if res.Status != StatusOK && res.Remediation == "" {
		t.Errorf("expected actionable remediation when Node check fails or warns")
	}
}

func TestDoctorFreshInitWithoutBuild(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Fresh init generates workflow.json but no dist/manifest.json
	err := InitProject(tmpDir, "fresh-service", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	workflowPath := filepath.Join(tmpDir, "workflow.json")
	if !fileExists(workflowPath) {
		t.Fatalf("expected workflow.json to exist after init")
	}
	distManifestPath := filepath.Join(tmpDir, "dist", "manifest.json")
	if fileExists(distManifestPath) {
		t.Fatalf("expected dist/manifest.json to NOT exist before build")
	}

	origWd, _ := os.Getwd()
	defer os.Chdir(origWd)
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to tmpDir: %v", err)
	}

	// 2. Check manifest and bundles without explicit path (auto-discovery)
	checks, passed := checkManifestAndBundles("", "")
	if !passed {
		t.Errorf("expected checkManifestAndBundles to pass (non-fatal WARN) before build, got passed=false")
	}

	foundManifestWarn := false
	for _, c := range checks {
		if c.Name == "Manifest Schema Conformance" {
			t.Errorf("unexpected 'Manifest Schema Conformance' check found: status=%s message=%s (workflow.json was erroneously treated as deployment manifest)", c.Status, c.Message)
		}
		if c.Name == "Deployment Manifest" {
			if c.Status != StatusWarn {
				t.Errorf("expected Deployment Manifest status WARN, got %s", c.Status)
			}
			if !strings.Contains(c.Remediation, "runtime build") {
				t.Errorf("expected remediation to mention 'runtime build', got %q", c.Remediation)
			}
			foundManifestWarn = true
		}
	}
	if !foundManifestWarn {
		t.Errorf("expected 'Deployment Manifest' check with status WARN in doctor results")
	}

	// 3. Verify RunDoctorChecks does not report Manifest Schema Conformance failure
	cfg := Config{
		APIURL: "http://127.0.0.1:8080",
		Env:    "development",
	}
	report := RunDoctorChecks(cfg, "", "")
	for _, c := range report.Checks {
		if c.Name == "Manifest Schema Conformance" && c.Status == StatusFail {
			t.Errorf("RunDoctorChecks reported critical schema conformance failure on fresh init: %s", c.Message)
		}
	}
}

func TestDoctorExplicitInvalidManifestFails(t *testing.T) {
	tmpDir := t.TempDir()

	// Create an invalid manifest file
	invalidManifestPath := filepath.Join(tmpDir, "invalid-manifest.json")
	invalidContent := `{"manifestVersion": 999, "notAValidField": true}`
	if err := os.WriteFile(invalidManifestPath, []byte(invalidContent), 0644); err != nil {
		t.Fatalf("failed to write invalid manifest: %v", err)
	}

	// Explicit path should fail validation
	checks, passed := checkManifestAndBundles(invalidManifestPath, "")
	if passed {
		t.Errorf("expected checkManifestAndBundles to fail with invalid manifest path, got passed=true")
	}

	foundSchemaFailure := false
	for _, c := range checks {
		if c.Name == "Manifest Schema Conformance" && c.Status == StatusFail {
			foundSchemaFailure = true
			if !strings.Contains(c.Message, "deployment schema validation") {
				t.Errorf("expected failure message to mention schema validation, got %s", c.Message)
			}
		}
	}
	if !foundSchemaFailure {
		t.Errorf("expected 'Manifest Schema Conformance' check with StatusFail for invalid manifest")
	}

	// Auto-discovery of invalid manifest in dist/manifest.json should also fail
	distDir := filepath.Join(tmpDir, "dist")
	if err := os.MkdirAll(distDir, 0755); err != nil {
		t.Fatalf("failed to create dist dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(distDir, "manifest.json"), []byte(invalidContent), 0644); err != nil {
		t.Fatalf("failed to write dist/manifest.json: %v", err)
	}

	origWd, _ := os.Getwd()
	defer os.Chdir(origWd)
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to tmpDir: %v", err)
	}

	checksAuto, passedAuto := checkManifestAndBundles("", "")
	if passedAuto {
		t.Errorf("expected checkManifestAndBundles to fail for invalid dist/manifest.json, got passed=true")
	}
	foundAutoFail := false
	for _, c := range checksAuto {
		if c.Name == "Manifest Schema Conformance" && c.Status == StatusFail {
			foundAutoFail = true
		}
	}
	if !foundAutoFail {
		t.Errorf("expected schema failure when dist/manifest.json is invalid")
	}
}

func TestDeployRejectsFreshInitWithoutBuild(t *testing.T) {
	tmpDir := t.TempDir()

	err := InitProject(tmpDir, "fresh-deploy", false)
	if err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	origWd, _ := os.Getwd()
	defer os.Chdir(origWd)
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to tmpDir: %v", err)
	}

	// HandleDeploy without --manifest in a fresh project should fail with actionable message to run runtime build
	err = HandleDeploy([]string{"--env", "development"})
	if err == nil {
		t.Fatalf("expected HandleDeploy to fail when dist/manifest.json does not exist")
	}
	if !strings.Contains(err.Error(), "Run `runtime build` first") {
		t.Errorf("expected error to instruct running runtime build, got %v", err)
	}
}

func TestPackagedStandaloneComposePlatformContract(t *testing.T) {
	// 1. Locate repository root
	repoRoot, err := filepath.Abs("../../")
	if err != nil {
		t.Fatalf("failed to resolve repo root: %v", err)
	}

	standaloneComposePath := filepath.Join(repoRoot, "deploy", "compose", "standalone-compose.yaml")
	composeBytes, err := os.ReadFile(standaloneComposePath)
	if err != nil {
		t.Fatalf("failed to read standalone-compose.yaml: %v", err)
	}
	composeStr := string(composeBytes)

	// Verify static contract declarations:
	// - control-plane MUST specify platform: linux/amd64
	// - control-plane image MUST be pinned to release.json immutable controlPlaneImage
	// - postgres, nats, minio MUST NOT specify platform (preserving native multi-arch host compatibility)
	if !strings.Contains(composeStr, "platform: linux/amd64") {
		t.Errorf("standalone-compose.yaml must declare 'platform: linux/amd64' for control-plane")
	}
	if !strings.Contains(composeStr, "${DEADBOLT_CONTROL_PLANE_IMAGE:") {
		t.Errorf("standalone-compose.yaml must interpolate DEADBOLT_CONTROL_PLANE_IMAGE")
	}

	// 2. Test packaging flow with scripts/package-cli-distribution.sh
	pkgDir := t.TempDir()
	dummyBin := filepath.Join(pkgDir, "dummy-runtime")
	if err := os.WriteFile(dummyBin, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatalf("failed to create dummy binary: %v", err)
	}

	mockDigest := "ghcr.io/ryanakml/deadbolt/control-plane@sha256:1111222233334444555566667777888899990000aaaaabbbbbcccccdddddeeeee"
	outDir := filepath.Join(pkgDir, "dist")
	scriptPath := filepath.Join(repoRoot, "scripts", "package-cli-distribution.sh")

	cmd := exec.Command(scriptPath, dummyBin, mockDigest, outDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("package-cli-distribution.sh failed: %v\n%s", err, string(out))
	}

	// 3. Verify packaged companion asset share/deadbolt/compose.yaml
	packagedComposePath := filepath.Join(outDir, "share", "deadbolt", "compose.yaml")
	packagedBytes, err := os.ReadFile(packagedComposePath)
	if err != nil {
		t.Fatalf("failed to read packaged compose.yaml: %v", err)
	}
	packagedStr := string(packagedBytes)

	if !strings.Contains(packagedStr, "platform: linux/amd64") {
		t.Errorf("packaged compose.yaml missing 'platform: linux/amd64' for control-plane")
	}

	// 4. Validate with docker compose config if docker is installed
	if _, err := exec.LookPath("docker"); err == nil {
		composeCmd := exec.Command("docker", "compose", "-f", packagedComposePath, "config", "--format", "json")
		composeCmd.Env = append(os.Environ(), "DEADBOLT_CONTROL_PLANE_IMAGE="+mockDigest)
		jsonOut, err := composeCmd.CombinedOutput()
		if err != nil {
			t.Fatalf("docker compose config validation on packaged compose.yaml failed: %v\n%s", err, string(jsonOut))
		}

		var parsed struct {
			Services map[string]struct {
				Platform string `json:"platform"`
				Image    string `json:"image"`
			} `json:"services"`
		}
		if err := json.Unmarshal(jsonOut, &parsed); err != nil {
			t.Fatalf("failed to parse docker compose config json: %v", err)
		}

		// Control-plane must be explicitly pinned to linux/amd64
		cp, ok := parsed.Services["control-plane"]
		if !ok {
			t.Fatalf("packaged compose.yaml missing control-plane service")
		}
		if cp.Platform != "linux/amd64" {
			t.Errorf("expected control-plane platform 'linux/amd64', got %q", cp.Platform)
		}
		if cp.Image != mockDigest {
			t.Errorf("expected control-plane image %q, got %q", mockDigest, cp.Image)
		}

		// Postgres, NATS, and MinIO must NOT specify platform (preserving native host architecture e.g. arm64 on Apple Silicon)
		for _, svc := range []string{"postgres", "nats", "minio"} {
			s, ok := parsed.Services[svc]
			if !ok {
				t.Fatalf("packaged compose.yaml missing service %q", svc)
			}
			if s.Platform != "" {
				t.Errorf("service %q should not specify platform override (expected native, got %q)", svc, s.Platform)
			}
		}
	}
}
