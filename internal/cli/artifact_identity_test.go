package cli

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// 1. same bytes + same target => same digest
func TestArtifactIdentity_SameBytesSameTarget_SameDigest(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "identity-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}

	res1, err := BuildDeployment(opts)
	if err != nil {
		t.Fatalf("first build failed: %v", err)
	}

	res2, err := BuildDeployment(opts)
	if err != nil {
		t.Fatalf("second build failed: %v", err)
	}

	if res1.BundleDigest != res2.BundleDigest {
		t.Fatalf("expected identical bundleDigest for same bytes and same target, got %s vs %s", res1.BundleDigest, res2.BundleDigest)
	}

	tar1, err := os.ReadFile(res1.BundlePath)
	if err != nil {
		t.Fatalf("read bundle 1: %v", err)
	}
	tar2, err := os.ReadFile(res2.BundlePath)
	if err != nil {
		t.Fatalf("read bundle 2: %v", err)
	}
	if !bytes.Equal(tar1, tar2) {
		t.Fatalf("expected byte-for-byte identical tar archives for same bytes and target")
	}
}

// 2. same bytes + amd64 vs arm64 => different digest, and same bytes + different OS => different digest
func TestArtifactIdentity_ArchitectureVariance_DifferentDigest(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "arch-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	optsAmd64 := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist-amd64"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}
	resAmd64, err := BuildDeployment(optsAmd64)
	if err != nil {
		t.Fatalf("build amd64 failed: %v", err)
	}

	optsArm64 := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist-arm64"),
		TargetArch: "arm64",
		TargetOS:   "linux",
	}
	resArm64, err := BuildDeployment(optsArm64)
	if err != nil {
		t.Fatalf("build arm64 failed: %v", err)
	}

	if resAmd64.BundleDigest == resArm64.BundleDigest {
		t.Fatalf("expected different bundleDigest for amd64 vs arm64 on same source, but got identical %s", resAmd64.BundleDigest)
	}

	// Verify different target OS also produces different canonical platform bytes and digest
	platLinux, err := worker.CanonicalPlatformBytes("linux", "arm64")
	if err != nil {
		t.Fatalf("canonical platform linux: %v", err)
	}
	platDarwin, err := worker.CanonicalPlatformBytes("darwin", "arm64")
	if err != nil {
		t.Fatalf("canonical platform darwin: %v", err)
	}
	if bytes.Equal(platLinux, platDarwin) {
		t.Fatalf("expected canonical platform bytes to differ across target OS")
	}

	// Same source + different target OS must produce different bundle digests
	// at the artifact layer. This goes through buildBundleTar directly because
	// the deployment schema constrains deployable manifests to linux; the
	// identity mechanism itself must still separate OS variants.
	taskFiles := map[string][]byte{
		"tasks/validate.js": []byte("export default async function task(input) { return {}; }\n"),
	}
	tarLinux, err := buildBundleTar(taskFiles, "linux", "arm64")
	if err != nil {
		t.Fatalf("buildBundleTar linux: %v", err)
	}
	tarDarwin, err := buildBundleTar(taskFiles, "darwin", "arm64")
	if err != nil {
		t.Fatalf("buildBundleTar darwin: %v", err)
	}
	digestLinux := sha256.Sum256(tarLinux)
	digestDarwin := sha256.Sum256(tarDarwin)
	if hex.EncodeToString(digestLinux[:]) == hex.EncodeToString(digestDarwin[:]) {
		t.Fatalf("expected different bundle digests for linux vs darwin on same source")
	}

	// Helper itself is deterministic for a fixed source + target
	tarLinuxAgain, err := buildBundleTar(taskFiles, "linux", "arm64")
	if err != nil {
		t.Fatalf("buildBundleTar linux again: %v", err)
	}
	if !bytes.Equal(tarLinux, tarLinuxAgain) {
		t.Fatalf("expected byte-for-byte identical tar archives for same source and target")
	}
}

// 3. deterministic rebuild across restored identical source and target
func TestArtifactIdentity_DeterministicRebuild(t *testing.T) {
	dirA := t.TempDir()
	if err := InitProject(dirA, "rebuild-service", false); err != nil {
		t.Fatalf("InitProject dirA failed: %v", err)
	}

	dirB := t.TempDir()
	if err := InitProject(dirB, "rebuild-service", false); err != nil {
		t.Fatalf("InitProject dirB failed: %v", err)
	}

	optsA := BuildOptions{
		ProjectDir: dirA,
		BundleDir:  filepath.Join(dirA, "bundles"),
		OutputDir:  filepath.Join(dirA, "dist"),
		TargetArch: "arm64",
		TargetOS:   "linux",
	}
	resA, err := BuildDeployment(optsA)
	if err != nil {
		t.Fatalf("build dirA failed: %v", err)
	}

	optsB := BuildOptions{
		ProjectDir: dirB,
		BundleDir:  filepath.Join(dirB, "bundles"),
		OutputDir:  filepath.Join(dirB, "dist"),
		TargetArch: "arm64",
		TargetOS:   "linux",
	}
	resB, err := BuildDeployment(optsB)
	if err != nil {
		t.Fatalf("build dirB failed: %v", err)
	}

	if resA.BundleDigest != resB.BundleDigest {
		t.Fatalf("rebuild in clean identical directory produced different digest: %s vs %s", resA.BundleDigest, resB.BundleDigest)
	}

	// Verify archive internal properties: fixed epoch timestamp, 0644 mode, sorted entries with platform.json first
	f, err := os.Open(resA.BundlePath)
	if err != nil {
		t.Fatalf("open bundle: %v", err)
	}
	defer f.Close()

	tr := tar.NewReader(f)
	var entryNames []string
	expectedTime := time.Unix(1700000000, 0).UTC()

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar header: %v", err)
		}
		entryNames = append(entryNames, hdr.Name)
		if hdr.Mode != 0o644 {
			t.Errorf("entry %s had mode %o, expected 0644", hdr.Name, hdr.Mode)
		}
		if !hdr.ModTime.Equal(expectedTime) {
			t.Errorf("entry %s had modTime %v, expected %v", hdr.Name, hdr.ModTime, expectedTime)
		}
	}

	if len(entryNames) == 0 || entryNames[0] != worker.BundlePlatformPath {
		t.Fatalf("expected first entry in tar archive to be %q, got %v", worker.BundlePlatformPath, entryNames)
	}
}

// 4. manifest target matches the target encoded into bundle identity
func TestArtifactIdentity_ManifestTargetMatchesBundleIdentity(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "match-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "arm64",
		TargetOS:   "linux",
	}
	res, err := BuildDeployment(opts)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	// Read manifest
	manifestBytes, err := os.ReadFile(res.ManifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest struct {
		TargetOS           string `json:"targetOS"`
		TargetArchitecture string `json:"targetArchitecture"`
		BundleDigest       string `json:"bundleDigest"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}

	if manifest.TargetArchitecture != "arm64" || manifest.TargetOS != "linux" {
		t.Fatalf("unexpected manifest target: %s/%s", manifest.TargetOS, manifest.TargetArchitecture)
	}

	// Read bundle platform metadata directly from tar
	bundlePlatform, err := worker.ReadBundlePlatformFromFile(res.BundlePath)
	if err != nil {
		t.Fatalf("read bundle platform from tar failed: %v", err)
	}

	if bundlePlatform.TargetArchitecture != manifest.TargetArchitecture {
		t.Fatalf("bundle internal target architecture %q does not match manifest %q", bundlePlatform.TargetArchitecture, manifest.TargetArchitecture)
	}
	if bundlePlatform.TargetOS != manifest.TargetOS {
		t.Fatalf("bundle internal target OS %q does not match manifest %q", bundlePlatform.TargetOS, manifest.TargetOS)
	}
}

// 5. fresh local build default is compatible with the host architecture contract
func TestArtifactIdentity_FreshLocalBuildDefaultCompatibleWithHost(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "fresh-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	// HandleBuild without --arch flag simulates `runtime build`
	if err := HandleBuild([]string{"--dir", tmpDir}); err != nil {
		t.Fatalf("runtime build without --arch failed: %v", err)
	}

	manifestBytes, err := os.ReadFile(filepath.Join(tmpDir, "dist", "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest struct {
		TargetArchitecture string `json:"targetArchitecture"`
		TargetOS           string `json:"targetOS"`
		BundleDigest       string `json:"bundleDigest"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}

	expectedArch := defaultArchitecture()
	if manifest.TargetArchitecture != expectedArch {
		t.Fatalf("expected fresh build to default to host arch %q, got %q", expectedArch, manifest.TargetArchitecture)
	}

	// VerifyArchitecture must accept the default build on this host
	if err := worker.VerifyArchitecture(manifest.TargetArchitecture, ""); err != nil {
		t.Fatalf("worker.VerifyArchitecture rejected default fresh build target %q on host (%s): %v",
			manifest.TargetArchitecture, worker.CurrentHostArchitecture(), err)
	}

	// Apple Silicon host verification contract
	if runtime.GOARCH == "arm64" {
		if manifest.TargetArchitecture != "arm64" {
			t.Fatalf("Apple Silicon host must default to arm64, got %q", manifest.TargetArchitecture)
		}
	}

	// The real build output must pass the real fail-closed worker preflight:
	// embedded identity equals the manifest target and the host accepts it.
	bundlePath := filepath.Join(tmpDir, "bundles", manifest.BundleDigest+".tar")
	supervisor := worker.NewProcessSupervisor("definitely-not-node", "ignored")
	supervisor.LeaseTracker = worker.NewLeaseTracker(time.Now().Add(time.Minute), 0, 0)
	supervisor.StartAckFn = func(context.Context, string, int64) error { return nil }
	started := false
	supervisor.OnProcessStart = func(int) { started = true }
	_, _, err = supervisor.ExecuteAttempt(context.Background(), &worker.TaskInput{
		AttemptID:  "fresh-build-preflight",
		Entrypoint: "tasks/validate.js",
		Bundle: &worker.BundleSpec{
			Path:       bundlePath,
			SHA256:     manifest.BundleDigest,
			TargetArch: manifest.TargetArchitecture,
			TargetOS:   manifest.TargetOS,
			Entrypoint: "tasks/validate.js",
		},
	}, 1)
	if err == nil || !strings.Contains(err.Error(), "start runner") || started {
		t.Fatalf("expected fresh build bundle to pass preflight and reach process start, got err=%v started=%v", err, started)
	}
}

// 6. deploying two platform variants no longer collides on bundleDigest because they are distinct immutable artifacts
func TestArtifactIdentity_DeployPlatformVariants_NoBundleDigestCollision(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "variant-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	resAmd64, err := BuildDeployment(BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist-amd64"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	})
	if err != nil {
		t.Fatalf("build amd64: %v", err)
	}

	resArm64, err := BuildDeployment(BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist-arm64"),
		TargetArch: "arm64",
		TargetOS:   "linux",
	})
	if err != nil {
		t.Fatalf("build arm64: %v", err)
	}

	if resAmd64.BundleDigest == resArm64.BundleDigest {
		t.Fatalf("expected distinct bundle digests for amd64 and arm64, got %s", resAmd64.BundleDigest)
	}

	// Simulate control plane immutability tracking per environment
	envDeployments := make(map[string]string) // bundleDigest -> manifestHash

	registerManifest := func(manifestPath string) error {
		data, err := os.ReadFile(manifestPath)
		if err != nil {
			return err
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		bDigest := m["bundleDigest"].(string)
		mHash := "hash-of-" + string(data)

		if existingHash, exists := envDeployments[bDigest]; exists {
			if existingHash != mHash {
				return worker.ErrBundleDigestMismatch // 409 IMMUTABLE_CONTENT_CONFLICT simulation
			}
			return nil // idempotent replay
		}
		envDeployments[bDigest] = mHash
		return nil
	}

	// Deploy amd64 variant
	if err := registerManifest(resAmd64.ManifestPath); err != nil {
		t.Fatalf("deploying amd64 variant failed: %v", err)
	}

	// Deploy arm64 variant: must succeed without IMMUTABLE_CONTENT_CONFLICT because bundleDigest is distinct
	if err := registerManifest(resArm64.ManifestPath); err != nil {
		t.Fatalf("deploying arm64 variant failed (collision on bundleDigest): %v", err)
	}

	if len(envDeployments) != 2 {
		t.Fatalf("expected 2 distinct deployment records, got %d", len(envDeployments))
	}
}
