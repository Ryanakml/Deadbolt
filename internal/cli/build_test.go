package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildContract_SelfContainedJS(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "valid-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
		TargetArch: "amd64",
		TargetOS:   "linux",
	}

	res, err := BuildDeployment(opts)
	if err != nil {
		t.Fatalf("BuildDeployment failed: %v", err)
	}
	if res.BundleDigest == "" {
		t.Errorf("expected non-empty BundleDigest")
	}
	if res.DependencyLockDigest == "" {
		t.Errorf("expected non-empty DependencyLockDigest")
	}
}

func TestBuildContract_MissingLockMaterial(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "no-lock-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}
	_ = os.Remove(filepath.Join(tmpDir, "package-lock.json"))
	_ = os.Remove(filepath.Join(tmpDir, "pnpm-lock.yaml"))

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
	}

	_, err := BuildDeployment(opts)
	if err == nil {
		t.Fatalf("expected build to fail when lock material is missing")
	}
	if !strings.Contains(err.Error(), "dependency lockfile is required") {
		t.Errorf("expected error to mention required lockfile, got: %v", err)
	}
}

func TestBuildContract_RawTS(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "ts-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}
	tsPath := filepath.Join(tmpDir, "tasks", "handler.ts")
	if err := os.WriteFile(tsPath, []byte("export const run = () => {};"), 0o644); err != nil {
		t.Fatalf("write ts file: %v", err)
	}

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
	}

	_, err := BuildDeployment(opts)
	if err == nil {
		t.Fatalf("expected build to fail on raw TypeScript")
	}
	if !strings.Contains(err.Error(), "TypeScript") {
		t.Errorf("expected error to mention TypeScript rejection, got: %v", err)
	}
}

func TestBuildContract_MissingWorkflow(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "missing-wf-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}
	_ = os.Remove(filepath.Join(tmpDir, "workflow.json"))

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
	}

	_, err := BuildDeployment(opts)
	if err == nil {
		t.Fatalf("expected build to fail when workflow definition is missing")
	}
	if !strings.Contains(err.Error(), "workflow definition is required") {
		t.Errorf("expected error about workflow definition, got: %v", err)
	}
}

func TestBuildContract_MissingTaskDefinitions(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "missing-tasks-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}
	_ = os.Remove(filepath.Join(tmpDir, "tasks.json"))

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
	}

	_, err := BuildDeployment(opts)
	if err == nil {
		t.Fatalf("expected build to fail when tasks.json is missing")
	}
	if !strings.Contains(err.Error(), "task definition file is required") {
		t.Errorf("expected error about task definition file, got: %v", err)
	}
}

func TestBuildContract_MissingEntrypoint(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "missing-entry-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}
	badTasks := `[{"name":"ghost","entrypoint":"tasks/nonexistent.js","recovery":"safe","timeoutMs":1000,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"}}]`
	if err := os.WriteFile(filepath.Join(tmpDir, "tasks.json"), []byte(badTasks), 0o644); err != nil {
		t.Fatalf("write bad tasks.json: %v", err)
	}

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
	}

	_, err := BuildDeployment(opts)
	if err == nil {
		t.Fatalf("expected build to fail when task entrypoint does not exist")
	}
	if !strings.Contains(err.Error(), "not found in project") {
		t.Errorf("expected error about entrypoint not found, got: %v", err)
	}
}

func TestBuildContract_UnsupportedExternalDependency(t *testing.T) {
	tmpDir := t.TempDir()
	if err := InitProject(tmpDir, "ext-dep-service", false); err != nil {
		t.Fatalf("InitProject failed: %v", err)
	}
	taskWithImport := `import axios from "axios";
export default async function task(input, ctx) { return {}; }
`
	if err := os.WriteFile(filepath.Join(tmpDir, "tasks", "validate.js"), []byte(taskWithImport), 0o644); err != nil {
		t.Fatalf("write task with external import: %v", err)
	}

	opts := BuildOptions{
		ProjectDir: tmpDir,
		BundleDir:  filepath.Join(tmpDir, "bundles"),
		OutputDir:  filepath.Join(tmpDir, "dist"),
	}

	_, err := BuildDeployment(opts)
	if err == nil {
		t.Fatalf("expected build to fail when external dependency is imported")
	}
	if !strings.Contains(err.Error(), "imports external dependency \"axios\"") {
		t.Errorf("expected error about external dependency, got: %v", err)
	}

	taskWithRequire := `const lodash = require("lodash");
export default async function task(input, ctx) { return {}; }
`
	if err := os.WriteFile(filepath.Join(tmpDir, "tasks", "validate.js"), []byte(taskWithRequire), 0o644); err != nil {
		t.Fatalf("write task with external require: %v", err)
	}

	_, err = BuildDeployment(opts)
	if err == nil {
		t.Fatalf("expected build to fail when external dependency is required")
	}
	if !strings.Contains(err.Error(), "imports external dependency \"lodash\"") {
		t.Errorf("expected error about external dependency, got: %v", err)
	}
}
