package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var (
	ErrCredentialNotFound = errors.New("credential not found")
	ErrCredentialBackend  = errors.New("credential backend failure")
	customCredentialsDir  string
	credentialsMu         sync.Mutex
	// credentialFault injects failures into credential operations.
	// Test-only hook for failure-atomicity coverage; always nil in production.
	credentialFault func(service, account, op string) error
	// credentialOS, credentialToolExists and runCredentialCommand are narrow
	// test seams over OS identity and native command execution. Production
	// always uses runtime.GOOS, exec.LookPath and real process execution.
	credentialOS         = runtime.GOOS
	credentialToolExists = func(name string) bool { _, err := exec.LookPath(name); return err == nil }
	runCredentialCommand = func(name string, args ...string) (stdout, stderr []byte, err error) {
		cmd := exec.Command(name, args...)
		var so, se bytes.Buffer
		cmd.Stdout, cmd.Stderr = &so, &se
		err = cmd.Run()
		return so.Bytes(), se.Bytes(), err
	}
)

// SetCustomCredentialsDir overrides the storage path for credentials (useful for testing).
func SetCustomCredentialsDir(dir string) {
	credentialsMu.Lock()
	defer credentialsMu.Unlock()
	customCredentialsDir = dir
}

// GetCredentialsDirectory returns the path to the credentials directory
func GetCredentialsDirectory() (string, error) {
	return getCredentialsDir()
}

func getCredentialsDir() (string, error) {
	credentialsMu.Lock()
	defer credentialsMu.Unlock()
	if customCredentialsDir != "" {
		return customCredentialsDir, nil
	}
	if envDir := os.Getenv("DEADBOLT_CREDENTIALS_DIR"); envDir != "" {
		return envDir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".deadbolt"), nil
}

func getCredentialsFilePath() (string, error) {
	dir, err := getCredentialsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "credentials.json"), nil
}

// StoreCredential saves a credential to the native OS keychain. File storage is
// deliberately restricted to explicitly configured test/local-fixture directories.
func StoreCredential(service, account, secret string) error {
	if credentialFault != nil {
		if err := credentialFault(service, account, "store"); err != nil {
			return err
		}
	}
	// If custom directory or test environment is set, use secure file directly to avoid polluting system keychain
	if customCredentialsDir != "" || os.Getenv("DEADBOLT_CREDENTIALS_DIR") != "" {
		return storeFileCredential(service, account, secret)
	}

	if runtime.GOOS == "darwin" {
		cmd := exec.Command("security", "add-generic-password", "-s", service, "-a", account, "-w", secret, "-U")
		err := cmd.Run()
		if err == nil {
			return nil
		}
		return fmt.Errorf("macOS Keychain is unavailable; hosted credentials are not written to disk: %w", err)
	} else if runtime.GOOS == "linux" {
		if _, err := exec.LookPath("secret-tool"); err == nil {
			cmd := exec.Command("secret-tool", "store", "--label="+service, "service", service, "account", account)
			cmd.Stdin = strings.NewReader(secret)
			if err := cmd.Run(); err == nil {
				return nil
			}
			return fmt.Errorf("Linux Secret Service is unavailable; hosted credentials are not written to disk")
		}
		return fmt.Errorf("secret-tool is required for hosted credentials on Linux; install a Secret Service provider")
	}

	return fmt.Errorf("no supported native credential store for %s", runtime.GOOS)
}

// GetCredential retrieves a credential from the OS keychain or fallback secure storage.
// Genuine absence returns ErrCredentialNotFound; any backend/read failure
// returns a distinct error so callers never mistake an unknown prior state
// for absence.
func GetCredential(service, account string) (string, error) {
	if credentialFault != nil {
		if err := credentialFault(service, account, "get"); err != nil {
			return "", err
		}
	}
	if customCredentialsDir != "" || os.Getenv("DEADBOLT_CREDENTIALS_DIR") != "" {
		return getFileCredential(service, account)
	}

	switch credentialOS {
	case "darwin":
		stdout, stderr, err := runCredentialCommand("security", "find-generic-password", "-s", service, "-a", account, "-w")
		if err == nil {
			return strings.TrimSpace(string(stdout)), nil
		}
		// The `security` tool reports a missing item on stderr, which
		// Cmd.Output() never returns; both streams are classified.
		if isKeychainNotFoundOutput(string(stdout) + "\n" + string(stderr)) {
			return "", ErrCredentialNotFound
		}
		return "", fmt.Errorf("%w: macOS Keychain lookup failed: %v", ErrCredentialBackend, err)
	case "linux":
		if !credentialToolExists("secret-tool") {
			return "", fmt.Errorf("%w: secret-tool is required for hosted credentials on Linux; install a Secret Service provider", ErrCredentialBackend)
		}
		stdout, stderr, err := runCredentialCommand("secret-tool", "lookup", "service", service, "account", account)
		combined := string(stdout) + "\n" + string(stderr)
		if err == nil {
			if len(bytes.TrimSpace(stdout)) == 0 {
				return "", ErrCredentialNotFound
			}
			return strings.TrimSpace(string(stdout)), nil
		}
		if isSecretToolNotFoundOutput(combined) {
			return "", ErrCredentialNotFound
		}
		return "", fmt.Errorf("%w: Linux Secret Service lookup failed: %v", ErrCredentialBackend, err)
	default:
		return "", fmt.Errorf("%w: no supported native credential store for %s", ErrCredentialBackend, credentialOS)
	}
}

// DeleteCredential removes a credential from the OS keychain or fallback secure storage.
// Genuine absence is success; real deletion failures are reported instead of
// being silently swallowed. There is no silent-success fallthrough: an
// unavailable backend or unsupported OS is an error.
func DeleteCredential(service, account string) error {
	if credentialFault != nil {
		if err := credentialFault(service, account, "delete"); err != nil {
			return err
		}
	}
	if customCredentialsDir != "" || os.Getenv("DEADBOLT_CREDENTIALS_DIR") != "" {
		return deleteFileCredential(service, account)
	}

	switch credentialOS {
	case "darwin":
		stdout, stderr, err := runCredentialCommand("security", "delete-generic-password", "-s", service, "-a", account)
		if err == nil {
			return nil
		}
		if credentialAbsent(service, account) {
			return nil
		}
		return fmt.Errorf("%w: delete credential from macOS Keychain: %v: %s", ErrCredentialBackend, err, strings.TrimSpace(string(stdout)+"\n"+string(stderr)))
	case "linux":
		if !credentialToolExists("secret-tool") {
			return fmt.Errorf("%w: secret-tool is required to delete hosted credentials on Linux", ErrCredentialBackend)
		}
		stdout, stderr, err := runCredentialCommand("secret-tool", "clear", "service", service, "account", account)
		if err == nil {
			return nil
		}
		if credentialAbsent(service, account) {
			return nil
		}
		return fmt.Errorf("%w: delete credential from Linux Secret Service: %v: %s", ErrCredentialBackend, err, strings.TrimSpace(string(stdout)+"\n"+string(stderr)))
	default:
		return fmt.Errorf("%w: no supported native credential store for %s", ErrCredentialBackend, credentialOS)
	}
}

// credentialAbsent reports whether no credential exists for service/account.
// Only positive evidence of absence counts: ambiguous lookup failures return
// false so the original deletion error is reported fail-closed.
func credentialAbsent(service, account string) bool {
	_, err := GetCredential(service, account)
	return err != nil && errors.Is(err, ErrCredentialNotFound)
}

// credentialMutation is one ordered credential write: store value, or remove
// the credential when remove is true. desc names the human context for
// error messages without ever carrying secret values.
type credentialMutation struct {
	account string
	value   string
	remove  bool
	desc    string
}

// applyCredentialMutations snapshots every touched account first (aborting on
// unknown prior state), applies writes in the given deterministic order, and
// restores all priors if any write fails. It never reports partial success.
func applyCredentialMutations(muts []credentialMutation) error {
	snaps := make(map[string]credentialSnapshot, len(muts))
	for _, m := range muts {
		if _, ok := snaps[m.account]; ok {
			continue
		}
		shot, err := snapshotCredential("deadbolt", m.account)
		if err != nil {
			return fmt.Errorf("snapshot credential context: %w", err)
		}
		snaps[m.account] = shot
	}
	for _, m := range muts {
		var err error
		verb := "store"
		if m.remove {
			verb = "clear"
			err = DeleteCredential("deadbolt", m.account)
		} else {
			err = StoreCredential("deadbolt", m.account, m.value)
		}
		if err != nil {
			var firstErr error
			for _, r := range muts {
				if e := restoreCredential("deadbolt", r.account, snaps[r.account]); e != nil && firstErr == nil {
					firstErr = e
				}
			}
			if firstErr != nil {
				return fmt.Errorf("%s %s: %v (rollback: %v)", verb, m.desc, err, firstErr)
			}
			return fmt.Errorf("%s %s: %w", verb, m.desc, err)
		}
	}
	return nil
}

// isKeychainNotFoundOutput recognizes macOS `security` absence output.
func isKeychainNotFoundOutput(out string) bool {
	return strings.Contains(out, "could not be found")
}

// isSecretToolNotFoundOutput recognizes Secret Service absence output.
func isSecretToolNotFoundOutput(out string) bool {
	return strings.Contains(strings.ToLower(out), "no such")
}

func storeFileCredential(service, account, secret string) error {
	dir, err := getCredentialsDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create credentials dir: %w", err)
	}

	filePath, err := getCredentialsFilePath()
	if err != nil {
		return err
	}

	creds := make(map[string]string)
	if data, err := os.ReadFile(filePath); err == nil {
		_ = json.Unmarshal(data, &creds)
	}

	key := fmt.Sprintf("%s:%s", service, account)
	creds[key] = secret

	data, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal credentials: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0o600); err != nil {
		return err
	}
	// os.WriteFile does not tighten permissions when the file already exists.
	return os.Chmod(filePath, 0o600)
}

func getFileCredential(service, account string) (string, error) {
	filePath, err := getCredentialsFilePath()
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrCredentialNotFound
		}
		return "", err
	}

	// Validate permissions on POSIX
	if runtime.GOOS != "windows" {
		if fi, err := os.Stat(filePath); err == nil {
			if fi.Mode().Perm()&0o077 != 0 {
				_ = os.Chmod(filePath, 0o600)
			}
		}
	}

	var creds map[string]string
	if err := json.Unmarshal(data, &creds); err != nil {
		return "", fmt.Errorf("unmarshal credentials: %w", err)
	}

	key := fmt.Sprintf("%s:%s", service, account)
	val, ok := creds[key]
	if !ok || val == "" {
		return "", ErrCredentialNotFound
	}

	return val, nil
}

func deleteFileCredential(service, account string) error {
	filePath, err := getCredentialsFilePath()
	if err != nil {
		return err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var creds map[string]string
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil
	}

	key := fmt.Sprintf("%s:%s", service, account)
	delete(creds, key)

	updated, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, updated, 0o600)
}
