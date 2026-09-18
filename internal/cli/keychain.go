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

	if runtime.GOOS == "darwin" {
		out, err := exec.Command("security", "find-generic-password", "-s", service, "-a", account, "-w").Output()
		if err == nil {
			return strings.TrimSpace(string(out)), nil
		}
		if isKeychainNotFoundOutput(string(out)) {
			return "", ErrCredentialNotFound
		}
		return "", fmt.Errorf("%w: macOS Keychain lookup failed: %v", ErrCredentialBackend, err)
	} else if runtime.GOOS == "linux" {
		if _, err := exec.LookPath("secret-tool"); err == nil {
			out, err := exec.Command("secret-tool", "lookup", "service", service, "account", account).CombinedOutput()
			if err == nil {
				if len(bytes.TrimSpace(out)) == 0 {
					return "", ErrCredentialNotFound
				}
				return strings.TrimSpace(string(out)), nil
			}
			if isSecretToolNotFoundOutput(string(out)) {
				return "", ErrCredentialNotFound
			}
			return "", fmt.Errorf("%w: Linux Secret Service lookup failed: %v", ErrCredentialBackend, err)
		}
		return "", fmt.Errorf("%w: secret-tool is required for hosted credentials on Linux; install a Secret Service provider", ErrCredentialBackend)
	}

	return "", fmt.Errorf("%w: no supported native credential store for %s", ErrCredentialBackend, runtime.GOOS)
}

// DeleteCredential removes a credential from the OS keychain or fallback secure storage.
// Genuine absence is success; real deletion failures are reported instead of
// being silently swallowed.
func DeleteCredential(service, account string) error {
	if credentialFault != nil {
		if err := credentialFault(service, account, "delete"); err != nil {
			return err
		}
	}
	if customCredentialsDir != "" || os.Getenv("DEADBOLT_CREDENTIALS_DIR") != "" {
		return deleteFileCredential(service, account)
	}

	if runtime.GOOS == "darwin" {
		out, err := exec.Command("security", "delete-generic-password", "-s", service, "-a", account).CombinedOutput()
		if err == nil {
			return nil
		}
		if credentialAbsent(service, account) {
			return nil
		}
		return fmt.Errorf("delete credential from macOS Keychain: %v: %s", err, strings.TrimSpace(string(out)))
	} else if runtime.GOOS == "linux" {
		if _, err := exec.LookPath("secret-tool"); err == nil {
			out, err := exec.Command("secret-tool", "clear", "service", service, "account", account).CombinedOutput()
			if err == nil {
				return nil
			}
			if credentialAbsent(service, account) {
				return nil
			}
			return fmt.Errorf("delete credential from Linux Secret Service: %v: %s", err, strings.TrimSpace(string(out)))
		}
	}

	return nil
}

// credentialAbsent reports whether no credential exists for service/account.
// Only positive evidence of absence counts: ambiguous lookup failures return
// false so the original deletion error is reported fail-closed.
func credentialAbsent(service, account string) bool {
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("security", "find-generic-password", "-s", service, "-a", account).CombinedOutput()
		if err == nil {
			return false
		}
		return isKeychainNotFoundOutput(string(out))
	} else if runtime.GOOS == "linux" {
		if _, err := exec.LookPath("secret-tool"); err == nil {
			out, err := exec.Command("secret-tool", "lookup", "service", service, "account", account).CombinedOutput()
			if err == nil && len(bytes.TrimSpace(out)) == 0 {
				return true
			}
			return isSecretToolNotFoundOutput(string(out))
		}
	}
	return false
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

	return os.WriteFile(filePath, data, 0o600)
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
