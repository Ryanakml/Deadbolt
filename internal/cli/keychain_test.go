package cli

import (
	"errors"
	"testing"
)

// withNativeSeams installs stubbed OS identity, tool lookup and command
// execution, restoring production behavior afterwards.
func withNativeSeams(t *testing.T, os string, toolExists bool, run func(name string, args ...string) ([]byte, []byte, error)) {
	t.Helper()
	prevOS, prevTool, prevRun, prevFault := credentialOS, credentialToolExists, runCredentialCommand, credentialFault
	prevDir := customCredentialsDir
	credentialOS = os
	credentialToolExists = func(string) bool { return toolExists }
	runCredentialCommand = run
	credentialFault = nil
	customCredentialsDir = ""
	t.Setenv("DEADBOLT_CREDENTIALS_DIR", "")
	t.Cleanup(func() {
		credentialOS, credentialToolExists, runCredentialCommand, credentialFault = prevOS, prevTool, prevRun, prevFault
		customCredentialsDir = prevDir
	})
}

func exitErr() error { return errors.New("exit status 1") }

func TestNativeMacOSLookupClassification(t *testing.T) {
	// Present: value on stdout.
	withNativeSeams(t, "darwin", false, func(string, ...string) ([]byte, []byte, error) {
		return []byte("s3cr3t\n"), nil, nil
	})
	if v, err := GetCredential("svc", "acc"); err != nil || v != "s3cr3t" {
		t.Fatalf("present lookup: got %q (%v)", v, err)
	}

	// Absent: diagnostic arrives on stderr with empty stdout. A reader of
	// stdout alone would misclassify this as a backend failure.
	withNativeSeams(t, "darwin", false, func(string, ...string) ([]byte, []byte, error) {
		return nil, []byte("security: SecKeychainSearchCopyNext: The specified item could not be found in the keychain."), exitErr()
	})
	if _, err := GetCredential("svc", "acc"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("absent lookup must map to ErrCredentialNotFound, got %v", err)
	}

	// Backend failure: authorization diagnostic is not absence.
	withNativeSeams(t, "darwin", false, func(string, ...string) ([]byte, []byte, error) {
		return nil, []byte("security: SecKeychainItemModifyContent: auth failed."), exitErr()
	})
	if _, err := GetCredential("svc", "acc"); !errors.Is(err, ErrCredentialBackend) || errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("backend failure must map to ErrCredentialBackend only, got %v", err)
	}
}

func TestNativeMacOSDeleteClassification(t *testing.T) {
	// Success.
	withNativeSeams(t, "darwin", false, func(string, ...string) ([]byte, []byte, error) {
		return nil, nil, nil
	})
	if err := DeleteCredential("svc", "acc"); err != nil {
		t.Fatalf("successful delete: %v", err)
	}

	// Delete fails but the item is genuinely absent: success.
	withNativeSeams(t, "darwin", false, func(name string, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "delete-generic-password" {
			return nil, []byte("error"), exitErr()
		}
		return nil, []byte("The specified item could not be found in the keychain."), exitErr()
	})
	if err := DeleteCredential("svc", "acc"); err != nil {
		t.Fatalf("absent delete must succeed, got %v", err)
	}

	// Delete fails and the item is present: error.
	withNativeSeams(t, "darwin", false, func(name string, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "delete-generic-password" {
			return nil, []byte("error"), exitErr()
		}
		return []byte("s3cr3t\n"), nil, nil
	})
	if err := DeleteCredential("svc", "acc"); err == nil {
		t.Fatal("failed delete of present credential must error")
	}
}

func TestNativeLinuxClassification(t *testing.T) {
	// Tool missing: both operations fail closed.
	withNativeSeams(t, "linux", false, func(string, ...string) ([]byte, []byte, error) {
		t.Fatal("command must not run without secret-tool")
		return nil, nil, nil
	})
	if _, err := GetCredential("svc", "acc"); !errors.Is(err, ErrCredentialBackend) {
		t.Fatalf("missing tool lookup must fail closed, got %v", err)
	}
	if err := DeleteCredential("svc", "acc"); !errors.Is(err, ErrCredentialBackend) {
		t.Fatalf("missing tool delete must fail closed, got %v", err)
	}

	// Present value.
	withNativeSeams(t, "linux", true, func(string, ...string) ([]byte, []byte, error) {
		return []byte("s3cr3t\n"), nil, nil
	})
	if v, err := GetCredential("svc", "acc"); err != nil || v != "s3cr3t" {
		t.Fatalf("present lookup: got %q (%v)", v, err)
	}

	// Empty successful lookup: absent.
	withNativeSeams(t, "linux", true, func(string, ...string) ([]byte, []byte, error) {
		return []byte(""), nil, nil
	})
	if _, err := GetCredential("svc", "acc"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("empty lookup must map to ErrCredentialNotFound, got %v", err)
	}

	// Operational failure with absence diagnostic: absent.
	withNativeSeams(t, "linux", true, func(string, ...string) ([]byte, []byte, error) {
		return nil, []byte("No such secret"), exitErr()
	})
	if _, err := GetCredential("svc", "acc"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("absent lookup must map to ErrCredentialNotFound, got %v", err)
	}
	if err := DeleteCredential("svc", "acc"); err != nil {
		t.Fatalf("absent delete must succeed, got %v", err)
	}

	// D-Bus failure: backend error on both paths.
	withNativeSeams(t, "linux", true, func(string, ...string) ([]byte, []byte, error) {
		return nil, []byte("Cannot autolaunch D-Bus without X11"), exitErr()
	})
	if _, err := GetCredential("svc", "acc"); !errors.Is(err, ErrCredentialBackend) {
		t.Fatalf("D-Bus failure must map to ErrCredentialBackend, got %v", err)
	}
	if err := DeleteCredential("svc", "acc"); !errors.Is(err, ErrCredentialBackend) {
		t.Fatalf("D-Bus delete failure must map to ErrCredentialBackend, got %v", err)
	}
}

func TestNativeUnsupportedOSFailsClosed(t *testing.T) {
	withNativeSeams(t, "windows", false, func(string, ...string) ([]byte, []byte, error) {
		t.Fatal("command must not run on unsupported OS")
		return nil, nil, nil
	})
	if _, err := GetCredential("svc", "acc"); !errors.Is(err, ErrCredentialBackend) {
		t.Fatalf("unsupported OS lookup must fail closed, got %v", err)
	}
	if err := DeleteCredential("svc", "acc"); !errors.Is(err, ErrCredentialBackend) {
		t.Fatalf("unsupported OS delete must fail closed, got %v", err)
	}
}
