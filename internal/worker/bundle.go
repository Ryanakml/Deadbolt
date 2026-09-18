package worker

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

const BundlePlatformPath = ".deadbolt/platform.json"

var (
	ErrBundleDigestMismatch   = errors.New("BUNDLE_DIGEST_MISMATCH: Computed bundle SHA-256 does not match manifest")
	ErrArchitectureMismatch   = errors.New("ARCHITECTURE_MISMATCH: Worker runtime architecture incompatible with bundle target")
	ErrNoBundlePlatform       = errors.New("BUNDLE_PLATFORM_NOT_FOUND: Bundle archive is missing platform metadata")
	ErrBundlePlatformInvalid  = errors.New("BUNDLE_PLATFORM_INVALID: Bundle archive platform metadata is malformed or incomplete")
	ErrBundlePlatformMismatch = errors.New("BUNDLE_PLATFORM_MISMATCH: Bundle embedded platform does not match assigned execution target")
)

// BundlePlatform captures immutable platform metadata embedded inside the bundle.
type BundlePlatform struct {
	TargetArchitecture string `json:"targetArchitecture"`
	TargetOS           string `json:"targetOS"`
}

// CanonicalPlatformBytes serializes target platform metadata into deterministic canonical JSON bytes.
func CanonicalPlatformBytes(targetOS, targetArch string) ([]byte, error) {
	raw, err := json.Marshal(map[string]string{
		"targetArchitecture": targetArch,
		"targetOS":           targetOS,
	})
	if err != nil {
		return nil, err
	}
	return contracts.CanonicalizeFromJSON(raw)
}

// ReadBundlePlatform extracts and parses canonical platform metadata from a bundle tar archive reader.
func ReadBundlePlatform(r io.Reader) (*BundlePlatform, error) {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if filepath.Clean(hdr.Name) == BundlePlatformPath {
			var plat BundlePlatform
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(data, &plat); err != nil {
				return nil, err
			}
			return &plat, nil
		}
	}
	return nil, ErrNoBundlePlatform
}

// ReadBundlePlatformFromFile opens and extracts canonical platform metadata from a bundle archive path.
func ReadBundlePlatformFromFile(path string) (*BundlePlatform, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadBundlePlatform(f)
}

// normalizeBundleArch reduces bare ("arm64"), canonical ("linux/arm64"), and
// legacy alias ("linux-x64", "x64", ...) target forms to a canonical bare
// architecture for exact identity comparison. Unknown values pass through so
// equality still fails closed against well-formed embedded metadata.
func normalizeBundleArch(v string) string {
	s := strings.ToLower(strings.TrimSpace(v))
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	switch s {
	case "amd64", "x64", "x86_64", "linux-x64", "darwin-x64", "win32-x64", "windows-x64":
		return "amd64"
	case "arm64", "linux-arm64", "darwin-arm64":
		return "arm64"
	default:
		return s
	}
}

// normalizeBundleOS canonicalizes a target OS for exact identity comparison.
func normalizeBundleOS(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

// VerifyBundleDigest computes the SHA-256 digest of the bundle data from reader
// and verifies that it exactly matches the expected hex digest.
func VerifyBundleDigest(r io.Reader, expectedDigest string) (string, error) {
	hasher := sha256.New()
	if _, err := io.Copy(hasher, r); err != nil {
		return "", fmt.Errorf("failed to compute bundle digest: %w", err)
	}

	computed := hex.EncodeToString(hasher.Sum(nil))
	expected := strings.TrimPrefix(expectedDigest, "sha256:")

	if !strings.EqualFold(computed, expected) {
		return computed, ErrBundleDigestMismatch
	}

	return "sha256:" + computed, nil
}

// CurrentHostArchitecture returns the current canonical OS/arch string, e.g. "linux/amd64".
func CurrentHostArchitecture() string {
	return runtime.GOOS + "/" + runtime.GOARCH
}

// VerifyArchitecture checks whether the bundle's declared target architecture
// is compatible with the host architecture.
func VerifyArchitecture(targetArch string, hostArch string) error {
	normTarget := strings.ToLower(strings.TrimSpace(targetArch))
	normHost := strings.ToLower(strings.TrimSpace(hostArch))

	if normHost == "" {
		normHost = CurrentHostArchitecture()
	}

	// Direct match
	if normTarget == normHost {
		return nil
	}

	// Common architecture alias normalization
	aliases := map[string]string{
		"arm64":        "linux/arm64",
		"amd64":        "linux/amd64",
		"x64":          "linux/amd64",
		"x86_64":       "linux/amd64",
		"linux-x64":    "linux/amd64",
		"linux-arm64":  "linux/arm64",
		"darwin-arm64": "darwin/arm64",
		"darwin-x64":   "darwin/amd64",
		"win32-x64":    "windows/amd64",
		"windows-x64":  "windows/amd64",
	}

	if targetAliased, ok := aliases[normTarget]; ok {
		normTarget = targetAliased
	}
	if hostAliased, ok := aliases[normHost]; ok {
		normHost = hostAliased
	}

	if normTarget == normHost {
		return nil
	}

	// Allow Darwin (macOS) local worker execution for matching CPU architecture of Linux target
	if (normTarget == "linux/arm64" && normHost == "darwin/arm64") ||
		(normTarget == "linux/amd64" && normHost == "darwin/amd64") {
		return nil
	}

	return ErrArchitectureMismatch
}
