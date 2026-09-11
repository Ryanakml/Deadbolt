package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	jcs "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	"math"
	"unicode/utf8"
)

type Error struct{ Code string }

func (e *Error) Error() string  { return e.Code }
func failure(code string) error { return &Error{code} }

func checkJSON(v any, depth int) error {
	switch x := v.(type) {
	case nil, bool:
		return nil
	case string:
		if !utf8.ValidString(x) {
			return failure("INVALID_JSON")
		}
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) || (math.Trunc(x) == x && math.Abs(x) > 9007199254740991) {
			return failure("INVALID_JSON")
		}
	case []any:
		if depth >= 32 {
			return failure("INVALID_JSON")
		}
		for _, item := range x {
			if err := checkJSON(item, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		if depth >= 32 {
			return failure("INVALID_JSON")
		}
		for k, item := range x {
			if !utf8.ValidString(k) {
				return failure("INVALID_JSON")
			}
			if err := checkJSON(item, depth+1); err != nil {
				return err
			}
		}
	default:
		return failure("INVALID_JSON")
	}
	return nil
}

// ParseJSON checks raw syntax before decoding can discard duplicates or repair Unicode.
func ParseJSON(raw []byte) (any, error) {
	if err := rawGuard(raw); err != nil {
		return nil, err
	}
	if !utf8.Valid(raw) {
		return nil, failure("INVALID_JSON")
	}
	canonical, err := transform(raw)
	if err != nil {
		return nil, failure("INVALID_JSON")
	}
	var v any
	if json.Unmarshal(canonical, &v) != nil {
		return nil, failure("INVALID_JSON")
	}
	if err = checkJSON(v, 0); err != nil {
		return nil, err
	}
	return v, nil
}
func CanonicalizeFromJSON(raw []byte) ([]byte, error) {
	if _, err := ParseJSON(raw); err != nil {
		return nil, err
	}
	return transform(raw)
}
func CanonicalizeGeneric(v any) ([]byte, error) {
	if err := checkJSON(v, 0); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, failure("INVALID_JSON")
	}
	return transform(raw)
}
func SHA256Hex(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func Digest(raw []byte) ([]byte, string, error) {
	c, err := CanonicalizeFromJSON(raw)
	if err != nil {
		return nil, "", err
	}
	return c, SHA256Hex(c), nil
}

// The upstream implementation accepts object/array roots. A one-element array
// adapter covers all JSON roots without changing their canonical bytes.
func transform(raw []byte) ([]byte, error) {
	if !json.Valid(raw) {
		return nil, failure("INVALID_JSON")
	}
	wrapped := make([]byte, 0, len(raw)+2)
	wrapped = append(wrapped, '[')
	wrapped = append(wrapped, raw...)
	wrapped = append(wrapped, ']')
	result, err := jcs.Transform(wrapped)
	if err != nil {
		return nil, failure("INVALID_JSON")
	}
	return result[1 : len(result)-1], nil
}
