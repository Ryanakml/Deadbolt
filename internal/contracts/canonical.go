package contracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// CanonicalizeFromJSON parses a JSON byte slice and returns its RFC 8785 canonical bytes.
func CanonicalizeFromJSON(raw []byte) ([]byte, error) {
	var generic any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&generic); err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}
	return CanonicalizeGeneric(generic)
}

// CanonicalizeGeneric serializes an unmarshaled JSON value into RFC 8785 canonical bytes.
func CanonicalizeGeneric(v any) ([]byte, error) {
	switch val := v.(type) {
	case nil:
		return []byte("null"), nil
	case bool:
		if val {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case string:
		return json.Marshal(val)
	case json.Number:
		return []byte(val.String()), nil
	case []any:
		var buf bytes.Buffer
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteByte(',')
			}
			b, err := CanonicalizeGeneric(item)
			if err != nil {
				return nil, err
			}
			buf.Write(b)
		}
		buf.WriteByte(']')
		return buf.Bytes(), nil
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		// RFC 8785 sorts keys lexicographically by UTF-16 code units.
		// In Go, UTF-8 strings sort identically to UTF-16 code units for BMP code points.
		sort.Strings(keys)

		var buf bytes.Buffer
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			buf.Write(kb)
			buf.WriteByte(':')
			vb, err := CanonicalizeGeneric(val[k])
			if err != nil {
				return nil, err
			}
			buf.Write(vb)
		}
		buf.WriteByte('}')
		return buf.Bytes(), nil
	default:
		return json.Marshal(val)
	}
}

// SHA256Hex computes the SHA-256 hex string of canonical bytes.
func SHA256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// Digest computes the canonical form and SHA-256 hash from raw JSON bytes.
func Digest(raw []byte) ([]byte, string, error) {
	canonical, err := CanonicalizeFromJSON(raw)
	if err != nil {
		return nil, "", err
	}
	return canonical, SHA256Hex(canonical), nil
}
