package contracts

import "strconv"

// Bound parser recursion and reject malformed surrogate pairs before any decoder
// can replace them with U+FFFD. Full JSON syntax is checked by the JCS parser.
func rawGuard(raw []byte) error {
	depth := 0
	quoted := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if !quoted {
			switch c {
			case '"':
				quoted = true
			case '{', '[':
				depth++
				if depth > 32 {
					return failure("INVALID_JSON")
				}
			case '}', ']':
				depth--
			}
			continue
		}
		if c == '"' {
			quoted = false
			continue
		}
		if c != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return failure("INVALID_JSON")
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return failure("INVALID_JSON")
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return failure("INVALID_JSON")
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return failure("INVALID_JSON")
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return failure("INVALID_JSON")
			}
			low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return failure("INVALID_JSON")
			}
			i += 6
		}
	}
	return nil
}
