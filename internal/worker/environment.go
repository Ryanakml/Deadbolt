package worker

import (
	"sort"
	"strings"
)

// AllowedSystemKeys are fundamental OS environment variables required for standard runtime execution.
var AllowedSystemKeys = map[string]bool{
	"PATH":        true,
	"SYSTEMROOT":  true,
	"WINDIR":      true,
	"COMSPEC":     true,
	"PATHEXT":     true,
	"TEMP":        true,
	"TMP":         true,
	"HOME":        true,
	"USERPROFILE": true,
	"NODE_PATH":   true,
	"LANG":        true,
	"LC_ALL":      true,
	"TZ":          true,
}

// BlockedParentKeywords are sensitive substrings that must never leak from the parent worker agent.
var BlockedParentKeywords = []string{
	"TOKEN",
	"SECRET",
	"PASSWORD",
	"CREDENTIAL",
	"PRIVATE_KEY",
	"DEADBOLT_AGENT",
	"DEADBOLT_SESSION",
	"DATABASE_URL",
	"POSTGRES",
}

// SanitizeEnvironment filters the parent process environment and merges only
// allowed system variables and explicitly declared task environment variables.
// Per Blueprint §12.3: Handlers receive secrets only from allowlisted worker variables.
// There is no fallback that sends the agent's entire environment into the child process.
func SanitizeEnvironment(
	parentEnv []string,
	declaredEnv map[string]string,
	allowlistKeys []string,
) []string {
	result := make(map[string]string)

	allowlistSet := make(map[string]bool)
	for _, k := range allowlistKeys {
		allowlistSet[strings.ToUpper(strings.TrimSpace(k))] = true
	}

	// 1. Filter parent environment strictly
	for _, entry := range parentEnv {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		upperKey := strings.ToUpper(key)
		val := parts[1]

		// Check if it matches sensitive blocked keywords
		blocked := false
		for _, kw := range BlockedParentKeywords {
			if strings.Contains(upperKey, kw) {
				blocked = true
				break
			}
		}

		if blocked && !allowlistSet[upperKey] {
			continue
		}

		// Only include if it is in AllowedSystemKeys or explicitly allowlisted
		if AllowedSystemKeys[upperKey] || allowlistSet[upperKey] {
			result[key] = val
		}
	}

	// 2. Inject task-declared environment variables
	for k, v := range declaredEnv {
		result[k] = v
	}

	// 3. Produce stable sorted slice
	entries := make([]string, 0, len(result))
	for k, v := range result {
		entries = append(entries, k+"="+v)
	}
	sort.Strings(entries)

	return entries
}
