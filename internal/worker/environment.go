package worker

import (
	"fmt"
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

var ReservedRunnerKeys = map[string]bool{
	"DEADBOLT_RESULT_FILE": true,
	"DEADBOLT_RESULT_FD":   true,
	"NODE_OPTIONS":         true,
}

// ValidateTaskEnvironment enforces the manifest-approved task variable names.
// A task cannot smuggle a runner control variable or override agent runtime.
func ValidateTaskEnvironment(declared map[string]string, approved []string) error {
	allowed := make(map[string]bool, len(approved))
	for _, key := range approved {
		allowed[strings.ToUpper(strings.TrimSpace(key))] = true
	}
	for key := range declared {
		normalized := strings.ToUpper(strings.TrimSpace(key))
		if ReservedRunnerKeys[normalized] {
			return fmt.Errorf("TASK_ENV_RESERVED_KEY: %s", key)
		}
		if !allowed[normalized] {
			return fmt.Errorf("TASK_ENV_NOT_DECLARED: %s", key)
		}
	}
	return nil
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
