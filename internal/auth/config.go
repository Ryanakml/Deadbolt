package auth

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	// SessionCookieName is the exact __Host- cookie mandated by Blueprint §24.1
	SessionCookieName = "__Host-runtime_session"

	// DefaultSessionIdleTimeout is 12 hours (Blueprint §24.1)
	DefaultSessionIdleTimeout = 12 * time.Hour

	// DefaultSessionAbsoluteTimeout is 7 days (Blueprint §24.1)
	DefaultSessionAbsoluteTimeout = 7 * 24 * time.Hour

	// ModeHosted represents production/staging hosted control plane
	ModeHosted = "hosted"

	// ModeLocal represents local workstation development
	ModeLocal = "local"
)

// OIDCConfig defines configuration for standard OpenID Connect provider
type OIDCConfig struct {
	Issuer       string
	ClientID     string
	ClientSecret string
	RedirectURL  string
}

// Config represents authentication and Go BFF configuration
type Config struct {
	RuntimeMode            string
	OIDC                   OIDCConfig
	AllowedOrigins         []string
	DevAuthEnabled         bool
	CookieSecure           bool
	SessionIdleTimeout     time.Duration
	SessionAbsoluteTimeout time.Duration
}

// DefaultConfig returns default authentication configuration
func DefaultConfig() Config {
	return Config{
		RuntimeMode:            ModeHosted,
		CookieSecure:           true,
		SessionIdleTimeout:     DefaultSessionIdleTimeout,
		SessionAbsoluteTimeout: DefaultSessionAbsoluteTimeout,
	}
}

// Validate verifies configuration boundaries according to Blueprint §22.2 & §24.1.
// In hosted mode, dev auth is strictly rejected.
// In local mode, dev auth is permitted only when bound to loopback.
func (c *Config) Validate(listenHost string) error {
	if c.RuntimeMode == "" {
		return errors.New("RuntimeMode must be configured ('hosted' or 'local')")
	}

	if c.SessionIdleTimeout <= 0 {
		c.SessionIdleTimeout = DefaultSessionIdleTimeout
	}
	if c.SessionAbsoluteTimeout <= 0 {
		c.SessionAbsoluteTimeout = DefaultSessionAbsoluteTimeout
	}

	if c.RuntimeMode == ModeHosted {
		// Blueprint §22.2: "hosted-mode startup rejects dev auth"
		if c.DevAuthEnabled {
			return errors.New("hosted startup rejects dev auth and development keys")
		}
		if c.OIDC.Issuer == "" {
			return errors.New("DEADBOLT_OIDC_ISSUER is required in hosted mode")
		}
		if c.OIDC.ClientID == "" {
			return errors.New("DEADBOLT_OIDC_CLIENT_ID is required in hosted mode")
		}
		if len(c.AllowedOrigins) == 0 {
			return errors.New("at least one allowed origin must be configured in hosted mode")
		}
		return nil
	}

	if c.RuntimeMode == ModeLocal {
		if c.DevAuthEnabled {
			if !isLoopbackHost(listenHost) {
				return fmt.Errorf("dev auth is restricted strictly to loopback binding (got listen host %q)", listenHost)
			}
		}
		return nil
	}

	return fmt.Errorf("unsupported RuntimeMode %q (must be 'hosted' or 'local')", c.RuntimeMode)
}

// IsOriginAllowed checks whether the specified origin is in the allowed origins list
func (c *Config) IsOriginAllowed(origin string) bool {
	if origin == "" {
		return false
	}
	parsedOrigin, err := url.Parse(origin)
	if err != nil {
		return false
	}
	normalizedOrigin := fmt.Sprintf("%s://%s", parsedOrigin.Scheme, parsedOrigin.Host)

	for _, allowed := range c.AllowedOrigins {
		allowed = strings.TrimRight(allowed, "/")
		if strings.EqualFold(normalizedOrigin, allowed) {
			return true
		}
	}
	return false
}

// isLoopbackHost verifies whether the host/IP represents loopback
func isLoopbackHost(host string) bool {
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}

	// Try parsing host without port if present
	h, _, err := net.SplitHostPort(host)
	if err == nil {
		host = h
	}

	if host == "localhost" {
		return true
	}

	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return true
	}

	return false
}
