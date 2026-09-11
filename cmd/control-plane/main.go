package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/gateway"
)

var (
	// Version is the semantic release version
	Version = "0.1.0"
	// CommitSHA is the exact git commit SHA, injected via -ldflags
	CommitSHA = "dev"
	// BuildTime is the ISO 8601 build timestamp, injected via -ldflags
	BuildTime = ""
	// ImageDigest is the immutable container image digest (@sha256:...), injected via -ldflags
	ImageDigest = ""
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := log.New(os.Stdout, "[DEADBOLT_CONTROL_PLANE] ", log.LstdFlags|log.Lmsgprefix)

	runtimeMode := strings.ToLower(strings.TrimSpace(os.Getenv("RUNTIME_MODE")))
	if runtimeMode == "" {
		runtimeMode = auth.ModeHosted
	}

	listenAddr := strings.TrimSpace(os.Getenv("LISTEN_ADDR"))
	if listenAddr == "" {
		port := strings.TrimSpace(os.Getenv("PORT"))
		if port != "" {
			if runtimeMode == auth.ModeLocal {
				listenAddr = "127.0.0.1:" + port
			} else {
				listenAddr = ":" + port
			}
		} else {
			if runtimeMode == auth.ModeLocal {
				listenAddr = "127.0.0.1:8080"
			} else {
				listenAddr = ":8080"
			}
		}
	}

	// Determine listen host for loopback validation
	listenHost := listenAddr
	if h, _, err := net.SplitHostPort(listenAddr); err == nil {
		listenHost = h
	}

	cookieSecure := runtimeMode == auth.ModeHosted
	if val := os.Getenv("COOKIE_SECURE"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			cookieSecure = parsed
		}
	}

	devAuthEnabled := false
	if val := os.Getenv("DEV_AUTH_ENABLED"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			devAuthEnabled = parsed
		}
	}

	var allowedOrigins []string
	if rawOrigins := os.Getenv("DEADBOLT_ALLOWED_ORIGINS"); rawOrigins != "" {
		for _, o := range strings.Split(rawOrigins, ",") {
			if trimmed := strings.TrimSpace(o); trimmed != "" {
				allowedOrigins = append(allowedOrigins, trimmed)
			}
		}
	}

	cfg := auth.Config{
		RuntimeMode:            runtimeMode,
		DevAuthEnabled:         devAuthEnabled,
		DevKey:                 os.Getenv("DEADBOLT_DEV_KEY"),
		DevKeyPath:             os.Getenv("DEADBOLT_DEV_KEY_PATH"),
		CookieSecure:           cookieSecure,
		AllowedOrigins:         allowedOrigins,
		SessionIdleTimeout:     auth.DefaultSessionIdleTimeout,
		SessionAbsoluteTimeout: auth.DefaultSessionAbsoluteTimeout,
		OIDC: auth.OIDCConfig{
			Issuer:       os.Getenv("DEADBOLT_OIDC_ISSUER"),
			ClientID:     os.Getenv("DEADBOLT_OIDC_CLIENT_ID"),
			ClientSecret: os.Getenv("DEADBOLT_OIDC_CLIENT_SECRET"),
			RedirectURL:  os.Getenv("DEADBOLT_OIDC_REDIRECT_URL"),
		},
	}

	// Validate configuration boundaries according to Blueprint §22.2, §24.1 & §24.4
	if err := cfg.Validate(listenHost); err != nil {
		return fmt.Errorf("configuration validation failed: %w\n"+
			"Remediation:\n"+
			"  - Hosted mode (RUNTIME_MODE=hosted) strictly requires:\n"+
			"      COOKIE_SECURE=true\n"+
			"      DEV_AUTH_ENABLED=false (dev auth and development keys are barred in hosted mode)\n"+
			"      DEADBOLT_OIDC_ISSUER, DEADBOLT_OIDC_CLIENT_ID, and DEADBOLT_ALLOWED_ORIGINS configured\n"+
			"  - Local mode (RUNTIME_MODE=local) requires:\n"+
			"      LISTEN_ADDR bound strictly to loopback (127.0.0.1 or localhost)\n"+
			"      Valid DevKeyPath if configured", err)
	}

	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dbURL == "" && runtimeMode == auth.ModeHosted {
		return errors.New("DATABASE_URL is required in hosted mode\n" +
			"Remediation: Configure DATABASE_URL=postgres://<user>:<password>@<host>:<port>/<dbname>?sslmode=...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pool *pgxpool.Pool
	if dbURL != "" {
		poolConfig, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			return fmt.Errorf("invalid DATABASE_URL: %w", err)
		}
		poolConfig.MaxConns = 25
		poolConfig.MinConns = 2
		poolConfig.MaxConnIdleTime = 5 * time.Minute

		p, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return fmt.Errorf("failed to initialize database pool: %w", err)
		}
		pool = p
		defer pool.Close()
		logger.Printf("Database connection pool initialized.")
	}

	versionInfo := gateway.VersionInfo{
		Version:     Version,
		CommitSHA:   CommitSHA,
		BuildTime:   BuildTime,
		ImageDigest: ImageDigest,
		RuntimeMode: cfg.RuntimeMode,
	}

	// Latest expected migration in M0 is 5 (00005_auth_and_sessions.sql)
	healthChecker := gateway.NewHealthChecker(versionInfo, pool, nil, 5)

	mux := http.NewServeMux()
	healthChecker.Routes(mux)

	if pool != nil {
		store := auth.NewSessionStore(pool)
		oidcClient := auth.NewOIDCClient(cfg.OIDC, http.DefaultClient)
		bff := auth.NewBFFHandler(cfg, oidcClient, store, pool)
		bff.SetLogger(logger)

		mux.Handle("/api/auth/", bff.Routes())

		if cfg.RuntimeMode == auth.ModeLocal && cfg.DevAuthEnabled {
			devAuth := auth.NewDevAuthHandler(cfg, store, pool)
			mux.HandleFunc("/api/auth/dev-login", devAuth.HandleDevLogin)
			logger.Printf("Local developer authentication endpoint enabled at /api/auth/dev-login (LOOPBACK ONLY)")
		}
	}

	server := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Printf("Starting Deadbolt Control Plane [%s] on %s (commit: %s)", runtimeMode, listenAddr, CommitSHA)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		return fmt.Errorf("http server failed: %w", err)
	case sig := <-quit:
		logger.Printf("Received termination signal %s; starting graceful shutdown...", sig)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Server shutdown error: %v", err)
	}

	logger.Printf("Control plane shutdown cleanly completed.")
	return nil
}
