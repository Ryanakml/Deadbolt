package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := log.New(os.Stdout, "[DEADBOLT_WORKER] ", log.LstdFlags|log.Lmsgprefix)

	cpURL := flag.String("control-plane-url", getEnv("DEADBOLT_CONTROL_PLANE_URL", "https://api.deadbolt.cloud"), "Control plane URL")
	keyPath := flag.String("key-path", getEnv("DEADBOLT_WORKER_KEY", ""), "Path to worker private key file (mode 0600)")
	enrollToken := flag.String("enroll-token", getEnv("DEADBOLT_ENROLLMENT_TOKEN", ""), "Single-use enrollment token")
	pool := flag.String("pool", getEnv("DEADBOLT_POOL", "default"), "Worker pool name")
	slotsStr := getEnv("DEADBOLT_SLOTS", "2")
	slotsDefault, _ := strconv.Atoi(slotsStr)
	slots := flag.Int("slots", slotsDefault, "Worker concurrency slot capacity")
	nodePath := flag.String("node-path", getEnv("DEADBOLT_NODE_PATH", "node"), "Path to Node.js executable")
	runnerPath := flag.String("runner-path", getEnv("DEADBOLT_RUNNER_PATH", "./runner/node/dist/index.js"), "Path to Node runner script")
	bundleDir := flag.String("bundle-dir", getEnv("DEADBOLT_BUNDLE_DIR", "./bundles"), "Local bundle storage directory")

	flag.Parse()

	cfg := worker.AgentConfig{
		ControlPlaneURL: *cpURL,
		KeyPath:         *keyPath,
		EnrollmentToken: *enrollToken,
		Pool:            *pool,
		Slots:           *slots,
		NodePath:        *nodePath,
		RunnerPath:      *runnerPath,
		BundleDir:       *bundleDir,
		Logger:          logger,
	}

	agent, err := worker.NewAgent(cfg)
	if err != nil {
		return fmt.Errorf("initialize worker agent: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	logger.Printf("Starting Deadbolt Worker Agent connected to %s", *cpURL)
	return agent.Start(ctx)
}

func getEnv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}
