package testdb

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// GetAdminDatabaseURL returns the admin database connection URL.
func GetAdminDatabaseURL() string {
	if s := os.Getenv("TEST_DATABASE_URL"); s != "" {
		return s
	}
	if s := os.Getenv("DATABASE_URL"); s != "" {
		return s
	}
	return "postgres://localhost:5432/deadbolt_test?sslmode=disable"
}

// GetRoleDatabaseURL builds a database connection URL for a specific role and database.
func GetRoleDatabaseURL(role, dbName string) string {
	baseURL := GetAdminDatabaseURL()
	u, err := url.Parse(baseURL)
	if err != nil {
		if role != "" {
			return fmt.Sprintf("postgres://%s@localhost:5432/%s?sslmode=disable", role, dbName)
		}
		return fmt.Sprintf("postgres://localhost:5432/%s?sslmode=disable", dbName)
	}
	if role != "" {
		u.User = url.User(role)
	}
	u.Path = "/" + dbName
	return u.String()
}

// SetupIsolatedDatabase ensures the target database exists, executes bootstrap-db-roles.sql
// using an admin connection, and returns connection URLs for deadbolt_migrator, deadbolt_runtime,
// and deadbolt_system.
func SetupIsolatedDatabase(dbName, bootstrapScriptPath string) (migratorURL, runtimeURL, systemURL string, err error) {
	adminURL := GetAdminDatabaseURL()
	adminDB, err := sql.Open("pgx", adminURL)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to open admin db: %w", err)
	}
	defer adminDB.Close()

	// 1. Ensure target database exists
	var exists bool
	_ = adminDB.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if !exists {
		if _, err := adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return "", "", "", fmt.Errorf("failed to create db %s: %w", dbName, err)
			}
		}
	}

	// 2. Connect to the target database as admin and run bootstrap-db-roles.sql
	targetAdminURL := GetRoleDatabaseURL("", dbName)
	targetAdminDB, err := sql.Open("pgx", targetAdminURL)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to connect to %s as admin: %w", dbName, err)
	}
	defer targetAdminDB.Close()

	bootstrapSQL, err := os.ReadFile(bootstrapScriptPath)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to read bootstrap script at %s: %w", bootstrapScriptPath, err)
	}
	if _, err := targetAdminDB.Exec(string(bootstrapSQL)); err != nil {
		return "", "", "", fmt.Errorf("failed to execute bootstrap script on %s: %w", dbName, err)
	}

	migratorURL = GetRoleDatabaseURL("deadbolt_migrator", dbName)
	runtimeURL = GetRoleDatabaseURL("deadbolt_runtime", dbName)
	systemURL = GetRoleDatabaseURL("deadbolt_system", dbName)

	return migratorURL, runtimeURL, systemURL, nil
}
