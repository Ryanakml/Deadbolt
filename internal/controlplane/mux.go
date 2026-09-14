package controlplane

import (
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/gateway"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// BuildMux wires all production routes onto a new http.ServeMux.
// This canonical constructor is shared between production (cmd/control-plane)
// and integration tests to guarantee identical route mountings and middleware.
func BuildMux(cfg auth.Config, pool *pgxpool.Pool, healthChecker *gateway.HealthChecker, logger *log.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	if healthChecker != nil {
		healthChecker.Routes(mux)
	}

	if pool != nil {
		store := auth.NewSessionStore(pool)
		storagePool := storage.NewPool(pool)
		tenantService := tenant.NewService(storagePool)
		tenantHandler := tenant.NewHTTPHandler(tenantService, pool, store, cfg)
		tenantHandler.RegisterRoutes(mux)

		oidcClient := auth.NewOIDCClient(cfg.OIDC, http.DefaultClient)
		bff := auth.NewBFFHandler(cfg, oidcClient, store, pool)
		if logger != nil {
			bff.SetLogger(logger)
		}

		mux.Handle("/api/auth/", bff.Routes())

		if cfg.RuntimeMode == auth.ModeLocal && cfg.DevAuthEnabled {
			devAuth := auth.NewDevAuthHandler(cfg, store, pool)
			mux.HandleFunc("/api/auth/dev-login", devAuth.HandleDevLogin)
			if logger != nil {
				logger.Printf("Local developer authentication endpoint enabled at /api/auth/dev-login")
			}
		}
	}

	return mux
}
