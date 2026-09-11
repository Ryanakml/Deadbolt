# Authentication Architecture & Security Boundaries

This document details the architecture, operational boundaries, and security guarantees for Deadbolt's authentication subsystem established under Milestone 0 (Issue #4).

## 1. Architectural Overview

Deadbolt implements a **Backend-For-Frontend (BFF)** authentication model in Go, eliminating browser exposure of raw identity tokens, access tokens, and refresh tokens in accordance with Blueprint §22.2 and §24.1.

```
+------------------+         +-------------------------+         +---------------------+
|                  |  (1)    |                         |  (2)    |                     |
|  Browser / Client+-------->+ Go BFF (/api/auth/login)+-------->+   OIDC Provider     |
|                  |         | (Generates PKCE+State)  |         | (Google, Okta, etc.)|
|                  |<--------+                         |<--------+                     |
|                  |  (3)    |                         |  (4)    |                     |
|                  |         |                         |         +---------------------+
|                  |  (5)    |                         |  (6)               |
|                  +-------->+ /api/auth/callback      +--------(Token Exch)|
|                  |         | (Validates PKCE, Nonce, |                    v
|                  |         |  JWKS Signature)        |         +---------------------+
|                  |         |                         |         | PostgreSQL          |
|                  |  (7)    | (Issues Session + CSRF) |  (7)    | (auth_sessions,     |
|                  |<--------+-------------------------+-------->|  oidc_identities)   |
|                  | Set-Cookie: __Host-runtime_session          +---------------------+
|                  | Set-Cookie: __Host-csrf_token (readable)
+------------------+ Header: X-CSRF-Token
```

---

## 2. Session Management & Cookie Policy

### Hosted Cookie Attributes (Blueprint §24.1)

The browser session and CSRF bootstrap tokens are stored exclusively in origin-locked cookies conforming to RFC 6265bis:

- **Session Cookie**: `__Host-runtime_session`
  - `Path=/` (enforced by `__Host-` specification)
  - `HttpOnly=true` (strictly prohibits JavaScript DOM access)
  - `Secure=true` (transmitted exclusively over TLS; hosted mode strictly rejects `CookieSecure=false`)
  - `SameSite=Lax` (safeguards top-level navigations while preventing cross-site leakages)
- **CSRF Bootstrap Cookie**: `__Host-csrf_token`
  - `Path=/`
  - `HttpOnly=false` (readable by single-page applications to retrieve the CSRF token)
  - `Secure=true`
  - `SameSite=Lax`

### Local Development Non-Secure Fallback

When developing locally over unencrypted HTTP (`CookieSecure=false`), conforming browsers strictly reject any cookie starting with `__Host-`. To remain browser-compliant in local dev:

- Session cookie uses `deadbolt_local_session` (`HttpOnly=true`, `SameSite=Lax`).
- CSRF cookie uses `deadbolt_local_csrf` (`HttpOnly=false`, `SameSite=Lax`).

### Lifecycle Rules

1. **Idle Expiration**: 12 hours. Every authenticated request advances the `idle_expires_at` timestamp in `auth_sessions`.
2. **Absolute Expiration**: 7 days. Once `absolute_expires_at` passes, the session is irrevocably expired regardless of recent activity.
3. **Database-Backed Revocation**: Every session validation inspects `revoked_at` in `auth_sessions`. When revoked via `/api/auth/logout`, immediate rejection occurs across all nodes.
4. **Session Rotation on Privilege Mutation**: When switching active organizations or altering security contexts (`/api/auth/switch-org`), the existing session ID and tokens are revoked with reason `ROTATED`, and entirely fresh session and CSRF tokens are issued.

---

## 3. CSRF Bootstrap & Origin Validation

### Browser CSRF Bootstrap Pattern

Because `HandleCallback` redirects top-level navigations to `/`, client-side single-page applications cannot inspect 302 redirect response headers. Deadbolt provides a dual bootstrap mechanism:

1. **Readable Cookie Bootstrap**: The `__Host-csrf_token` (or `deadbolt_local_csrf`) cookie is set during login and session rotation, readable directly by browser JavaScript.
2. **Protected API Bootstrap Endpoint**: `GET /api/auth/csrf` (guarded by `RequireAuth`) synchronizes and returns a fresh CSRF token.

### Mutating Request Enforcement

For all state-mutating requests (`POST`, `PUT`, `PATCH`, `DELETE`):

1. **Origin Verification**: The request must carry an `Origin` or `Referer` header matching the server's configured `AllowedOrigins` whitelist. Missing or untrusted origins yield `403 Forbidden` (`ORIGIN_FORBIDDEN`).
2. **CSRF Token Header**: The request must present an `X-CSRF-Token` header matching the active session's hash in `auth_sessions`. Mismatches yield `403 Forbidden` (`CSRF_VALIDATION_FAILED`).
3. **Logout Route Hardening**: `/api/auth/logout` is strictly guarded by both `RequireAuth` and `RequireCSRFAndOrigin`, preventing unauthorized cross-site session destruction.

---

## 4. Allowlisted CORS & Preflight Policy

The Go BFF enforces strict allowlisted CORS via `CORSMiddleware`:

- **Dynamic Origin Matching**: Only origins present in `AllowedOrigins` are permitted. Wildcard (`*`) is never used because credentials (`Access-Control-Allow-Credentials: true`) are enabled.
- **Preflight Handling**: Valid `OPTIONS` preflight requests receive `204 No Content` with allowed methods, headers (`Content-Type, Authorization, X-CSRF-Token, Idempotency-Key`), and `Access-Control-Max-Age: 86400`.
- **Untrusted Preflight Rejection**: Preflights from unlisted origins are immediately rejected with `403 Forbidden` (`ORIGIN_FORBIDDEN`).

---

## 5. OIDC Verification Engine & PKCE S256

The Go BFF implements strict OpenID Connect verification:

- **Discovery**: Resolves OpenID Provider metadata from `.well-known/openid-configuration`.
- **JWKS Validation**: Caches and rotates JSON Web Key Sets (supporting RSA and ECDSA keys). Validates matching `kid` and cryptographically verifies token signatures. Untrusted or forged signatures are rejected.
- **PKCE S256**: Enforces RFC 7636 PKCE with SHA-256 code challenge generation and validation. Authorization codes are strictly single-use and bound to the code challenge; replay attacks are rejected.
- **Mandatory Claims**: Rejects ID tokens missing `iss`, `sub`, or `exp`. Enforces audience matching and cryptographic random `nonce` validation.

---

## 6. Local Dev Auth Isolation & Production Guardrails

To permit frictionless local workstation development without external OIDC dependencies:

- **Explicit Enablement**: `DEV_AUTH_ENABLED=true` and `DEADBOLT_RUNTIME_MODE=local`.
- **Strict Loopback Binding**: Dev auth endpoints verify that incoming connections originate from loopback addresses (`127.0.0.1`, `::1`). Requests from external interfaces are rejected with `403 Forbidden`. Empty or wildcard listen hosts (`""`, `0.0.0.0`) are strictly rejected during configuration validation.
- **Hosted Production Safeguard**: If `DEADBOLT_RUNTIME_MODE=hosted` and `DEV_AUTH_ENABLED=true`, the runtime startup rejects configuration immediately.
- **Warning Banner**: Local dev auth issuance emits prominent console log warnings notifying developers that dev auth is strictly prohibited in production.

---

## 7. Database Privileges & Log Redaction Guarantees

### Schema & Roles (Migration 00005)

- `users`, `oidc_identities`, and `auth_sessions` are owned by the migration role.
- `deadbolt_runtime` is granted minimal required DML (`SELECT`, `INSERT`, `UPDATE`, `DELETE`).
- `deadbolt_system` has all privileges explicitly revoked (`REVOKE ALL`).

### Negative Responses & Secret Redaction

- All authentication error responses return structured, redacted JSON: `{"code": "...", "message": "..."}`.
- Error payloads, stack traces, and debug logs are stripped of raw tokens, cookies, authorization codes, client secrets, and authorization headers (`[AUTH_SECURITY]` audit logs redact all sensitive credentials).
