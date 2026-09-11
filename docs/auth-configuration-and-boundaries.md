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
+------------------+ Header: X-CSRF-Token
```

---

## 2. Session Management & Cookie Policy

### Cookie Attributes

The browser session is stored exclusively in a secure, origin-locked cookie:

- **Name**: `__Host-runtime_session`
- **Path**: `/` (enforced by `__Host-` specification)
- **HttpOnly**: `true` (prohibits JavaScript DOM access)
- **Secure**: `true` (transmitted exclusively over HTTPS; configurable in local dev)
- **SameSite**: `Lax` (safeguards top-level navigations while preventing cross-site leakages)

### Lifecycle Rules

1. **Idle Expiration**: 12 hours. Every authenticated request advances the `idle_expires_at` timestamp in `auth_sessions`.
2. **Absolute Expiration**: 7 days. Once `absolute_expires_at` passes, the session is irrevocably expired regardless of recent activity.
3. **Database-Backed Revocation**: Every session validation inspects `revoked_at` in `auth_sessions`. When revoked via `/api/auth/logout`, immediate rejection occurs across all nodes.
4. **Session Rotation on Privilege Mutation**: When switching active organizations or altering security contexts (`/api/auth/switch-org`), the existing session ID and tokens are revoked with reason `ROTATED`, and an entirely fresh session is issued.

---

## 3. CSRF & Origin Validation

For all state-mutating requests (`POST`, `PUT`, `PATCH`, `DELETE`) authenticated via cookie:

1. **Origin Verification**: The request must carry an `Origin` or `Referer` header matching the server's configured `AllowedOrigins` whitelist. Missing or untrusted origins yield `403 Forbidden` (`ORIGIN_FORBIDDEN`).
2. **CSRF Token Header**: The request must present an `X-CSRF-Token` header.
3. **Cryptographic Validation**: The SHA-256 hash of `X-CSRF-Token` must match the `csrf_token_hash` tied to the active database session. Mismatches yield `403 Forbidden` (`CSRF_VALIDATION_FAILED`).

---

## 4. OIDC Verification Pipeline

The Go BFF implements strict OpenID Connect verification:

- **Discovery**: Resolves OpenID Provider metadata from `.well-known/openid-configuration`.
- **JWKS Validation**: Caches and rotates JSON Web Key Sets (supporting RSA and ECDSA keys). Validates matching `kid` and cryptographically verifies token signatures.
- **PKCE S256**: Enforces PKCE (RFC 7636) with SHA-256 code challenge generation and validation.
- **Nonce Verification**: Emits cryptographic random nonce during authorization and verifies identical claim in the returned `id_token`.
- **Audience & Issuer Check**: Asserts that `iss` matches provider issuer and `aud` matches Deadbolt's registered client ID.

---

## 5. Local Dev Auth Isolation

To permit frictionless local workstation development without external OIDC dependencies:

- **Explicit Enablement**: `DEV_AUTH_ENABLED=true` and `DEADBOLT_RUNTIME_MODE=local`.
- **Strict Loopback Binding**: Dev auth endpoints verify that incoming connections originate from loopback addresses (`127.0.0.1`, `::1`). Requests from external interfaces are rejected with `403 Forbidden`.
- **Hosted Production Safeguard**: If `DEADBOLT_RUNTIME_MODE=hosted` (or any non-local mode) and `DEV_AUTH_ENABLED=true`, the runtime startup **panics immediately**, preventing accidental exposure in production.
- **Warning Banner**: Local dev auth issuance emits prominent console log warnings notifying developers that dev auth is strictly prohibited in production.

---

## 6. Database Privileges & Data Redaction

### Schema & Roles (Migration 00005)

- `users`, `oidc_identities`, and `auth_sessions` are owned by the migration role.
- `deadbolt_runtime` is granted minimal required DML (`SELECT`, `INSERT`, `UPDATE`, `DELETE`).
- `deadbolt_system` has all privileges explicitly revoked (`REVOKE ALL`).

### Negative Responses & Log Sanitization

- All authentication error responses return structured, redacted JSON: `{"code": "...", "message": "..."}`.
- Error payloads, stack traces, and debug logs are stripped of raw tokens, cookies, authorization codes, and client secrets.
