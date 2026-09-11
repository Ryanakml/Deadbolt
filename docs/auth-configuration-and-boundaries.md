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

## 6. Local Dev Auth & Development Key Boundaries (Blueprint §24.4)

### Development Key Management

Per Blueprint §24.4:

- Task secrets remain on customer workers; platform secrets use cloud secret manager and envelope encryption with KMS in hosted mode.
- Local mode uses an explicit development key stored in a gitignored secret file (e.g. `.deadbolt-dev-key` or `dev.key`).
- Hosted startup strictly rejects any development keys, secret paths, or dev auth flags.

### Configuration Inputs & Environment Variables

| Setting / Env Var                                     | Hosted Mode (`hosted`)                      | Local Mode (`local`)                                               |
| :---------------------------------------------------- | :------------------------------------------ | :----------------------------------------------------------------- |
| `Config.DevAuthEnabled` / `DEADBOLT_DEV_AUTH_ENABLED` | **Strictly Rejected** (fatal startup error) | Permitted (strictly requires loopback listen host)                 |
| `Config.DevKey` / `DEADBOLT_DEV_KEY`                  | **Strictly Rejected** (fatal startup error) | Permitted (resolves development encryption/auth key)               |
| `Config.DevKeyPath` / `DEADBOLT_DEV_KEY_PATH`         | **Strictly Rejected** (fatal startup error) | Permitted (reads gitignored secret file, e.g. `.deadbolt-dev-key`) |
| `Config.ReadDevKey()`                                 | Returns error (`prohibited in hosted mode`) | Resolves key from field, file path, or env var                     |

### Strict Loopback Listen Host Validation

When dev auth is enabled in local mode:

- The listen host must be non-empty and resolve strictly to loopback (`127.0.0.1`, `localhost`, `::1`).
- Empty strings, wildcards (`0.0.0.0`), and remote interfaces (`192.168.x.x`) are rejected at startup.
- Dev login endpoints check the caller's IP and reject non-loopback requests with `403 Forbidden` (`LOOPBACK_REQUIRED`).
- Development sessions emit prominent security warning banners in logs.

---

## 7. Structured Security Log Redaction & Negative Log Isolation

### Security Log Invariants

Issue #4 and Blueprint §24.4 mandate that logs never record Authorization headers, cookies, tokens, signed URLs, secrets, or other-tenant identifiers:

1. **Allowlisted Reason Codes**: `LogSecurityEvent` rejects arbitrary string details. All log events record a static, typed `SecurityReason` from an allowlisted classification map (`ReasonTokenVerificationFailed`, `ReasonUnauthorizedOrgMembership`, `ReasonOriginNotAllowlisted`, etc.).
2. **Upstream OIDC Error Isolation**: When OIDC discovery, JWKS retrieval, or token exchange fails, raw upstream HTTP error response bodies (which might echo authorization codes, client secrets, or tokens) are strictly omitted from logs. The event records only `event=OIDC_EXCHANGE_FAILED reason=token_verification_failed`.
3. **Cross-Tenant Data Isolation**: When a tenant membership switch is denied (`/api/auth/switch-org`), the requested foreign tenant organization ID is strictly omitted from logs. The event records `event=ORG_SWITCH_DENIED reason=unauthorized_org_membership`.
4. **Credential Redaction**: Authorization headers (`Bearer ...`), cookie headers (`Cookie`, `Set-Cookie`), and CSRF tokens (`X-CSRF-Token`) are replaced with `[REDACTED]`.
5. **Sentinel Verification**: Automated integration tests inject sensitive token sentinels (`SENTINEL_OIDC_SECRET_TOKEN_99999`) and foreign tenant UUID sentinels (`foreign-tenant-sentinel-uuid-77777777-8888`), proving they never appear in log buffers or response payloads.

---

## 8. Real Browser Session Smoke Test (Headless Chrome via CDP)

To guarantee that authentication behavior is validated against an actual browser engine (and not merely simulated via `http.Client`):

- **Script**: `scripts/browser-smoke.mjs` executes headless Google Chrome / Chromium over the Chrome DevTools Protocol (CDP) via native Node 22 `WebSocket` with zero external npm dependencies.
- **End-to-End Lifecycle**:
  1. **Navigation & Redirect Chain**: Navigates to `/api/auth/login`, follows the 302 redirect chain through OIDC fixture `/authorize`, Go BFF `/api/auth/callback`, and lands on the SPA root `/`.
  2. **Browser Storage Inspection**: Queries CDP `Network.getCookies` to verify real browser cookie storage for `__Host-runtime_session` (`HttpOnly=true, Secure=true`) and `__Host-csrf_token` (`HttpOnly=false, Secure=true`).
  3. **DOM Masking Enforcement**: Evaluates `document.cookie` inside browser context, confirming that the session cookie is strictly invisible to JavaScript while the CSRF bootstrap cookie is readable.
  4. **Authenticated Mutation**: Extracts the CSRF token via JavaScript and dispatches an authenticated `POST /api/mutation` fetch request with credentials, verifying `200 OK`.
  5. **Authenticated Logout & Revocation**: Dispatches `POST /api/auth/logout` fetch request, verifying `204 No Content` and cookie clearing.
  6. **Revocation Verification**: Dispatches subsequent `GET /api/auth/session` fetch request from the browser, verifying `401 Unauthorized`.
- **Test Integration**: `TestRealChromeBrowserSmoke` in `tests/integration/auth_test.go` automatically runs this browser engine smoke test when Chrome/Chromium is installed, ensuring continuous automated verification across local and CI environments.
