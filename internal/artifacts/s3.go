package artifacts

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// S3-compatible object storage access using AWS Signature Version 4.
// Implemented with the standard library only (no vendor SDK): presigned URLs
// for single-object worker PUT and scoped GET, plus signed server-side calls
// for verification, bucket setup, and orphan collection. Correctness is
// proven against real S3-compatible storage in integration tests.

// S3Config addresses one bucket on an S3-compatible store.
type S3Config struct {
	Endpoint  string // host:port without scheme
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
	UseSSL    bool
}

// ConfigFromEnv reads the object-store configuration. The second return is
// false when the store is not configured; endpoints then fail closed with
// ErrStoreUnavailable instead of misrouting bytes.
func ConfigFromEnv() (S3Config, bool) {
	cfg := S3Config{
		Endpoint:  strings.TrimSpace(os.Getenv("DEADBOLT_ARTIFACTS_S3_ENDPOINT")),
		Bucket:    strings.TrimSpace(os.Getenv("DEADBOLT_ARTIFACTS_S3_BUCKET")),
		AccessKey: strings.TrimSpace(os.Getenv("DEADBOLT_ARTIFACTS_S3_ACCESS_KEY")),
		SecretKey: strings.TrimSpace(os.Getenv("DEADBOLT_ARTIFACTS_S3_SECRET_KEY")),
		Region:    strings.TrimSpace(os.Getenv("DEADBOLT_ARTIFACTS_S3_REGION")),
		UseSSL:    strings.EqualFold(strings.TrimSpace(os.Getenv("DEADBOLT_ARTIFACTS_S3_USE_SSL")), "true"),
	}
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return S3Config{}, false
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	return cfg, true
}

func (c S3Config) scheme() string {
	if c.UseSSL {
		return "https"
	}
	return "http"
}

func amzDate(t time.Time) string {
	return t.UTC().Format("20060102T150405Z")
}

func dateStamp(t time.Time) string {
	return t.UTC().Format("20060102")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

func signingKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hexSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func encodePath(key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return strings.Join(segments, "/")
}

func sortedQuery(params url.Values) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// SigV4 mandates RFC 3986 encoding: Go's QueryEscape emits "+" for
	// spaces, which breaks signature agreement with S3 implementations.
	escape := func(s string) string {
		return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
	}
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(escape(k))
		b.WriteByte('=')
		b.WriteString(escape(params.Get(k)))
	}
	return b.String()
}

// PresignURL returns a SigV4 query-auth URL for a single object operation.
// Extra query parameters (for example response-content-disposition) are part
// of the signed canonical request. The resulting URL carries signature
// material and must never be written to logs.
func PresignURL(cfg S3Config, method, key string, ttl time.Duration, extra url.Values, now time.Time) string {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	secs := int64(ttl / time.Second)
	if secs < 1 {
		secs = 1
	}
	if secs > 604800 {
		secs = 604800
	}
	query := url.Values{}
	for k, vs := range extra {
		for _, v := range vs {
			query.Add(k, v)
		}
	}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", cfg.AccessKey+"/"+dateStamp(now)+"/"+cfg.Region+"/s3/aws4_request")
	query.Set("X-Amz-Date", amzDate(now))
	query.Set("X-Amz-Expires", fmt.Sprintf("%d", secs))
	query.Set("X-Amz-SignedHeaders", "host")
	canonical := method + "\n" +
		"/" + cfg.Bucket + "/" + encodePath(key) + "\n" +
		sortedQuery(query) + "\n" +
		"host:" + cfg.Endpoint + "\n" +
		"\n" +
		"host\n" +
		"UNSIGNED-PAYLOAD"
	scope := dateStamp(now) + "/" + cfg.Region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate(now) + "\n" + scope + "\n" + hexSHA256Hex([]byte(canonical))
	signature := hex.EncodeToString(hmacSHA256(signingKey(cfg.SecretKey, dateStamp(now), cfg.Region, "s3"), toSign))
	query.Set("X-Amz-Signature", signature)
	return cfg.scheme() + "://" + cfg.Endpoint + "/" + cfg.Bucket + "/" + encodePath(key) + "?" + sortedQuery(query)
}

// signRequest applies SigV4 header auth for server-side calls.
func signRequest(cfg S3Config, req *http.Request, payloadHash string, now time.Time) {
	req.Header.Set("X-Amz-Date", amzDate(now))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	canonicalHeaders := "host:" + req.Host + "\n" + "x-amz-content-sha256:" + payloadHash + "\n" + "x-amz-date:" + amzDate(now) + "\n"
	canonical := req.Method + "\n" +
		req.URL.EscapedPath() + "\n" +
		req.URL.Query().Encode() + "\n" +
		canonicalHeaders + "\n" +
		"host;x-amz-content-sha256;x-amz-date\n" +
		payloadHash
	scope := dateStamp(now) + "/" + cfg.Region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate(now) + "\n" + scope + "\n" + hexSHA256Hex([]byte(canonical))
	signature := hex.EncodeToString(hmacSHA256(signingKey(cfg.SecretKey, dateStamp(now), cfg.Region, "s3"), toSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+cfg.AccessKey+"/"+scope+", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="+signature)
}

// StoreFromEnv builds the configured object store. ok is false when the
// store is not configured; callers must fail closed with ErrStoreUnavailable.
func StoreFromEnv() (ObjectStore, bool) {
	cfg, ok := ConfigFromEnv()
	if !ok {
		return nil, false
	}
	return NewS3Store(cfg), true
}

type ObjectStore interface {
	EnsureBucket(ctx context.Context) error
	PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (string, time.Time, error)
	PresignGet(ctx context.Context, key, filename string, ttl time.Duration) (string, time.Time, error)
	Stat(ctx context.Context, key string) (int64, error)
	Fetch(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}

// S3Store implements ObjectStore against S3-compatible storage.
type S3Store struct {
	cfg    S3Config
	client *http.Client
	now    func() time.Time
}

// NewS3Store builds a store from explicit configuration.
func NewS3Store(cfg S3Config) *S3Store {
	return &S3Store{
		cfg:    cfg,
		client: &http.Client{Timeout: 2 * time.Minute},
		now:    time.Now,
	}
}

func (s *S3Store) objectURL(key string) string {
	return s.cfg.scheme() + "://" + s.cfg.Endpoint + "/" + s.cfg.Bucket + "/" + encodePath(key)
}

func (s *S3Store) doSigned(ctx context.Context, method, key string, query url.Values, body io.Reader, payloadHash string) (*http.Response, error) {
	raw := s.objectURL(key)
	if encoded := query.Encode(); encoded != "" {
		raw += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, method, raw, body)
	if err != nil {
		return nil, err
	}
	signRequest(s.cfg, req, payloadHash, s.now())
	return s.client.Do(req)
}

// EnsureBucket creates the bucket when absent; existing buckets are a no-op.
func (s *S3Store) EnsureBucket(ctx context.Context) error {
	raw := s.cfg.scheme() + "://" + s.cfg.Endpoint + "/" + s.cfg.Bucket + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, raw, nil)
	if err != nil {
		return err
	}
	emptyHash := hexSHA256Hex(nil)
	signRequest(s.cfg, req, emptyHash, s.now())
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict ||
		resp.StatusCode == http.StatusForbidden && bucketExists(ctx, s, raw) {
		return nil
	}
	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("s3 bucket forbidden: %s", resp.Status)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("s3 ensure bucket: %s", resp.Status)
}

func bucketExists(ctx context.Context, s *S3Store, raw string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, raw, nil)
	if err != nil {
		return false
	}
	signRequest(s.cfg, req, hexSHA256Hex(nil), s.now())
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

// PresignPut mints a single-object upload URL valid for ttl.
func (s *S3Store) PresignPut(_ context.Context, key, contentType string, ttl time.Duration) (string, time.Time, error) {
	now := s.now()
	_ = contentType
	return PresignURL(s.cfg, http.MethodPut, key, ttl, nil, now), now.Add(ttl), nil
}

// PresignGet mints a short-lived download URL that forces attachment
// delivery with a neutral content type so bytes never execute in the
// dashboard origin.
func (s *S3Store) PresignGet(_ context.Context, key, filename string, ttl time.Duration) (string, time.Time, error) {
	now := s.now()
	name := strings.ReplaceAll(strings.TrimSpace(filename), "\"", "_")
	if name == "" {
		name = "artifact"
	}
	extra := url.Values{
		"response-content-disposition": {`attachment; filename="` + name + `"`},
		"response-content-type":        {"application/octet-stream"},
	}
	return PresignURL(s.cfg, http.MethodGet, key, ttl, extra, now), now.Add(ttl), nil
}

// Stat returns the stored object size or ErrObjectNotFound.
func (s *S3Store) Stat(ctx context.Context, key string) (int64, error) {
	resp, err := s.doSigned(ctx, http.MethodHead, key, nil, nil, hexSHA256Hex(nil))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return 0, ErrObjectNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("s3 head: %s", resp.Status)
	}
	return resp.ContentLength, nil
}

// Fetch opens the object body for verification. Callers must close it.
func (s *S3Store) Fetch(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.doSigned(ctx, http.MethodGet, key, nil, nil, hexSHA256Hex(nil))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrObjectNotFound
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("s3 get: %s", resp.Status)
	}
	return resp.Body, nil
}

// Delete removes the object; absent objects are a no-op so orphan
// collection stays idempotent across restarts.
func (s *S3Store) Delete(ctx context.Context, key string) error {
	resp, err := s.doSigned(ctx, http.MethodDelete, key, nil, nil, hexSHA256Hex(nil))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("s3 delete: %s", resp.Status)
}
