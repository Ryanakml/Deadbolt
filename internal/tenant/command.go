package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5"
)

type commandContextKey struct{}

// Command identifies one HTTP tenant mutation. The scope is an organization ID
// for tenant commands and a user-derived scope for organization creation.
type Command struct {
	Scope       string
	Key         string
	Fingerprint string
	Operation   string
}

func ContextWithCommand(ctx context.Context, command Command) context.Context {
	return context.WithValue(ctx, commandContextKey{}, command)
}

func commandFromContext(ctx context.Context) (Command, bool) {
	command, ok := ctx.Value(commandContextKey{}).(Command)
	return command, ok && command.Key != "" && command.Scope != "" && command.Fingerprint != "" && command.Operation != ""
}

// WithCommandTx is the canonical mutation idempotency path. It claims, mutates,
// records, and replays a scoped command in one tenant transaction.
func (s *Service) WithCommandTx(ctx context.Context, orgID string, operation string, responseCode int, mutate func(context.Context, storage.Tx) error, outcome func() any, replay func(json.RawMessage) error) (bool, error) {
	return s.withCommandTx(ctx, orgID, operation, responseCode, mutate, outcome, replay)
}

// WithCommandTxDynamic records the response code selected by the authoritative
// mutation outcome in the same transaction. It is for create-or-return flows
// where a concurrent winner can turn an otherwise-new request into 200.
func (s *Service) WithCommandTxDynamic(ctx context.Context, orgID, operation string, mutate func(context.Context, storage.Tx) error, responseCode func() int, outcome func() any, replay func(json.RawMessage) error) (bool, int, error) {
	command, ok := commandFromContext(ctx)
	if !ok {
		err := s.pool.WithTenantTx(ctx, orgID, mutate)
		return false, responseCode(), err
	}
	if operation != "" && command.Operation != operation {
		return false, 0, fmt.Errorf("%w: command operation mismatch", ErrCommandStorage)
	}
	replayed := false
	recordedCode := 0
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var insertedID string
		err := tx.QueryRow(ctx, `INSERT INTO tenant_commands (command_scope,organization_id,idempotency_key,request_fingerprint,operation,status,response_code,outcome) VALUES ($1,NULLIF($2,'')::uuid,$3,$4,$5,'PROCESSING',200,'{}'::jsonb) ON CONFLICT (command_scope,idempotency_key) DO NOTHING RETURNING id::text`, command.Scope, orgID, command.Key, command.Fingerprint, command.Operation).Scan(&insertedID)
		if err == nil {
			if err := mutate(ctx, tx); err != nil {
				return err
			}
			encoded, err := json.Marshal(outcome())
			if err != nil {
				return fmt.Errorf("%w: encode outcome: %v", ErrCommandStorage, err)
			}
			code := responseCode()
			if code <= 0 {
				code = http.StatusOK
			}
			recordedCode = code
			_, err = tx.Exec(ctx, `UPDATE tenant_commands SET status='COMPLETED',response_code=$3,outcome=$4::jsonb,completed_at=clock_timestamp() WHERE command_scope=$1 AND idempotency_key=$2`, command.Scope, command.Key, code, encoded)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: claim command: %v", ErrCommandStorage, err)
		}
		var fp, op, status string
		var code int
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT status,request_fingerprint,operation,response_code,outcome FROM tenant_commands WHERE command_scope=$1 AND idempotency_key=$2 FOR UPDATE`, command.Scope, command.Key).Scan(&status, &fp, &op, &code, &raw); err != nil {
			return err
		}
		if fp != command.Fingerprint || op != command.Operation {
			return ErrIdempotencyConflict
		}
		if status != "COMPLETED" {
			return fmt.Errorf("%w: command in unexpected status %q", ErrCommandStorage, status)
		}
		if err := replay(raw); err != nil {
			return err
		}
		replayed = true
		recordedCode = code
		return nil
	})
	return replayed, recordedCode, err
}

func RequestFingerprint(method, path string, body []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(method))
	_, _ = h.Write([]byte{'\n'})
	_, _ = h.Write([]byte(path))
	_, _ = h.Write([]byte{'\n'})
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// commandOrganizationID gives organization creation a stable tenant ID before
// the command is claimed. That lets a retry enter the same RLS scope while the
// command row's deferred foreign key and organization insert commit together.
func commandOrganizationID(userID, key string) string {
	sum := sha256.Sum256([]byte("deadbolt:organization-command:" + userID + ":" + key))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func extractResourceID(v any) *string {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case *Organization:
		if val != nil && val.ID != "" {
			return &val.ID
		}
	case *Project:
		if val != nil && val.ID != "" {
			return &val.ID
		}
	case *Environment:
		if val != nil && val.ID != "" {
			return &val.ID
		}
	case *GeneratedKey:
		if val != nil && val.ID != "" {
			return &val.ID
		}
	case *Member:
		if val != nil && val.ID != "" {
			return &val.ID
		}
	}
	return nil
}

// CompletedCommand represents an authoritative completed mutation command.
type CompletedCommand struct {
	Scope        string
	Key          string
	Fingerprint  string
	Operation    string
	ResponseCode int
	Outcome      []byte
}

// GetCompletedCommand checks whether a command was already accepted and completed.
func (s *Service) GetCompletedCommand(ctx context.Context, orgID string, scope string, key string) (*CompletedCommand, error) {
	var cmd CompletedCommand
	cmd.Scope = scope
	cmd.Key = key
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT request_fingerprint, operation, response_code, outcome
			FROM tenant_commands
			WHERE command_scope = $1 AND idempotency_key = $2 AND status = 'COMPLETED'
		`
		return tx.QueryRow(ctx, query, scope, key).Scan(&cmd.Fingerprint, &cmd.Operation, &cmd.ResponseCode, &cmd.Outcome)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// withCommandTx claims, mutates, records and replays inside one transaction.
// The unique index serializes simultaneous claims: the loser blocks on the
// conflicting row, then reads the committed durable outcome.
func (s *Service) withCommandTx(ctx context.Context, orgID string, operation string, responseCode int, mutate func(context.Context, storage.Tx) error, outcome func() any, replay func(json.RawMessage) error) (bool, error) {
	command, ok := commandFromContext(ctx)
	if !ok {
		return false, s.pool.WithTenantTx(ctx, orgID, mutate)
	}
	if operation != "" && command.Operation != operation {
		return false, fmt.Errorf("%w: command operation mismatch", ErrCommandStorage)
	}
	if responseCode <= 0 {
		responseCode = http.StatusOK
	}

	replayed := false
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var insertedID string
		err := tx.QueryRow(ctx, `
			INSERT INTO tenant_commands (command_scope, organization_id, idempotency_key, request_fingerprint, operation, status, response_code, outcome)
			VALUES ($1, NULLIF($2, '')::uuid, $3, $4, $5, 'PROCESSING', $6, '{}'::jsonb)
			ON CONFLICT (command_scope, idempotency_key) DO NOTHING
			RETURNING id::text
		`, command.Scope, orgID, command.Key, command.Fingerprint, command.Operation, responseCode).Scan(&insertedID)
		if err == nil {
			if err := mutate(ctx, tx); err != nil {
				return err
			}
			out := outcome()
			encoded, err := json.Marshal(out)
			if err != nil {
				return fmt.Errorf("%w: encode outcome: %v", ErrCommandStorage, err)
			}
			resID := extractResourceID(out)
			if _, err := tx.Exec(ctx, `
				UPDATE tenant_commands
				SET status = 'COMPLETED', resource_id = $3, response_code = $4, outcome = $5::jsonb, completed_at = clock_timestamp()
				WHERE command_scope = $1 AND idempotency_key = $2
			`, command.Scope, command.Key, resID, responseCode, encoded); err != nil {
				return fmt.Errorf("%w: record outcome: %v", ErrCommandStorage, err)
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: claim command: %v", ErrCommandStorage, err)
		}

		// FOR UPDATE waits for the winning transaction before reading its outcome.
		var recordedFingerprint, recordedOperation, recordedStatus string
		var recordedResponseCode int
		var recordedOutcome []byte
		if err := tx.QueryRow(ctx, `
			SELECT status, request_fingerprint, operation, response_code, outcome
			FROM tenant_commands
			WHERE command_scope = $1 AND idempotency_key = $2
			FOR UPDATE
		`, command.Scope, command.Key).Scan(&recordedStatus, &recordedFingerprint, &recordedOperation, &recordedResponseCode, &recordedOutcome); err != nil {
			return fmt.Errorf("%w: load command: %v", ErrCommandStorage, err)
		}
		if recordedFingerprint != command.Fingerprint || recordedOperation != command.Operation {
			return ErrIdempotencyConflict
		}
		if recordedStatus != "COMPLETED" {
			return fmt.Errorf("%w: command in unexpected status %q", ErrCommandStorage, recordedStatus)
		}
		if err := replay(recordedOutcome); err != nil {
			return fmt.Errorf("%w: decode outcome: %v", ErrCommandStorage, err)
		}
		replayed = true
		return nil
	})
	return replayed, err
}
