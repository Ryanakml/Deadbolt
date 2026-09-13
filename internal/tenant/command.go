package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

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

// withCommandTx claims, mutates, records and replays inside one transaction.
// The unique index serializes simultaneous claims: the loser blocks on the
// conflicting row, then reads the committed durable outcome.
func (s *Service) withCommandTx(ctx context.Context, orgID string, operation string, mutate func(context.Context, storage.Tx) error, outcome func() any, replay func(json.RawMessage) error) (bool, error) {
	command, ok := commandFromContext(ctx)
	if !ok {
		return false, s.pool.WithTenantTx(ctx, orgID, mutate)
	}
	if operation != "" && command.Operation != operation {
		return false, fmt.Errorf("%w: command operation mismatch", ErrCommandStorage)
	}

	replayed := false
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		var recordedFingerprint, recordedOperation string
		var recordedOutcome []byte
		err := tx.QueryRow(ctx, `
			INSERT INTO tenant_commands (command_scope, organization_id, idempotency_key, request_fingerprint, operation, outcome)
			VALUES ($1, NULLIF($2, '')::uuid, $3, $4, $5, '{}'::jsonb)
			ON CONFLICT (command_scope, idempotency_key) DO NOTHING
			RETURNING request_fingerprint
		`, command.Scope, orgID, command.Key, command.Fingerprint, command.Operation).Scan(&recordedFingerprint)
		if err == nil {
			if err := mutate(ctx, tx); err != nil {
				return err
			}
			encoded, err := json.Marshal(outcome())
			if err != nil {
				return fmt.Errorf("%w: encode outcome: %v", ErrCommandStorage, err)
			}
			if _, err := tx.Exec(ctx, `UPDATE tenant_commands SET outcome = $3::jsonb, completed_at = clock_timestamp() WHERE command_scope = $1 AND idempotency_key = $2`, command.Scope, command.Key, encoded); err != nil {
				return fmt.Errorf("%w: record outcome: %v", ErrCommandStorage, err)
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: claim command: %v", ErrCommandStorage, err)
		}

		// FOR UPDATE waits for the winning transaction before reading its outcome.
		if err := tx.QueryRow(ctx, `SELECT request_fingerprint, operation, outcome FROM tenant_commands WHERE command_scope = $1 AND idempotency_key = $2 FOR UPDATE`, command.Scope, command.Key).Scan(&recordedFingerprint, &recordedOperation, &recordedOutcome); err != nil {
			return fmt.Errorf("%w: load command: %v", ErrCommandStorage, err)
		}
		if recordedFingerprint != command.Fingerprint || recordedOperation != command.Operation {
			return ErrIdempotencyConflict
		}
		if err := replay(recordedOutcome); err != nil {
			return fmt.Errorf("%w: decode outcome: %v", ErrCommandStorage, err)
		}
		replayed = true
		return nil
	})
	return replayed, err
}
