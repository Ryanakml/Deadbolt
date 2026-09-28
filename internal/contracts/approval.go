package contracts

// Approval control-node contract (Blueprint §16.3, §24.2).
//
// These constants live outside the generated enums file so regenerating
// `scripts/generate-contracts.mjs` output never drops approval semantics.

// ApprovalDecisionCapability is the only permission that may gate an approval
// decision. Blueprint §24.2 grants approval decisions to Operator, Admin, and
// Owner roles only, and §24.2 explicitly withholds approval/reconciliation
// machine keys in V1: a decision requires an identifiable human actor.
const ApprovalDecisionCapability = "approvals:decide"

// Approval wait bounds (Blueprint §15.2).
const (
	// ApprovalDefaultWaitMs is the default approval wait: 24 hours.
	ApprovalDefaultWaitMs int64 = 24 * 60 * 60 * 1000
	// ApprovalMaxWaitMs bounds a declared approval wait. A declared wait longer
	// than the remaining run lifetime is clamped by the engine rather than
	// rejected, because the run deadline is the real upper bound.
	ApprovalMaxWaitMs int64 = 30 * 24 * 60 * 60 * 1000
)

// Approval decision values as they appear in step output and API payloads
// (Blueprint §16.3). Both approve and reject are successful step outcomes:
// the node's job was to collect a valid human decision, not to make the
// business outcome.
const (
	ApprovalDecisionApproved = "approved"
	ApprovalDecisionRejected = "rejected"
)

// Approval node config is the persisted contract for an approval control node.
// The shape is a closed subset of what the manifest schema accepts.
type ApprovalNodeConfig struct {
	// Payload is the human-readable request shown to the approver.
	Payload any `json:"payload"`
	// OutputSchema validates the committed decision output.
	OutputSchema any `json:"outputSchema"`
	// RequiredPermission is optional; it defaults to
	// ApprovalDecisionCapability and may not name any other capability.
	RequiredPermission string `json:"requiredPermission"`
	// ExpiresInMs optionally shortens the 24h default wait.
	ExpiresInMs int64 `json:"expiresInMs"`
}
