package execution

import (
	"math/rand"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

// TestChoiceMergeExpressionEvaluatorProperty generates random inputs and operand
// comparisons to verify that:
// 1. All valid operators evaluate deterministically according to contracts.EvaluateChoice.
// 2. Mismatched operand types strictly fail with INVALID_EXPRESSION.
// 3. No panic occurs on arbitrary input or unexpected types.
func TestChoiceMergeExpressionEvaluatorProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	ops := []string{"eq", "neq", "gt", "gte", "lt", "lte", "in", "exists", "and", "or", "not"}

	for i := 0; i < 200; i++ {
		op := ops[rng.Intn(len(ops))]
		valA := float64(rng.Intn(100))
		valB := float64(rng.Intn(100))

		var expr map[string]any
		switch op {
		case "eq", "neq", "gt", "gte", "lt", "lte":
			expr = map[string]any{
				"op": op,
				"args": []any{
					map[string]any{"literal": valA},
					map[string]any{"literal": valB},
				},
			}
		case "in":
			expr = map[string]any{
				"op": "in",
				"args": []any{
					map[string]any{"literal": valA},
					map[string]any{"literal": []any{valA, valB, float64(999)}},
				},
			}
		case "exists":
			expr = map[string]any{
				"op": "exists",
				"args": []any{
					map[string]any{"literal": valA},
				},
			}
		case "and":
			expr = map[string]any{
				"op": "and",
				"args": []any{
					map[string]any{"op": "eq", "args": []any{map[string]any{"literal": valA}, map[string]any{"literal": valA}}},
					map[string]any{"op": "eq", "args": []any{map[string]any{"literal": valB}, map[string]any{"literal": valB}}},
				},
			}
		case "or":
			expr = map[string]any{
				"op": "or",
				"args": []any{
					map[string]any{"op": "eq", "args": []any{map[string]any{"literal": valA}, map[string]any{"literal": valB}}},
					map[string]any{"op": "eq", "args": []any{map[string]any{"literal": valA}, map[string]any{"literal": valA}}},
				},
			}
		case "not":
			expr = map[string]any{
				"op": "not",
				"args": []any{
					map[string]any{"op": "eq", "args": []any{map[string]any{"literal": valA}, map[string]any{"literal": valB}}},
				},
			}
		}

		res1, err1 := contracts.EvaluateChoice(expr, nil, nil)
		res2, err2 := contracts.EvaluateChoice(expr, nil, nil)

		if (err1 != nil) != (err2 != nil) {
			t.Fatalf("nondeterministic error for op %s: %v vs %v", op, err1, err2)
		}
		if err1 == nil && res1 != res2 {
			t.Fatalf("nondeterministic result for op %s: %v vs %v", op, res1, res2)
		}
	}
}

// TestChoiceMergeTypeMismatchProperty proves that mismatched operand types
// (e.g. number vs string, string vs array) always fail with INVALID_EXPRESSION.
func TestChoiceMergeTypeMismatchProperty(t *testing.T) {
	relOps := []string{"gt", "gte", "lt", "lte"}
	mismatched := []struct {
		a any
		b any
	}{
		{float64(10), "hello"},
		{"world", float64(20)},
		{true, float64(5)},
		{float64(5), true},
		{[]any{float64(1)}, float64(1)},
		{map[string]any{"x": float64(1)}, float64(1)},
	}

	for _, op := range relOps {
		for _, pair := range mismatched {
			expr := map[string]any{
				"op": op,
				"args": []any{
					map[string]any{"literal": pair.a},
					map[string]any{"literal": pair.b},
				},
			}
			_, err := contracts.EvaluateChoice(expr, nil, nil)
			if err == nil {
				t.Fatalf("expected error for %s with mismatched operands (%T, %T), got nil", op, pair.a, pair.b)
			}
			if cErr, ok := err.(*contracts.Error); !ok || cErr.Code != "INVALID_EXPRESSION" {
				t.Fatalf("expected INVALID_EXPRESSION, got %v", err)
			}
		}
	}
}

// TestChoiceMergeGraphRelationsProperty tests that buildGraphRelations and getBranchNodes
// properly partition nodes into non-overlapping branches for arbitrary branch depths.
func TestChoiceMergeGraphRelationsProperty(t *testing.T) {
	nodes := []workflowNode{
		{ID: "choice", Type: "choice", Choice: &choiceConfig{
			Branches: []choiceBranch{{Name: "b1"}, {Name: "b2"}},
			Default:  "b2",
		}},
		{ID: "t1_1", Type: "task", After: []string{"choice"}},
		{ID: "t1_2", Type: "task", After: []string{"t1_1"}},
		{ID: "t2_1", Type: "task", After: []string{"choice"}},
		{ID: "merge", Type: "merge", After: []string{"t1_2", "t2_1"}, Merge: &mergeConfig{
			Choice: "choice",
			Branches: []mergeBranch{
				{Branch: "b1", Terminal: "t1_2"},
				{Branch: "b2", Terminal: "t2_1"},
			},
		}},
	}

	ancestors, descendants := buildGraphRelations(nodes)

	if !descendants["choice"]["t1_1"] || !descendants["choice"]["t1_2"] {
		t.Fatal("expected t1 nodes to be descendants of choice")
	}
	if !descendants["choice"]["t2_1"] {
		t.Fatal("expected t2_1 to be descendant of choice")
	}
	if !ancestors["t1_2"]["t1_1"] || !ancestors["t1_2"]["choice"] {
		t.Fatal("expected t1_1 and choice to be ancestors of t1_2")
	}

	wf := &workflowManifest{Nodes: nodes}
	branches := getBranchNodes(wf, "choice", ancestors, descendants)

	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got %d", len(branches))
	}
	if !branches["b1"]["t1_1"] || !branches["b1"]["t1_2"] {
		t.Fatalf("expected t1_1 and t1_2 in b1, got %v", branches["b1"])
	}
	if branches["b1"]["t2_1"] {
		t.Fatal("t2_1 must not be in b1")
	}
	if !branches["b2"]["t2_1"] {
		t.Fatalf("expected t2_1 in b2, got %v", branches["b2"])
	}
	if branches["b2"]["t1_1"] || branches["b2"]["t1_2"] {
		t.Fatal("t1 nodes must not be in b2")
	}
}
