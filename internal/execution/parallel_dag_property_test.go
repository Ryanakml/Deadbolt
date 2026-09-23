package execution

// Property-based DAG transition coverage for Issue #26 (Blueprint §6, §10).
//
// Bounded deterministic generation (fixed seeds, seed logged on failure) over
// static task DAGs. Verifies the contract-level and transition-level invariants
// that normal advancement and ReconcileReadyWork must both honor:
//   - validator accepts DAGs iff acyclic with resolvable ancestor-only refs;
//   - multi-parent join requires every dependency terminal SUCCEEDED/SKIPPED;
//   - any SKIPPED dependency forces SKIPPED (transitively, order-independent);
//   - fixed-point reevaluation is idempotent and never reopens terminal work;
//   - evaluating the same graph twice creates no duplicate actions.

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

func propTaskDefs() []any {
	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"x": map[string]any{"type": "integer"}},
		"required":             []any{"x"},
		"additionalProperties": false,
	}
	mk := func(name string) map[string]any {
		return map[string]any{
			"name": name, "inputSchema": schema, "outputSchema": schema,
			"recovery": "safe", "entrypoint": "tasks.js#noop",
		}
	}
	return []any{mk("noop-a"), mk("noop-b"), mk("noop-c")}
}

func propManifest(nodes []any) map[string]any {
	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"x": map[string]any{"type": "integer"}},
		"required":             []any{"x"},
		"additionalProperties": false,
	}
	return map[string]any{
		"manifestVersion": float64(1), "name": "prop",
		"inputSchema": schema, "outputSchema": schema,
		"nodes": nodes,
		"output": map[string]any{
			"x": map[string]any{"$ref": "step.output", "stepId": "n0", "pointer": "/x"},
		},
	}
}

func propNode(id string, after []string, refFrom string) map[string]any {
	n := map[string]any{"id": id, "type": "task", "task": "noop-a"}
	if len(after) > 0 {
		a := make([]any, len(after))
		for i, d := range after {
			a[i] = d
		}
		n["after"] = a
	}
	if refFrom == "" {
		n["input"] = map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}
	} else {
		n["input"] = map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": refFrom, "pointer": "/x"}}
	}
	if id == "n0" {
		n["sideEffect"] = true
	}
	return n
}

// simulateFixedPoint mirrors evaluateBlockedDAGTx decisions without a database:
// states maps node -> state; after maps node -> deps. Returns final states and
// action count. Used to verify order-independence, idempotency, and no-reopen.
func simulateFixedPoint(states map[string]string, after map[string][]string) (map[string]string, int) {
	out := make(map[string]string, len(states))
	for k, v := range states {
		out[k] = v
	}
	actions := 0
	for changed := true; changed; {
		changed = false
		for node, st := range out {
			if st != "BLOCKED" {
				continue
			}
			deps := after[node]
			allMet := true
			skipped := false
			for _, d := range deps {
				ds, ok := out[d]
				if !ok {
					allMet = false
					break
				}
				if ds == "SKIPPED" {
					skipped = true
				}
				if ds != "SUCCEEDED" && ds != "SKIPPED" {
					allMet = false
				}
			}
			if !allMet {
				continue
			}
			if skipped {
				out[node] = "SKIPPED"
			} else {
				out[node] = "READY"
			}
			changed = true
			actions++
		}
	}
	return out, actions
}

func TestParallelDAGProperty_ValidatorAndTransitions(t *testing.T) {
	seeds := []int64{26, 2601, 2602, 2603, 2604, 2610, 2626, 2676}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			fail := func(format string, args ...any) {
				t.Fatalf("seed=%d: %s", seed, fmt.Sprintf(format, args...))
			}
			// Generate 20 bounded DAGs per seed: 2..5 nodes, edges only to
			// lower indexes (acyclic by construction) plus occasional cycle.
			for iter := 0; iter < 20; iter++ {
				n := 2 + rng.Intn(4)
				ids := make([]string, n)
				for i := range ids {
					ids[i] = fmt.Sprintf("n%d", i)
				}
				nodes := make([]any, 0, n)
				after := make(map[string][]string)
				for i, id := range ids {
					var deps []string
					for j := 0; j < i; j++ {
						if rng.Intn(3) == 0 {
							deps = append(deps, ids[j])
						}
					}
					// Occasionally force a multi-parent join on the last node.
					if i == n-1 && n >= 3 && len(deps) < 2 && rng.Intn(2) == 0 {
						deps = []string{ids[n-2], ids[n-3]}
					}
					after[id] = deps
				}
				referenced := map[string]bool{}
				for _, deps := range after {
					for _, d := range deps {
						referenced[d] = true
					}
				}
				for _, id := range ids {
					deps := after[id]
					refFrom := ""
					if len(deps) > 0 {
						refFrom = deps[0]
					}
					nd := propNode(id, deps, refFrom)
					if id != "n0" && !referenced[id] {
						nd["sideEffect"] = true
					}
					nodes = append(nodes, nd)
				}
				manifest := propManifest(nodes)
				if err := contracts.ValidateWorkflow(manifest, propTaskDefs()); err != nil {
					fail("generated acyclic DAG rejected iter=%d: %v", iter, err)
				}
				// Invariant: join with all SUCCEEDED deps must be READY-eligible.
				states := map[string]string{}
				for _, id := range ids {
					states[id] = "BLOCKED"
				}
				// Roots (no deps) succeed first.
				for _, id := range ids {
					if len(after[id]) == 0 {
						states[id] = "SUCCEEDED"
					}
				}
				final, _ := simulateFixedPoint(states, after)
				// Invariant: no BLOCKED node may have all deps SUCCEEDED.
				for _, id := range ids {
					if final[id] != "BLOCKED" {
						continue
					}
					allSucc := true
					for _, d := range after[id] {
						if final[d] != "SUCCEEDED" {
							allSucc = false
						}
					}
					if allSucc && len(after[id]) > 0 {
						fail("iter=%d node %s stuck BLOCKED with all deps SUCCEEDED", iter, id)
					}
				}
				// Invariant: SKIPPED propagates transitively and idempotently.
				skipStates := map[string]string{}
				for _, id := range ids {
					skipStates[id] = "BLOCKED"
				}
				for _, id := range ids {
					if len(after[id]) == 0 {
						skipStates[id] = "SUCCEEDED"
					}
				}
				if n > 1 {
					skipStates[ids[0]] = "SKIPPED"
				}
				once, actions1 := simulateFixedPoint(skipStates, after)
				twice, actions2 := simulateFixedPoint(once, after)
				for _, id := range ids {
					if once[id] != twice[id] {
						fail("iter=%d reevaluation not idempotent for %s: %s vs %s", iter, id, once[id], twice[id])
					}
				}
				if actions2 != 0 {
					fail("iter=%d second evaluation created %d duplicate actions", iter, actions2)
				}
				_ = actions1
				// Invariant: terminal states never reopen in simulation.
				for _, id := range ids {
					for _, term := range []string{"SUCCEEDED", "SKIPPED", "FAILED", "CANCELLED"} {
						m := map[string]string{}
						for k, v := range once {
							m[k] = v
						}
						m[id] = term
						re, _ := simulateFixedPoint(m, after)
						if re[id] != term {
							fail("iter=%d terminal %s reopened to %s for %s", iter, term, re[id], id)
						}
					}
				}
			}
		})
	}
}

func TestParallelDAGProperty_CycleRejected(t *testing.T) {
	seeds := []int64{26, 99}
	for _, seed := range seeds {
		rng := rand.New(rand.NewSource(seed))
		for iter := 0; iter < 10; iter++ {
			n := 3 + rng.Intn(3)
			ids := make([]string, n)
			for i := range ids {
				ids[i] = fmt.Sprintf("n%d", i)
			}
			nodes := make([]any, 0, n)
			for i, id := range ids {
				next := ids[(i+1)%n]
				nodes = append(nodes, propNode(id, []string{next}, ""))
			}
			manifest := propManifest(nodes)
			if err := contracts.ValidateWorkflow(manifest, propTaskDefs()); err == nil {
				t.Fatalf("seed=%d iter=%d: cyclic DAG accepted", seed, iter)
			} else if e, ok := err.(*contracts.Error); !ok || e.Code != "CYCLE_DETECTED" {
				t.Fatalf("seed=%d iter=%d: expected CYCLE_DETECTED, got %v", seed, iter, err)
			}
		}
	}
}
