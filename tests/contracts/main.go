// Conformance driver only. This does not start a service or execute task code.
package main

import (
	"encoding/json"
	"fmt"
	c "github.com/Ryanakml/Deadbolt/internal/contracts"
	"os"
	"reflect"
)

func evaluate(f map[string]any) map[string]any {
	var value any
	var err error
	switch f["op"] {
	case "contract":
		value = c.ValidateAgainst(f["input"], f["schemaPath"].(string))
	case "digest":
		var canonical []byte
		var hash string
		canonical, hash, err = c.Digest([]byte(f["raw"].(string)))
		value = map[string]any{"canonical": string(canonical), "sha256": hash}
	case "mapping":
		value, err = c.MapInput(f["mapping"], f["input"], f["outputs"].(map[string]any))
	case "choice":
		value, err = c.EvaluateChoice(f["expression"], f["input"], f["outputs"].(map[string]any))
	case "schema":
		err = c.ValidatePayload(f["schema"], f["input"])
		value = true
	case "normalize":
		value, err = c.NormalizeTask(f["task"])
	case "workflow":
		err = c.ValidateWorkflow(f["manifest"], f["tasks"].([]any))
		value = true
	case "deployment":
		err = c.ValidateDeployment(f["manifest"])
		value = true
	default:
		panic("unknown fixture operation")
	}
	if err != nil {
		if e, ok := err.(*c.Error); ok {
			return map[string]any{"error": e.Code}
		}
		panic(err)
	}
	return map[string]any{"value": value}
}
func main() {
	raw, err := os.ReadFile("contracts/fixtures/conformance.json")
	if err != nil {
		panic(err)
	}
	var fixtures []map[string]any
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		panic(err)
	}
	results := map[string]any{}
	failed := false
	for _, f := range fixtures {
		got := evaluate(f)
		results[f["id"].(string)] = got
		if !reflect.DeepEqual(got, f["expected"]) {
			fmt.Fprintf(os.Stderr, "FAIL %s: got %v expected %v\n", f["id"], got, f["expected"])
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(results); err != nil {
		panic(err)
	}
}
