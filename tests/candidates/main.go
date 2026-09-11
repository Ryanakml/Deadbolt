// SP-01 comparison harness; not a runtime component.
package main

import (
	"encoding/json"
	"fmt"
	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

func main() {
	cases := []string{`{"s":"<>&"}`, `{"\ue000":1,"😀":2}`, `{"a":1,"a":2}`, `"\ud800"`}
	nativeMismatch := 0
	for _, raw := range cases {
		var value any
		nativeErr := json.Unmarshal([]byte(raw), &value)
		native, _ := json.Marshal(value)
		selected, _, selectedErr := contracts.Digest([]byte(raw))
		if (nativeErr == nil) != (selectedErr == nil) || (selectedErr == nil && string(native) != string(selected)) {
			nativeMismatch++
		}
	}
	fmt.Printf("encoding/json alone: %d/%d adversarial cases disagree with the selected strict JCS contract\n", nativeMismatch, len(cases))
	if nativeMismatch != len(cases) {
		panic("candidate comparison changed; inspect the decision")
	}
}
