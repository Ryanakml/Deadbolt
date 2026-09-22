package artifacts

import "testing"

func TestArtifactRefShape(t *testing.T) {
	id, ok := IsArtifactRef(map[string]any{"$artifact": "abc"})
	if !ok || id != "abc" {
		t.Fatalf("typed reference not recognized: %v %v", id, ok)
	}
	for _, v := range []any{
		nil, "abc", 42,
		map[string]any{"$artifact": "abc", "extra": 1},
		map[string]any{"$artifact": 42},
		map[string]any{"$ref": "step.output"},
	} {
		if _, ok := IsArtifactRef(v); ok {
			t.Fatalf("non-reference accepted: %v", v)
		}
	}
}

func TestCollectArtifactRefsFindsNested(t *testing.T) {
	v := map[string]any{
		"doc":  map[string]any{"$artifact": "a1"},
		"list": []any{map[string]any{"$artifact": "a2"}, "plain"},
	}
	got := CollectArtifactRefs(v)
	if len(got) != 2 || got[0] != "a1" || got[1] != "a2" {
		t.Fatalf("unexpected refs: %v", got)
	}
	if len(CollectArtifactRefs(map[string]any{"ok": true})) != 0 {
		t.Fatalf("clean payload must yield no refs")
	}
	if CollectArtifactRefs(nil) != nil {
		t.Fatalf("nil payload must yield no refs")
	}
}
