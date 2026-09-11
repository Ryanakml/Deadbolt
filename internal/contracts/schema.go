package contracts

import (
	"encoding/json"
	assets "github.com/Ryanakml/Deadbolt/contracts"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"sync"
)

var schemaCache sync.Map

func ValidateAgainst(value any, path string) bool {
	cached, ok := schemaCache.Load(path)
	if !ok {
		raw, err := assets.Files.ReadFile(path)
		if err != nil {
			return false
		}
		var doc any
		if json.Unmarshal(raw, &doc) != nil {
			return false
		}
		c := jsonschema.NewCompiler()
		c.DefaultDraft(jsonschema.Draft2020)
		c.AssertFormat()
		if c.AddResource("urn:runtime:check", doc) != nil {
			return false
		}
		s, err := c.Compile("urn:runtime:check")
		if err != nil {
			return false
		}
		cached = s
		schemaCache.Store(path, s)
	}
	return cached.(*jsonschema.Schema).Validate(value) == nil
}
func obj(v any) map[string]any { x, _ := v.(map[string]any); return x }
func arr(v any) []any          { x, _ := v.([]any); return x }
func str(v any) string         { x, _ := v.(string); return x }
func number(v any, defaultValue float64) float64 {
	x, ok := v.(float64)
	if !ok {
		return defaultValue
	}
	return x
}
func contains(a []any, s string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}
	return false
}
func ValidateSchema(v any) error {
	if err := checkJSON(v, 0); err != nil {
		return err
	}
	compact, err := CanonicalizeGeneric(v)
	if err != nil {
		return err
	}
	if len(compact) > 65536 {
		return failure("SCHEMA_SIZE_EXCEEDED")
	}
	if !ValidateAgainst(v, "manifest/payload-schema.schema.json") {
		return failure("UNSUPPORTED_SCHEMA")
	}
	var visit func(map[string]any) error
	visit = func(s map[string]any) error {
		if variants := arr(s["oneOf"]); variants != nil {
			tagged := false
			for key := range obj(obj(variants[0])["properties"]) {
				tags := map[string]bool{}
				valid := true
				for _, variant := range variants {
					a := obj(variant)
					field := obj(obj(a["properties"])[key])
					value, has := field["const"]
					if !contains(arr(a["required"]), key) || !has {
						valid = false
						break
					}
					b, _ := CanonicalizeGeneric(value)
					tags[string(b)] = true
				}
				if valid && len(tags) == len(variants) {
					tagged = true
				}
			}
			if !tagged {
				return failure("UNSUPPORTED_SCHEMA")
			}
			for _, a := range variants {
				if err := visit(obj(a)); err != nil {
					return err
				}
			}
		}
		for _, a := range obj(s["properties"]) {
			if err := visit(obj(a)); err != nil {
				return err
			}
		}
		for _, key := range []string{"items", "additionalProperties"} {
			if a := obj(s[key]); a != nil {
				if err := visit(a); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(obj(v))
}

func ValidatePayload(schema, value any) error {
	if err := ValidateSchema(schema); err != nil {
		return err
	}
	if err := checkJSON(value, 0); err != nil {
		return err
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	if c.AddResource("urn:runtime:input", schema) != nil {
		return failure("UNSUPPORTED_SCHEMA")
	}
	s, err := c.Compile("urn:runtime:input")
	if err != nil {
		return failure("UNSUPPORTED_SCHEMA")
	}
	if s.Validate(value) != nil {
		return failure("SCHEMA_VALIDATION_ERROR")
	}
	return nil
}
