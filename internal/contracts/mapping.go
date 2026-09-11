package contracts

import (
	"regexp"
	"strconv"
	"strings"
)

var pointerPattern = regexp.MustCompile(`^(?:/(?:[^~]|~[01])*)*$`)
var indexPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

func PointerParts(v any) ([]string, error) {
	s, ok := v.(string)
	if !ok || !pointerPattern.MatchString(s) {
		return nil, failure("INPUT_MAPPING_ERROR")
	}
	if s == "" {
		return []string{}, nil
	}
	parts := strings.Split(s[1:], "/")
	for i, s := range parts {
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
	}
	return parts, nil
}
func Lookup(value any, pointer any) (any, bool, error) {
	parts, err := PointerParts(pointer)
	if err != nil {
		return nil, false, err
	}
	current := value
	for _, key := range parts {
		switch x := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = x[key]
			if !ok {
				return nil, false, nil
			}
		case []any:
			if !indexPattern.MatchString(key) {
				return nil, false, nil
			}
			i, err := strconv.Atoi(key)
			if err != nil || i >= len(x) {
				return nil, false, nil
			}
			current = x[i]
		default:
			return nil, false, nil
		}
	}
	return current, true, nil
}
func Reference(v map[string]any) error {
	ref := str(v["$ref"])
	if ref != "run.input" && ref != "step.output" {
		return failure("INPUT_MAPPING_ERROR")
	}
	for key := range v {
		if key != "$ref" && key != "pointer" && key != "default" && !(key == "stepId" && ref == "step.output") {
			return failure("INPUT_MAPPING_ERROR")
		}
	}
	if ref == "step.output" {
		if _, ok := v["stepId"].(string); !ok {
			return failure("INPUT_MAPPING_ERROR")
		}
	}
	_, err := PointerParts(v["pointer"])
	return err
}
func MapInput(mapping, input any, outputs map[string]any) (any, error) {
	for _, v := range []any{mapping, input, outputs} {
		if err := checkJSON(v, 0); err != nil {
			return nil, err
		}
	}
	var walk func(any) (any, error)
	walk = func(value any) (any, error) {
		switch v := value.(type) {
		case []any:
			out := make([]any, len(v))
			for i, x := range v {
				y, err := walk(x)
				if err != nil {
					return nil, err
				}
				out[i] = y
			}
			return out, nil
		case map[string]any:
			if literal, ok := v["literal"]; ok {
				if len(v) != 1 {
					return nil, failure("INPUT_MAPPING_ERROR")
				}
				return literal, nil
			}
			if _, ok := v["$ref"]; ok {
				if err := Reference(v); err != nil {
					return nil, err
				}
				source := input
				found := true
				if v["$ref"] == "step.output" {
					source, found = outputs[str(v["stepId"])]
				}
				if found {
					val, ok, err := Lookup(source, v["pointer"])
					if err != nil {
						return nil, err
					}
					if ok {
						return val, nil
					}
				}
				if def, ok := v["default"]; ok {
					return def, nil
				}
				return nil, failure("INPUT_MAPPING_ERROR")
			}
			out := map[string]any{}
			for k, x := range v {
				y, err := walk(x)
				if err != nil {
					return nil, err
				}
				out[k] = y
			}
			return out, nil
		default:
			return value, nil
		}
	}
	return walk(mapping)
}
func EvaluateChoice(expression, input any, outputs map[string]any) (bool, error) {
	if err := checkJSON(expression, 0); err != nil {
		return false, err
	}
	var walk func(any) (bool, error)
	walk = func(e any) (bool, error) {
		v := obj(e)
		args := arr(v["args"])
		op := str(v["op"])
		bad := failure("INVALID_EXPRESSION")
		if len(v) != 2 || args == nil {
			return false, bad
		}
		if op == "and" || op == "or" || op == "not" {
			if (op == "not" && len(args) != 1) || (op != "not" && len(args) < 1) {
				return false, bad
			}
			result := op == "and"
			for _, a := range args {
				b, err := walk(a)
				if err != nil {
					return false, err
				}
				switch op {
				case "not":
					result = !b
				case "and":
					result = result && b
				case "or":
					result = result || b
				}
			}
			return result, nil
		}
		if op == "exists" {
			if len(args) != 1 {
				return false, bad
			}
			ref := obj(args[0])
			if err := Reference(ref); err != nil {
				return false, err
			}
			_, err := MapInput(ref, input, outputs)
			if err != nil {
				if e, ok := err.(*Error); ok && e.Code == "INPUT_MAPPING_ERROR" {
					return false, nil
				}
				return false, err
			}
			return true, nil
		}
		if len(args) != 2 {
			return false, bad
		}
		if op != "eq" && op != "neq" && op != "gt" && op != "gte" && op != "lt" && op != "lte" && op != "in" {
			return false, bad
		}
		a, err := MapInput(args[0], input, outputs)
		if err != nil {
			return false, err
		}
		b, err := MapInput(args[1], input, outputs)
		if err != nil {
			return false, err
		}
		equal := func(a, b any) (bool, error) {
			switch x := a.(type) {
			case nil:
				if b == nil {
					return true, nil
				}
			case string:
				y, ok := b.(string)
				if ok {
					return x == y, nil
				}
			case bool:
				y, ok := b.(bool)
				if ok {
					return x == y, nil
				}
			case float64:
				y, ok := b.(float64)
				if ok {
					return x == y, nil
				}
			case []any:
				if _, ok := b.([]any); ok {
					left, _ := CanonicalizeGeneric(a)
					right, _ := CanonicalizeGeneric(b)
					return string(left) == string(right), nil
				}
			case map[string]any:
				if _, ok := b.(map[string]any); ok {
					left, _ := CanonicalizeGeneric(a)
					right, _ := CanonicalizeGeneric(b)
					return string(left) == string(right), nil
				}
			}
			return false, bad
		}
		if op == "in" {
			list, ok := b.([]any)
			if !ok {
				return false, bad
			}
			result := false
			for _, x := range list {
				eq, err := equal(a, x)
				if err != nil {
					return false, err
				}
				result = result || eq
			}
			return result, nil
		}
		if op == "eq" || op == "neq" {
			eq, err := equal(a, b)
			if op == "neq" {
				eq = !eq
			}
			return eq, err
		}
		x, ok := a.(float64)
		y, other := b.(float64)
		if !ok || !other {
			return false, bad
		}
		switch op {
		case "gt":
			return x > y, nil
		case "gte":
			return x >= y, nil
		case "lt":
			return x < y, nil
		default:
			return x <= y, nil
		}
	}
	return walk(expression)
}
