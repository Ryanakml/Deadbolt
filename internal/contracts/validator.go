package contracts

import "strconv"

func ValidateTask(task any) error {
	if err := checkJSON(task, 0); err != nil {
		return err
	}
	if !ValidateAgainst(task, "manifest/task.schema.json") {
		return failure("INVALID_TASK")
	}
	t := obj(task)
	for _, key := range []string{"inputSchema", "outputSchema"} {
		if err := ValidateSchema(t[key]); err != nil {
			return err
		}
	}
	retry := obj(t["retry"])
	if number(retry["initialDelayMs"], 1000) > number(retry["maxDelayMs"], 30000) {
		return failure("INVALID_TASK")
	}
	if t["recovery"] == "idempotent" && number(t["idempotencyWindowMs"], 0) < 5000+number(t["timeoutMs"], 300000) {
		return failure("INVALID_TASK")
	}
	return nil
}
func NormalizeTask(task any) (any, error) {
	if err := checkJSON(task, 0); err != nil {
		return nil, err
	}
	if err := ValidateTask(task); err != nil {
		return nil, err
	}
	out := map[string]any{}
	for k, v := range obj(task) {
		out[k] = v
	}
	out["timeoutMs"] = number(out["timeoutMs"], 300000)
	retry := map[string]any{"maxAttempts": float64(3), "initialDelayMs": float64(1000), "maxDelayMs": float64(30000)}
	for k, v := range obj(out["retry"]) {
		retry[k] = v
	}
	out["retry"] = retry
	return out, nil
}
func guaranteed(schema any, parts []string) bool {
	if len(parts) == 0 {
		return true
	}
	s := obj(schema)
	if variants := arr(s["oneOf"]); variants != nil {
		for _, v := range variants {
			if !guaranteed(v, parts) {
				return false
			}
		}
		return true
	}
	key := parts[0]
	if s["type"] == "object" {
		p := obj(s["properties"])
		value, ok := p[key]
		return ok && contains(arr(s["required"]), key) && guaranteed(value, parts[1:])
	}
	if s["type"] == "array" {
		i, err := strconv.Atoi(key)
		return err == nil && indexPattern.MatchString(key) && float64(i) < number(s["minItems"], 0) && s["items"] != nil && guaranteed(s["items"], parts[1:])
	}
	return false
}
func ValidateChoiceExpression(expr any, allowedAncestors map[string]bool) error {
	if expr == nil {
		return failure("INVALID_EXPRESSION")
	}
	v := obj(expr)
	if v == nil || len(v) != 2 {
		return failure("INVALID_EXPRESSION")
	}
	op := str(v["op"])
	args := arr(v["args"])
	if op == "" || args == nil {
		return failure("INVALID_EXPRESSION")
	}
	switch op {
	case "not":
		if len(args) != 1 {
			return failure("INVALID_EXPRESSION")
		}
		return ValidateChoiceExpression(args[0], allowedAncestors)
	case "and", "or":
		if len(args) < 1 {
			return failure("INVALID_EXPRESSION")
		}
		for _, a := range args {
			if err := ValidateChoiceExpression(a, allowedAncestors); err != nil {
				return err
			}
		}
		return nil
	case "exists":
		if len(args) != 1 {
			return failure("INVALID_EXPRESSION")
		}
		ref := obj(args[0])
		if ref == nil || Reference(ref) != nil {
			return failure("INVALID_EXPRESSION")
		}
		if ref["$ref"] == "step.output" && (allowedAncestors == nil || !allowedAncestors[str(ref["stepId"])]) {
			return failure("INVALID_EXPRESSION")
		}
		return nil
	case "eq", "neq", "gt", "gte", "lt", "lte", "in":
		if len(args) != 2 {
			return failure("INVALID_EXPRESSION")
		}
		checkArg := func(a any) error {
			if aObj := obj(a); aObj != nil {
				if _, ok := aObj["$ref"]; ok {
					if err := Reference(aObj); err != nil {
						return failure("INVALID_EXPRESSION")
					}
					if aObj["$ref"] == "step.output" && (allowedAncestors == nil || !allowedAncestors[str(aObj["stepId"])]) {
						return failure("INVALID_EXPRESSION")
					}
					return nil
				}
				if _, ok := aObj["literal"]; ok {
					if len(aObj) != 1 {
						return failure("INVALID_EXPRESSION")
					}
					return nil
				}
			}
			return nil
		}
		if err := checkArg(args[0]); err != nil {
			return err
		}
		return checkArg(args[1])
	default:
		return failure("INVALID_EXPRESSION")
	}
}

func ValidateWorkflow(manifest any, definitions []any) error {
	if err := checkJSON(manifest, 0); err != nil {
		return err
	}
	m := obj(manifest)
	if m == nil {
		return failure("INVALID_MANIFEST")
	}
	if m["manifestVersion"] != float64(1) {
		return failure("UNSUPPORTED_MANIFEST_VERSION")
	}
	nodes := arr(m["nodes"])
	if len(nodes) == 0 {
		return failure("EMPTY_NODES")
	}
	if len(nodes) > 50 {
		return failure("NODE_COUNT_EXCEEDED")
	}
	if !ValidateAgainst(m, "manifest/workflow.schema.json") {
		return failure("INVALID_MANIFEST")
	}
	for _, key := range []string{"inputSchema", "outputSchema"} {
		if err := ValidateSchema(m[key]); err != nil {
			return err
		}
	}
	tasks := map[string]map[string]any{}
	for _, t := range definitions {
		if err := ValidateTask(t); err != nil {
			return err
		}
		task := obj(t)
		name := str(task["name"])
		if tasks[name] != nil {
			return failure("INVALID_TASK")
		}
		tasks[name] = task
	}
	byID := map[string]map[string]any{}
	for _, v := range nodes {
		n := obj(v)
		id := str(n["id"])
		if byID[id] != nil {
			return failure("DUPLICATE_NODE_ID")
		}
		byID[id] = n
		ntype := str(n["type"])
		if ntype != "task" && ntype != "choice" && ntype != "merge" {
			return failure("UNSUPPORTED_CAPABILITY")
		}
		if ntype == "task" {
			if tasks[str(n["task"])] == nil {
				return failure("MISSING_TASK_REF")
			}
		}
	}
	for _, v := range nodes {
		for _, dep := range arr(obj(v)["after"]) {
			d := str(dep)
			if byID[d] == nil {
				return failure("MISSING_DEPENDENCY")
			}
		}
	}
	ancestors := map[string]map[string]bool{}
	visiting := map[string]bool{}
	var visit func(string) (map[string]bool, error)
	visit = func(id string) (map[string]bool, error) {
		if visiting[id] {
			return nil, failure("CYCLE_DETECTED")
		}
		if a := ancestors[id]; a != nil {
			return a, nil
		}
		visiting[id] = true
		set := map[string]bool{}
		for _, dep := range arr(byID[id]["after"]) {
			d := str(dep)
			set[d] = true
			other, err := visit(d)
			if err != nil {
				return nil, err
			}
			for x := range other {
				set[x] = true
			}
		}
		delete(visiting, id)
		ancestors[id] = set
		return set, nil
	}
	for _, v := range nodes {
		if _, err := visit(str(obj(v)["id"])); err != nil {
			return err
		}
	}

	descendants := map[string]map[string]bool{}
	for id := range byID {
		descendants[id] = map[string]bool{}
	}
	for id, ancSet := range ancestors {
		for anc := range ancSet {
			descendants[anc][id] = true
		}
	}

	choices := map[string]map[string]any{}
	merges := map[string]map[string]any{}
	mergeForChoice := map[string]string{}
	for _, v := range nodes {
		n := obj(v)
		id := str(n["id"])
		if n["type"] == "choice" {
			choices[id] = n
		} else if n["type"] == "merge" {
			merges[id] = n
		}
	}

	for id, n := range choices {
		c := obj(n["choice"])
		if c == nil {
			return failure("INVALID_MANIFEST")
		}
		branches := arr(c["branches"])
		def := str(c["default"])
		if len(branches) == 0 || (len(branches) < 2 && def == "") {
			return failure("INVALID_CHOICE")
		}
		branchNames := map[string]bool{}
		for _, b := range branches {
			bObj := obj(b)
			if bObj == nil {
				return failure("INVALID_CHOICE")
			}
			name := str(bObj["name"])
			if name == "" || branchNames[name] {
				return failure("INVALID_CHOICE")
			}
			branchNames[name] = true
			if cond := bObj["condition"]; cond != nil {
				if err := ValidateChoiceExpression(cond, ancestors[id]); err != nil {
					return err
				}
			}
		}
		if def != "" {
			branchNames[def] = true
		}
	}

	for id, n := range merges {
		mrg := obj(n["merge"])
		if mrg == nil {
			return failure("INVALID_MANIFEST")
		}
		choiceID := str(mrg["choice"])
		choiceNode := choices[choiceID]
		if choiceNode == nil {
			return failure("INVALID_MERGE")
		}
		if !ancestors[id][choiceID] {
			return failure("INVALID_MERGE")
		}
		if mergeForChoice[choiceID] != "" {
			return failure("INVALID_MERGE")
		}
		mergeForChoice[choiceID] = id

		mBranches := arr(mrg["branches"])
		if len(mBranches) == 0 {
			return failure("INVALID_MERGE")
		}
		c := obj(choiceNode["choice"])
		cBranches := arr(c["branches"])
		cNames := map[string]bool{}
		for _, b := range cBranches {
			cNames[str(obj(b)["name"])] = true
		}
		if def := str(c["default"]); def != "" {
			cNames[def] = true
		}

		mNames := map[string]bool{}
		for _, b := range mBranches {
			bObj := obj(b)
			if bObj == nil {
				return failure("INVALID_MERGE")
			}
			bName := str(bObj["branch"])
			term := str(bObj["terminal"])
			if bName == "" || term == "" || !cNames[bName] || mNames[bName] {
				return failure("INVALID_MERGE")
			}
			if byID[term] == nil || !ancestors[id][term] || !descendants[choiceID][term] {
				return failure("INVALID_MERGE")
			}
			mNames[bName] = true
		}
		for cn := range cNames {
			if !mNames[cn] {
				return failure("INVALID_MERGE")
			}
		}
		if mrg["outputSchema"] != nil {
			if err := ValidateSchema(mrg["outputSchema"]); err != nil {
				return err
			}
		}
	}
	for id := range choices {
		if mergeForChoice[id] == "" {
			return failure("INVALID_CHOICE")
		}
	}

	choiceBranchNodeSets := map[string]map[string]map[string]bool{}
	for choiceID := range choices {
		mergeID := mergeForChoice[choiceID]
		mrg := obj(merges[mergeID]["merge"])
		mBranches := arr(mrg["branches"])
		choiceBranchNodeSets[choiceID] = map[string]map[string]bool{}

		for _, b := range mBranches {
			bObj := obj(b)
			bName := str(bObj["branch"])
			term := str(bObj["terminal"])

			bSet := map[string]bool{}
			for nodeID := range byID {
				if nodeID == choiceID || nodeID == mergeID {
					continue
				}
				if descendants[choiceID][nodeID] && (nodeID == term || ancestors[term][nodeID]) {
					bSet[nodeID] = true
				}
			}
			if len(bSet) == 0 {
				return failure("INVALID_BRANCH")
			}
			choiceBranchNodeSets[choiceID][bName] = bSet
		}

		// Check for cross-branch dependency before overlap check
		for i := 0; i < len(mBranches); i++ {
			t_i := str(obj(mBranches[i])["terminal"])
			nodesI := map[string]bool{t_i: true}
			for anc := range ancestors[t_i] {
				if anc != choiceID && !ancestors[choiceID][anc] {
					nodesI[anc] = true
				}
			}
			for j := 0; j < len(mBranches); j++ {
				if i == j {
					continue
				}
				t_j := str(obj(mBranches[j])["terminal"])
				nodesJ := map[string]bool{t_j: true}
				for anc := range ancestors[t_j] {
					if anc != choiceID && !ancestors[choiceID][anc] {
						nodesJ[anc] = true
					}
				}
				for v := range nodesJ {
					for _, dep := range arr(byID[v]["after"]) {
						d := str(dep)
						if nodesI[d] {
							return failure("CROSS_BRANCH_DEPENDENCY")
						}
					}
				}
			}
		}

		branchList := make([]string, 0, len(choiceBranchNodeSets[choiceID]))
		for bName := range choiceBranchNodeSets[choiceID] {
			branchList = append(branchList, bName)
		}
		for i := 0; i < len(branchList); i++ {
			for j := i + 1; j < len(branchList); j++ {
				set1 := choiceBranchNodeSets[choiceID][branchList[i]]
				set2 := choiceBranchNodeSets[choiceID][branchList[j]]
				for u := range set1 {
					if set2[u] {
						return failure("IRREDUCIBLE_GRAPH")
					}
				}
			}
		}

		for bName1, bSet1 := range choiceBranchNodeSets[choiceID] {
			for u := range bSet1 {
				for _, dep := range arr(byID[u]["after"]) {
					d := str(dep)
					for bName2, bSet2 := range choiceBranchNodeSets[choiceID] {
						if bName1 != bName2 && bSet2[d] {
							return failure("CROSS_BRANCH_DEPENDENCY")
						}
					}
				}
			}
		}

		for _, bSet := range choiceBranchNodeSets[choiceID] {
			for u := range bSet {
				for _, dep := range arr(byID[u]["after"]) {
					d := str(dep)
					if d == choiceID || bSet[d] || ancestors[choiceID][d] {
						continue
					}
					return failure("CROSS_BRANCH_DEPENDENCY")
				}
				for w := range byID {
					for _, d := range arr(byID[w]["after"]) {
						if str(d) == u {
							if w != mergeID && !bSet[w] {
								return failure("IRREDUCIBLE_GRAPH")
							}
						}
					}
				}
			}
		}
	}

	for choiceID := range choices {
		depth := 1
		for otherChoiceID, bMap := range choiceBranchNodeSets {
			if otherChoiceID == choiceID {
				continue
			}
			for _, bSet := range bMap {
				if bSet[choiceID] {
					depth++
				}
			}
		}
		if depth > 8 {
			return failure("CHOICE_NESTING_EXCEEDED")
		}
	}

	nodeAllowed := map[string]map[string]bool{}
	for id := range byID {
		allowed := map[string]bool{}
		for anc := range ancestors[id] {
			allowed[anc] = true
		}
		for choiceID, bMap := range choiceBranchNodeSets {
			mergeID := mergeForChoice[choiceID]
			for _, bSet := range bMap {
				if id == mergeID {
					continue
				}
				if !bSet[id] {
					for bNode := range bSet {
						delete(allowed, bNode)
					}
				}
			}
		}
		nodeAllowed[id] = allowed
	}

	used := map[string]bool{}
	var mapping func(any, map[string]bool, bool) error
	mapping = func(value any, allowed map[string]bool, isOutput bool) error {
		switch v := value.(type) {
		case []any:
			for _, x := range v {
				if err := mapping(x, allowed, isOutput); err != nil {
					return err
				}
			}
		case map[string]any:
			if _, ok := v["literal"]; ok {
				if len(v) != 1 {
					return failure("INPUT_MAPPING_ERROR")
				}
				return nil
			}
			if _, ok := v["$ref"]; ok {
				if err := Reference(v); err != nil {
					return err
				}
				source := m["inputSchema"]
				if v["$ref"] == "step.output" {
					id := str(v["stepId"])
					if !allowed[id] {
						return failure("INPUT_MAPPING_ERROR")
					}
					if byID[id]["type"] == "merge" {
						mrg := obj(byID[id]["merge"])
						if mrg != nil && mrg["outputSchema"] != nil {
							source = mrg["outputSchema"]
						} else {
							source = map[string]any{
								"type": "object",
								"properties": map[string]any{
									"branch": map[string]any{"type": "string"},
									"value":  map[string]any{},
								},
								"required": []any{"branch", "value"},
							}
						}
					} else if byID[id]["type"] == "choice" {
						source = map[string]any{
							"type": "object",
							"properties": map[string]any{
								"selected": map[string]any{"type": "string"},
								"branch":   map[string]any{"type": "string"},
							},
							"required": []any{"selected", "branch"},
						}
					} else {
						source = tasks[str(byID[id]["task"])]["outputSchema"]
					}
					if isOutput {
						used[id] = true
					}
				}
				parts, _ := PointerParts(v["pointer"])
				_, hasDefault := v["default"]
				if !guaranteed(source, parts) && !hasDefault {
					return failure("INPUT_MAPPING_ERROR")
				}
				return nil
			}
			for _, x := range v {
				if err := mapping(x, allowed, isOutput); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for _, v := range nodes {
		n := obj(v)
		id := str(n["id"])
		if err := mapping(n["input"], nodeAllowed[id], false); err != nil {
			return err
		}
		if n["type"] == "merge" {
			mrg := obj(n["merge"])
			for _, b := range arr(mrg["branches"]) {
				bObj := obj(b)
				if val := bObj["value"]; val != nil {
					if err := mapping(val, ancestors[id], false); err != nil {
						return err
					}
				}
			}
		}
	}
	allOutputsAllowed := map[string]bool{}
	for id := range byID {
		allOutputsAllowed[id] = true
	}
	for _, bMap := range choiceBranchNodeSets {
		for _, bSet := range bMap {
			for bNode := range bSet {
				delete(allOutputsAllowed, bNode)
			}
		}
	}
	if err := mapping(m["output"], allOutputsAllowed, true); err != nil {
		return err
	}
	referencedAsDependency := map[string]bool{}
	for _, v := range nodes {
		for _, dep := range arr(obj(v)["after"]) {
			referencedAsDependency[str(dep)] = true
		}
	}
	for _, v := range nodes {
		n := obj(v)
		id := str(n["id"])
		if !referencedAsDependency[id] && !used[id] && n["sideEffect"] != true {
			return failure("ORPHAN_LEAF")
		}
	}
	return nil
}
func ValidateDeployment(v any) error {
	if err := checkJSON(v, 0); err != nil {
		return err
	}
	if !ValidateAgainst(v, "manifest/deployment.schema.json") {
		return failure("INVALID_MANIFEST")
	}
	m := obj(v)
	names := map[string]bool{}
	for _, w := range arr(m["workflows"]) {
		name := str(obj(w)["name"])
		if names[name] {
			return failure("INVALID_MANIFEST")
		}
		names[name] = true
		if err := ValidateWorkflow(w, arr(m["tasks"])); err != nil {
			return err
		}
	}
	return nil
}
