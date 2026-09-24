import { assertJSON, ContractError, fail, type JSONValue } from "./json.js";
import { schemas } from "./schema-data.js";
import {
  object,
  validateAgainst,
  validateSchema,
  type ObjectValue,
} from "./schema.js";
import { pointerParts, reference } from "./mapping.js";
export interface ValidationError {
  code: string;
  message: string;
  path?: string;
}
export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}
const check = (fn: () => void): ValidationResult => {
  try {
    fn();
    return { valid: true, errors: [] };
  } catch (e) {
    if (e instanceof ContractError)
      return { valid: false, errors: [{ code: e.code, message: e.message }] };
    throw e;
  }
};
export function validateTask(task: JSONValue): void {
  assertJSON(task);
  if (!validateAgainst(task, schemas["task.schema.json"])) fail("INVALID_TASK");
  const t = object(task);
  validateSchema(t.inputSchema, schemas["payload-schema.schema.json"]);
  validateSchema(t.outputSchema, schemas["payload-schema.schema.json"]);
  const retry = object(t.retry ?? {});
  if (Number(retry.initialDelayMs ?? 1000) > Number(retry.maxDelayMs ?? 30000))
    fail("INVALID_TASK");
  if (
    t.recovery === "idempotent" &&
    Number(t.idempotencyWindowMs) < 5000 + Number(t.timeoutMs ?? 300000)
  )
    fail("INVALID_TASK");
}
export function normalizeTask(task: JSONValue): ObjectValue {
  assertJSON(task);
  validateTask(task);
  const t = object(task);
  return {
    ...t,
    timeoutMs: t.timeoutMs ?? 300000,
    retry: {
      maxAttempts: 3,
      initialDelayMs: 1000,
      maxDelayMs: 30000,
      ...object(t.retry ?? {}),
    },
  };
}
function guaranteed(schema: JSONValue, parts: string[]): boolean {
  if (!parts.length) return true;
  const s = object(schema);
  if (s.oneOf)
    return (s.oneOf as JSONValue[]).every((x) => guaranteed(x, parts));
  const [key, ...rest] = parts;
  if (s.type === "object") {
    const p = object(s.properties ?? {});
    return (
      Array.isArray(s.required) &&
      s.required.includes(key) &&
      Object.hasOwn(p, key) &&
      guaranteed(p[key], rest)
    );
  }
  if (s.type === "array")
    return (
      /^(0|[1-9][0-9]*)$/.test(key) &&
      key === String(Number(key)) &&
      Number(key) < Number(s.minItems ?? 0) &&
      !!s.items &&
      guaranteed(s.items, rest)
    );
  return false;
}
export function validateChoiceExpression(
  expr: JSONValue,
  allowedAncestors?: Set<string>,
): void {
  if (expr === null || typeof expr !== "object" || Array.isArray(expr)) {
    fail("INVALID_EXPRESSION");
  }
  const v = expr as ObjectValue;
  const keys = Object.keys(v);
  if (keys.length !== 2 || typeof v.op !== "string" || !Array.isArray(v.args)) {
    fail("INVALID_EXPRESSION");
  }
  const op = v.op;
  const args = v.args as JSONValue[];
  if (op === "not") {
    if (args.length !== 1) fail("INVALID_EXPRESSION");
    validateChoiceExpression(args[0], allowedAncestors);
    return;
  }
  if (op === "and" || op === "or") {
    if (args.length < 1) fail("INVALID_EXPRESSION");
    for (const a of args) validateChoiceExpression(a, allowedAncestors);
    return;
  }
  if (op === "exists") {
    if (args.length !== 1) fail("INVALID_EXPRESSION");
    const ref = object(args[0]);
    reference(ref);
    if (
      ref.$ref === "step.output" &&
      allowedAncestors &&
      !allowedAncestors.has(String(ref.stepId))
    ) {
      fail("INVALID_EXPRESSION");
    }
    return;
  }
  if (["eq", "neq", "gt", "gte", "lt", "lte", "in"].includes(op)) {
    if (args.length !== 2) fail("INVALID_EXPRESSION");
    const checkArg = (a: JSONValue) => {
      if (a !== null && typeof a === "object" && !Array.isArray(a)) {
        const objVal = a as ObjectValue;
        if (Object.hasOwn(objVal, "$ref")) {
          reference(objVal);
          if (
            objVal.$ref === "step.output" &&
            allowedAncestors &&
            !allowedAncestors.has(String(objVal.stepId))
          ) {
            fail("INVALID_EXPRESSION");
          }
          return;
        }
        if (Object.hasOwn(objVal, "literal")) {
          if (Object.keys(objVal).length !== 1) fail("INVALID_EXPRESSION");
          return;
        }
      }
    };
    checkArg(args[0]);
    checkArg(args[1]);
    return;
  }
  fail("INVALID_EXPRESSION");
}

function workflow(manifest: JSONValue, definitions: JSONValue[]): void {
  const m = object(manifest);
  if (m.manifestVersion !== 1) fail("UNSUPPORTED_MANIFEST_VERSION");
  if (!Array.isArray(m.nodes) || m.nodes.length === 0) fail("EMPTY_NODES");
  if (m.nodes.length > 50) fail("NODE_COUNT_EXCEEDED");
  if (!validateAgainst(m, schemas["workflow.schema.json"]))
    fail("INVALID_MANIFEST");
  validateSchema(m.inputSchema, schemas["payload-schema.schema.json"]);
  validateSchema(m.outputSchema, schemas["payload-schema.schema.json"]);
  const tasks = new Map<string, ObjectValue>();
  for (const t of definitions) {
    validateTask(t);
    const task = object(t);
    if (tasks.has(String(task.name))) fail("INVALID_TASK");
    tasks.set(String(task.name), task);
  }
  const nodes = m.nodes as ObjectValue[],
    byId = new Map<string, ObjectValue>();
  for (const n of nodes) {
    const id = String(n.id);
    if (byId.has(id)) fail("DUPLICATE_NODE_ID");
    byId.set(id, n);
    const ntype = String(n.type);
    if (ntype !== "task" && ntype !== "choice" && ntype !== "merge") {
      fail("UNSUPPORTED_CAPABILITY");
    }
    if (ntype === "task") {
      if (!tasks.has(String(n.task))) fail("MISSING_TASK_REF");
    }
  }
  for (const n of nodes)
    for (const d of (n.after ?? []) as string[]) {
      if (!byId.has(d)) fail("MISSING_DEPENDENCY");
    }
  const ancestors = new Map<string, Set<string>>(),
    visiting = new Set<string>();
  const visit = (id: string): Set<string> => {
    if (visiting.has(id)) fail("CYCLE_DETECTED");
    if (ancestors.has(id)) return ancestors.get(id)!;
    visiting.add(id);
    const set = new Set<string>();
    for (const d of (byId.get(id)!.after ?? []) as string[]) {
      set.add(d);
      for (const x of visit(d)) set.add(x);
    }
    visiting.delete(id);
    ancestors.set(id, set);
    return set;
  };
  nodes.forEach((n) => visit(String(n.id)));

  const descendants = new Map<string, Set<string>>();
  for (const id of byId.keys()) descendants.set(id, new Set());
  for (const [id, ancSet] of ancestors) {
    for (const anc of ancSet) {
      descendants.get(anc)!.add(id);
    }
  }

  const choices = new Map<string, ObjectValue>();
  const merges = new Map<string, ObjectValue>();
  const mergeForChoice = new Map<string, string>();
  for (const n of nodes) {
    const id = String(n.id);
    if (n.type === "choice") choices.set(id, n);
    else if (n.type === "merge") merges.set(id, n);
  }

  for (const [id, n] of choices) {
    const c = object(n.choice ?? {});
    const branches = (c.branches ?? []) as ObjectValue[];
    const def = c.default !== undefined ? String(c.default) : "";
    if (branches.length === 0 || (branches.length < 2 && !def)) {
      fail("INVALID_CHOICE");
    }
    const branchNames = new Set<string>();
    let conditionlessCount = 0;
    for (const b of branches) {
      const name = String(b.name ?? "");
      if (!name || branchNames.has(name)) fail("INVALID_CHOICE");
      branchNames.add(name);
      if (b.condition !== undefined) {
        validateChoiceExpression(b.condition as JSONValue, ancestors.get(id)!);
      } else {
        conditionlessCount++;
        if (name !== def) fail("INVALID_CHOICE");
      }
    }
    if (def && !branchNames.has(def)) fail("INVALID_CHOICE");
    if (conditionlessCount > 1) fail("INVALID_CHOICE");
  }

  for (const [id, n] of merges) {
    const mrg = object(n.merge ?? {});
    const choiceId = String(mrg.choice ?? "");
    const choiceNode = choices.get(choiceId);
    if (!choiceNode || !ancestors.get(id)!.has(choiceId)) {
      fail("INVALID_MERGE");
    }
    if (mergeForChoice.has(choiceId)) fail("INVALID_MERGE");
    mergeForChoice.set(choiceId, id);

    const mBranches = (mrg.branches ?? []) as ObjectValue[];
    if (mBranches.length === 0) fail("INVALID_MERGE");
    const c = object(choiceNode.choice ?? {});
    const cBranches = (c.branches ?? []) as ObjectValue[];
    const cNames = new Set<string>();
    for (const b of cBranches) cNames.add(String(b.name));
    if (c.default !== undefined) cNames.add(String(c.default));

    const mNames = new Set<string>();
    for (const b of mBranches) {
      const bName = String(b.branch ?? "");
      const term = String(b.terminal ?? "");
      if (!bName || !term || !cNames.has(bName) || mNames.has(bName)) {
        fail("INVALID_MERGE");
      }
      if (
        !byId.has(term) ||
        !ancestors.get(id)!.has(term) ||
        !descendants.get(choiceId)!.has(term)
      ) {
        fail("INVALID_MERGE");
      }
      mNames.add(bName);
    }
    for (const cn of cNames) {
      if (!mNames.has(cn)) fail("INVALID_MERGE");
    }
    if (mrg.outputSchema === undefined) fail("INVALID_MERGE");
    validateSchema(
      mrg.outputSchema as JSONValue,
      schemas["payload-schema.schema.json"],
    );
  }

  for (const id of choices.keys()) {
    if (!mergeForChoice.has(id)) fail("INVALID_CHOICE");
  }

  const choiceBranchNodeSets = new Map<string, Map<string, Set<string>>>();
  for (const choiceId of choices.keys()) {
    const mergeId = mergeForChoice.get(choiceId)!;
    const mrg = object(merges.get(mergeId)!.merge ?? {});
    const mBranches = (mrg.branches ?? []) as ObjectValue[];
    const bMap = new Map<string, Set<string>>();
    choiceBranchNodeSets.set(choiceId, bMap);

    for (const b of mBranches) {
      const bName = String(b.branch);
      const term = String(b.terminal);
      const bSet = new Set<string>();
      for (const nodeId of byId.keys()) {
        if (nodeId === choiceId || nodeId === mergeId) continue;
        if (
          descendants.get(choiceId)!.has(nodeId) &&
          (nodeId === term || ancestors.get(term)!.has(nodeId))
        ) {
          bSet.add(nodeId);
        }
      }
      if (bSet.size === 0) fail("INVALID_BRANCH");
      bMap.set(bName, bSet);
    }

    // Check for cross-branch dependency before overlap check
    for (let i = 0; i < mBranches.length; i++) {
      const tI = String(mBranches[i].terminal);
      const nodesI = new Set<string>([tI]);
      for (const anc of ancestors.get(tI)!) {
        if (anc !== choiceId && !ancestors.get(choiceId)!.has(anc)) {
          nodesI.add(anc);
        }
      }
      for (let j = 0; j < mBranches.length; j++) {
        if (i === j) continue;
        const tJ = String(mBranches[j].terminal);
        const nodesJ = new Set<string>([tJ]);
        for (const anc of ancestors.get(tJ)!) {
          if (anc !== choiceId && !ancestors.get(choiceId)!.has(anc)) {
            nodesJ.add(anc);
          }
        }
        for (const v of nodesJ) {
          for (const dep of (byId.get(v)!.after ?? []) as string[]) {
            if (nodesI.has(dep)) {
              fail("CROSS_BRANCH_DEPENDENCY");
            }
          }
        }
      }
    }

    const branchList = Array.from(bMap.keys());
    for (let i = 0; i < branchList.length; i++) {
      for (let j = i + 1; j < branchList.length; j++) {
        const set1 = bMap.get(branchList[i])!;
        const set2 = bMap.get(branchList[j])!;
        for (const u of set1) {
          if (set2.has(u)) fail("IRREDUCIBLE_GRAPH");
        }
      }
    }

    for (const [bName1, bSet1] of bMap) {
      for (const u of bSet1) {
        for (const dep of (byId.get(u)!.after ?? []) as string[]) {
          for (const [bName2, bSet2] of bMap) {
            if (bName1 !== bName2 && bSet2.has(dep)) {
              fail("CROSS_BRANCH_DEPENDENCY");
            }
          }
        }
      }
    }

    for (const bSet of bMap.values()) {
      for (const u of bSet) {
        for (const dep of (byId.get(u)!.after ?? []) as string[]) {
          if (
            dep === choiceId ||
            bSet.has(dep) ||
            ancestors.get(choiceId)!.has(dep)
          ) {
            continue;
          }
          fail("CROSS_BRANCH_DEPENDENCY");
        }
        for (const w of byId.keys()) {
          for (const d of (byId.get(w)!.after ?? []) as string[]) {
            if (d === u) {
              if (w !== mergeId && !bSet.has(w)) {
                fail("IRREDUCIBLE_GRAPH");
              }
            }
          }
        }
      }
    }
  }

  for (const choiceId of choices.keys()) {
    const mergeId = mergeForChoice.get(choiceId)!;
    const mrg = object(merges.get(mergeId)!.merge ?? {});
    const terminals = new Set<string>();
    for (const b of (mrg.branches ?? []) as ObjectValue[]) {
      terminals.add(String(b.terminal));
    }
    for (const dep of (byId.get(mergeId)!.after ?? []) as string[]) {
      let inBranch = false;
      for (const bSet of choiceBranchNodeSets.get(choiceId)!.values()) {
        if (bSet.has(dep)) {
          inBranch = true;
          break;
        }
      }
      if (inBranch && !terminals.has(dep)) fail("INVALID_MERGE");
    }
  }

  for (const choiceId of choices.keys()) {
    let depth = 1;
    for (const [otherChoiceId, bMap] of choiceBranchNodeSets) {
      if (otherChoiceId === choiceId) continue;
      for (const bSet of bMap.values()) {
        if (bSet.has(choiceId)) depth++;
      }
    }
    if (depth > 8) fail("CHOICE_NESTING_EXCEEDED");
  }

  const nodeAllowed = new Map<string, Set<string>>();
  for (const id of byId.keys()) {
    const allowed = new Set(ancestors.get(id)!);
    for (const [choiceId, bMap] of choiceBranchNodeSets) {
      const mergeId = mergeForChoice.get(choiceId)!;
      for (const bSet of bMap.values()) {
        if (id === mergeId) continue;
        if (!bSet.has(id)) {
          for (const bNode of bSet) allowed.delete(bNode);
        }
      }
    }
    nodeAllowed.set(id, allowed);
  }

  const used = new Set<string>();
  const mapping = (
    v: JSONValue,
    allowed: Set<string>,
    isOutput = false,
  ): void => {
    if (v === null || typeof v !== "object") return;
    if (Array.isArray(v)) {
      v.forEach((x) => mapping(x, allowed, isOutput));
      return;
    }
    if (Object.hasOwn(v, "literal")) {
      if (Object.keys(v).length !== 1) fail("INPUT_MAPPING_ERROR");
      return;
    }
    if (Object.hasOwn(v, "$ref")) {
      reference(v);
      let source = m.inputSchema;
      if (v.$ref === "step.output") {
        const id = String(v.stepId);
        if (!allowed.has(id)) fail("INPUT_MAPPING_ERROR");
        const node = byId.get(id)!;
        if (node.type === "merge") {
          const mrg = object(node.merge ?? {});
          source = (mrg.outputSchema as JSONValue) ?? {
            type: "object",
            properties: {
              branch: { type: "string" },
              value: {},
            },
            required: ["branch", "value"],
          };
        } else if (node.type === "choice") {
          source = {
            type: "object",
            properties: {
              selected: { type: "string" },
              branch: { type: "string" },
            },
            required: ["selected", "branch"],
          };
        } else {
          source = tasks.get(String(node.task))!.outputSchema;
        }
        if (isOutput) used.add(id);
      }
      if (
        !guaranteed(source, pointerParts(v.pointer)) &&
        !Object.hasOwn(v, "default")
      )
        fail("INPUT_MAPPING_ERROR");
      return;
    }
    Object.values(v).forEach((x) => mapping(x, allowed, isOutput));
  };
  const mergeChoiceById = new Map<string, string>();
  for (const [choiceId, mergeId] of mergeForChoice) {
    mergeChoiceById.set(mergeId, choiceId);
  }
  nodes.forEach((n) => {
    const id = String(n.id);
    mapping(n.input ?? {}, nodeAllowed.get(id)!);
    if (n.type === "merge") {
      const mrg = object(n.merge ?? {});
      const choiceId = mergeChoiceById.get(id)!;
      for (const b of (mrg.branches ?? []) as ObjectValue[]) {
        if (b.value !== undefined) {
          const bName = String(b.branch);
          const allowed = new Set<string>([
            ...ancestors.get(choiceId)!,
            choiceId,
          ]);
          const bSet = choiceBranchNodeSets.get(choiceId)?.get(bName);
          if (bSet) for (const nodeId of bSet) allowed.add(nodeId);
          mapping(b.value as JSONValue, allowed);
        }
      }
    }
  });

  const allOutputsAllowed = new Set(byId.keys());
  for (const bMap of choiceBranchNodeSets.values()) {
    for (const bSet of bMap.values()) {
      for (const bNode of bSet) allOutputsAllowed.delete(bNode);
    }
  }
  mapping(m.output, allOutputsAllowed, true);

  const referencedAsDependency = new Set(
    nodes.flatMap((n) => (n.after ?? []) as string[]),
  );
  for (const n of nodes)
    if (
      !referencedAsDependency.has(String(n.id)) &&
      !used.has(String(n.id)) &&
      n.sideEffect !== true
    )
      fail("ORPHAN_LEAF");
}
export function validateWorkflowManifest(
  manifest: unknown,
  tasks: unknown = [],
): ValidationResult {
  return check(() => {
    assertJSON(manifest);
    assertJSON(tasks);
    if (!Array.isArray(tasks)) fail("INVALID_TASK");
    workflow(manifest, tasks);
  });
}
export function validateDeployment(value: unknown): ValidationResult {
  return check(() => {
    assertJSON(value);
    if (!validateAgainst(value, schemas["deployment.schema.json"]))
      fail("INVALID_MANIFEST");
    const m = object(value),
      names = new Set<string>();
    for (const w of m.workflows as JSONValue[]) {
      const name = String(object(w).name);
      if (names.has(name)) fail("INVALID_MANIFEST");
      names.add(name);
      workflow(w, m.tasks as JSONValue[]);
    }
  });
}
