export interface WorkflowNode {
  id: string;
  type: string;
  task: string;
  after?: string[];
  input?: Record<string, unknown>;
}

export interface WorkflowManifest {
  manifestVersion: number;
  name: string;
  inputSchema: Record<string, unknown>;
  outputSchema: Record<string, unknown>;
  nodes: WorkflowNode[];
  output: Record<string, unknown>;
}

export interface ValidationError {
  code: string;
  message: string;
  path?: string;
}

export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}

const MAX_MVP_NODES = 50;
const MAX_NESTING_DEPTH = 32;
const MAX_SCHEMA_SIZE_BYTES = 64 * 1024; // 64 KiB

/**
 * Validates a workflow manifest against DAG structural rules, limits, and capabilities.
 */
export function validateWorkflowManifest(manifest: unknown): ValidationResult {
  const errors: ValidationError[] = [];

  if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) {
    return {
      valid: false,
      errors: [{ code: 'INVALID_MANIFEST', message: 'Workflow manifest must be a non-null object' }],
    };
  }

  const m = manifest as Partial<WorkflowManifest>;

  if (m.manifestVersion !== 1) {
    errors.push({ code: 'UNSUPPORTED_MANIFEST_VERSION', message: 'manifestVersion must be 1' });
  }

  if (!m.name || typeof m.name !== 'string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(m.name)) {
    errors.push({ code: 'INVALID_NAME', message: 'name must be a string matching ^[a-zA-Z0-9_-]{1,64}$' });
  }

  if (!Array.isArray(m.nodes) || m.nodes.length === 0) {
    errors.push({ code: 'EMPTY_NODES', message: 'Workflow must declare at least one node' });
    return { valid: false, errors };
  }

  if (m.nodes.length > MAX_MVP_NODES) {
    errors.push({
      code: 'NODE_COUNT_EXCEEDED',
      message: `Workflow exceeds MVP limit of ${MAX_MVP_NODES} nodes (found ${m.nodes.length})`,
    });
  }

  // Check Schema Sizes and Nesting
  checkSchemaDepthAndSize('inputSchema', m.inputSchema, errors);
  checkSchemaDepthAndSize('outputSchema', m.outputSchema, errors);

  // Validate Node IDs and MVP capabilities
  const nodeMap = new Map<string, WorkflowNode>();
  for (let i = 0; i < m.nodes.length; i++) {
    const node = m.nodes[i];
    if (!node.id || typeof node.id !== 'string') {
      errors.push({ code: 'INVALID_NODE_ID', message: `Node at index ${i} has invalid id` });
      continue;
    }

    if (nodeMap.has(node.id)) {
      errors.push({ code: 'DUPLICATE_NODE_ID', message: `Duplicate node ID "${node.id}" detected` });
    }
    nodeMap.set(node.id, node);

    // MVP capability check: only "task" allowed
    if (node.type !== 'task') {
      errors.push({
        code: 'UNSUPPORTED_CAPABILITY',
        message: `Node type "${node.type}" is unsupported in MVP. Only "task" nodes are permitted.`,
        path: `/nodes/${node.id}/type`,
      });
    }

    if (!node.task || typeof node.task !== 'string') {
      errors.push({ code: 'MISSING_TASK_REF', message: `Node "${node.id}" must specify task name reference` });
    }
  }

  // Validate Dependencies and Graph Acyclicity (DAG check)
  for (const node of m.nodes) {
    if (node.after) {
      for (const depId of node.after) {
        if (!nodeMap.has(depId)) {
          errors.push({
            code: 'MISSING_DEPENDENCY',
            message: `Node "${node.id}" depends on unknown predecessor "${depId}"`,
          });
        }
        if (depId === node.id) {
          errors.push({
            code: 'SELF_DEPENDENCY',
            message: `Node "${node.id}" cannot depend on itself`,
          });
        }
      }
    }
  }

  // Cycle Detection via DFS
  const cycleDetected = detectCycle(nodeMap);
  if (cycleDetected) {
    errors.push({
      code: 'CYCLE_DETECTED',
      message: `Workflow DAG contains a cycle: ${cycleDetected.join(' -> ')}`,
    });
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

function checkSchemaDepthAndSize(name: string, schema: unknown, errors: ValidationError[]): void {
  if (!schema || typeof schema !== 'object') {
    errors.push({ code: 'INVALID_SCHEMA', message: `${name} must be a JSON Schema object` });
    return;
  }

  const jsonStr = JSON.stringify(schema);
  if (Buffer.byteLength(jsonStr, 'utf8') > MAX_SCHEMA_SIZE_BYTES) {
    errors.push({
      code: 'SCHEMA_SIZE_EXCEEDED',
      message: `${name} exceeds maximum allowed size of 64 KiB`,
    });
  }

  const depth = getObjectDepth(schema);
  if (depth > MAX_NESTING_DEPTH) {
    errors.push({
      code: 'SCHEMA_NESTING_EXCEEDED',
      message: `${name} nesting depth (${depth}) exceeds maximum limit of ${MAX_NESTING_DEPTH}`,
    });
  }
}

function getObjectDepth(obj: unknown, current = 1): number {
  if (current > MAX_NESTING_DEPTH + 1) return current;
  if (!obj || typeof obj !== 'object') return current;

  let maxDepth = current;
  for (const val of Object.values(obj as Record<string, unknown>)) {
    if (typeof val === 'object' && val !== null) {
      const d = getObjectDepth(val, current + 1);
      if (d > maxDepth) maxDepth = d;
    }
  }
  return maxDepth;
}

function detectCycle(nodes: Map<string, WorkflowNode>): string[] | null {
  const visited = new Set<string>();
  const recursionStack = new Set<string>();
  const path: string[] = [];

  for (const nodeId of nodes.keys()) {
    const cycle = dfs(nodeId, nodes, visited, recursionStack, path);
    if (cycle) return cycle;
  }

  return null;
}

function dfs(
  nodeId: string,
  nodes: Map<string, WorkflowNode>,
  visited: Set<string>,
  recursionStack: Set<string>,
  path: string[]
): string[] | null {
  if (recursionStack.has(nodeId)) {
    return [...path, nodeId];
  }
  if (visited.has(nodeId)) {
    return null;
  }

  visited.add(nodeId);
  recursionStack.add(nodeId);
  path.push(nodeId);

  const node = nodes.get(nodeId);
  if (node && node.after) {
    for (const depId of node.after) {
      const cycle = dfs(depId, nodes, visited, recursionStack, path);
      if (cycle) return cycle;
    }
  }

  path.pop();
  recursionStack.delete(nodeId);
  return null;
}
