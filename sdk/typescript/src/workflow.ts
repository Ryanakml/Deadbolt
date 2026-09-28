import type { JSONValue } from "./json.js";
import { ContractError, fail } from "./json.js";
import { type ObjectValue } from "./schema.js";
import type { TaskDefinition } from "./task.js";
import { validateWorkflowManifest } from "./validator.js";

export interface InputRef {
  $ref: "run.input";
  pointer: string;
  default?: JSONValue;
}

export interface OutputRef {
  $ref: "step.output";
  stepId: string;
  pointer: string;
  default?: JSONValue;
}

export interface LiteralRef {
  literal: JSONValue;
}

export function input(pointer: string, defaultValue?: JSONValue): ObjectValue {
  if (
    typeof pointer !== "string" ||
    (pointer !== "" && !pointer.startsWith("/"))
  ) {
    fail("INPUT_MAPPING_ERROR");
  }
  const ref: ObjectValue = {
    $ref: "run.input",
    pointer,
  };
  if (defaultValue !== undefined) {
    ref.default = defaultValue;
  }
  return ref;
}

export function output(
  stepId: string,
  pointer: string,
  defaultValue?: JSONValue,
): ObjectValue {
  if (typeof stepId !== "string" || !stepId) {
    fail("INPUT_MAPPING_ERROR");
  }
  if (
    typeof pointer !== "string" ||
    (pointer !== "" && !pointer.startsWith("/"))
  ) {
    fail("INPUT_MAPPING_ERROR");
  }
  const ref: ObjectValue = {
    $ref: "step.output",
    stepId,
    pointer,
  };
  if (defaultValue !== undefined) {
    ref.default = defaultValue;
  }
  return ref;
}

export function literal(value: JSONValue): ObjectValue {
  return { literal: value };
}

export type WorkflowNodeType =
  | "task"
  | "choice"
  | "merge"
  | "approval"
  | "delay";

export interface ChoiceBranch {
  name: string;
  condition?: JSONValue;
}

export interface ChoiceNodeConfig {
  branches: ChoiceBranch[];
  default?: string;
}

export interface MergeBranch {
  branch: string;
  terminal: string;
  value?: JSONValue;
}

export interface MergeNodeConfig {
  choice: string;
  branches: MergeBranch[];
  outputSchema: JSONValue;
}

/**
 * ApprovalNodeConfig declares one durable human decision.
 *
 * An approval is a control node, not a task: it is never dispatched to a
 * worker, never holds a lease, and never blocks a process while it waits. The
 * node stores the request, then a person decides it later.
 */
export interface ApprovalNodeConfig {
  /** Human-readable request shown to the approver. */
  payload?: JSONValue;
  /** Schema the committed decision output is validated against. */
  outputSchema?: JSONValue;
  /**
   * Optional. `approvals:decide` is the only permission that may gate an
   * approval decision, and omitting this is equivalent to declaring it.
   */
  requiredPermission?: "approvals:decide";
  /**
   * Optional wait in milliseconds. Defaults to 24h, which is also the maximum
   * meaningful value: the wait is always clamped to the remaining run lifetime,
   * and an expired approval fails the run with `APPROVAL_EXPIRED`.
   */
  expiresInMs?: number;
}

export interface WorkflowNode {
  id: string;
  type: WorkflowNodeType;
  task?: TaskDefinition<any, any> | string;
  after?: string[];
  input?: Record<string, JSONValue>;
  sideEffect?: boolean;
  choice?: ChoiceNodeConfig | Record<string, JSONValue>;
  merge?: MergeNodeConfig | Record<string, JSONValue>;
  approval?: Record<string, JSONValue>;
  delayMs?: number;
}

export function choiceNode(
  id: string,
  config: ChoiceNodeConfig,
  after?: string[],
): WorkflowNode {
  return {
    id,
    type: "choice",
    choice: config as unknown as Record<string, JSONValue>,
    ...(after && after.length > 0 ? { after } : {}),
  };
}

export function mergeNode(
  id: string,
  config: MergeNodeConfig,
  after?: string[],
): WorkflowNode {
  return {
    id,
    type: "merge",
    merge: config as unknown as Record<string, JSONValue>,
    ...(after && after.length > 0 ? { after } : {}),
  };
}

export function expr(op: string, ...args: JSONValue[]): ObjectValue {
  return { op, args };
}

/**
 * approvalNode declares a durable human decision.
 *
 * Both `approved` and `rejected` are successful step outcomes: the node's job
 * was to collect a valid decision, and whether that decision is good for the
 * business is a later choice's problem. Route the business consequence with a
 * `choice` on `/decision` rather than by treating a rejection as a failure.
 */
export function approvalNode(
  id: string,
  config: ApprovalNodeConfig,
  after?: string[],
): WorkflowNode {
  return {
    id,
    type: "approval",
    approval: config as unknown as Record<string, JSONValue>,
    ...(after && after.length > 0 ? { after } : {}),
  };
}

export interface WorkflowConfig<TInput = unknown, TOutput = unknown> {
  name: string;
  inputSchema: JSONValue;
  outputSchema: JSONValue;
  nodes: WorkflowNode[];
  output: Record<string, JSONValue>;
  tasks?: TaskDefinition<any, any>[];
  validateOnInit?: boolean;
}

export interface WorkflowDefinition<TInput = unknown, TOutput = unknown> {
  readonly name: string;
  readonly inputSchema: JSONValue;
  readonly outputSchema: JSONValue;
  readonly nodes: readonly WorkflowNode[];
  readonly output: Readonly<Record<string, JSONValue>>;
  readonly tasks: readonly TaskDefinition<any, any>[];
  toManifest(): ObjectValue;
}

export function defineWorkflow<TInput = unknown, TOutput = unknown>(
  config: WorkflowConfig<TInput, TOutput>,
): WorkflowDefinition<TInput, TOutput> {
  const tasksMap = new Map<string, TaskDefinition<any, any>>();

  if (config.tasks) {
    for (const t of config.tasks) {
      tasksMap.set(t.name, t);
    }
  }

  const manifestNodes: ObjectValue[] = [];

  for (const node of config.nodes) {
    const nodeManifest: Record<string, unknown> = {
      id: node.id,
      type: node.type,
    };

    if (node.task !== undefined) {
      if (typeof node.task === "string") {
        nodeManifest.task = node.task;
      } else {
        nodeManifest.task = node.task.name;
        tasksMap.set(node.task.name, node.task);
      }
    }

    if (node.after !== undefined && node.after.length > 0) {
      nodeManifest.after = [...node.after];
    }
    if (node.input !== undefined) {
      nodeManifest.input = node.input;
    }
    if (node.sideEffect !== undefined) {
      nodeManifest.sideEffect = node.sideEffect;
    }
    if (node.choice !== undefined) {
      nodeManifest.choice = node.choice;
    }
    if (node.merge !== undefined) {
      nodeManifest.merge = node.merge;
    }
    if (node.approval !== undefined) {
      nodeManifest.approval = node.approval;
    }
    if (node.delayMs !== undefined) {
      nodeManifest.delayMs = node.delayMs;
    }

    manifestNodes.push(nodeManifest as ObjectValue);
  }

  const workflowManifest: ObjectValue = {
    manifestVersion: 1,
    name: config.name,
    inputSchema: config.inputSchema,
    outputSchema: config.outputSchema,
    nodes: manifestNodes,
    output: config.output,
  };

  const tasksList = Array.from(tasksMap.values());
  const shouldValidate = config.validateOnInit ?? true;

  if (shouldValidate) {
    const taskManifests = tasksList.map((t) => t.toManifest());
    const validation = validateWorkflowManifest(
      workflowManifest,
      taskManifests,
    );
    if (!validation.valid) {
      const err = validation.errors[0];
      throw new ContractError(err.code, err.message);
    }
  }

  return {
    name: config.name,
    inputSchema: config.inputSchema,
    outputSchema: config.outputSchema,
    nodes: Object.freeze([...config.nodes]),
    output: Object.freeze({ ...config.output }),
    tasks: Object.freeze(tasksList),
    toManifest(): ObjectValue {
      return JSON.parse(JSON.stringify(workflowManifest));
    },
  };
}
