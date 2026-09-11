import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateWorkflowManifest } from '../dist/validator.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const validLinearPath = path.resolve(__dirname, '../../../contracts/fixtures/dag-validation/valid-linear-workflow.json');
const invalidCyclePath = path.resolve(__dirname, '../../../contracts/fixtures/dag-validation/invalid-cycle-workflow.json');

test('Valid linear workflow passes all validations', () => {
  const manifest = JSON.parse(fs.readFileSync(validLinearPath, 'utf8'));
  const res = validateWorkflowManifest(manifest);
  assert.equal(res.valid, true);
  assert.equal(res.errors.length, 0);
});

test('Cyclic workflow is rejected with CYCLE_DETECTED', () => {
  const manifest = JSON.parse(fs.readFileSync(invalidCyclePath, 'utf8'));
  const res = validateWorkflowManifest(manifest);
  assert.equal(res.valid, false);
  assert.ok(res.errors.some(e => e.code === 'CYCLE_DETECTED'), 'Expected CYCLE_DETECTED error');
});

test('Non-task node in MVP is rejected with UNSUPPORTED_CAPABILITY', () => {
  const manifest = {
    manifestVersion: 1,
    name: 'approval-workflow',
    inputSchema: {},
    outputSchema: {},
    nodes: [
      { id: 'node1', type: 'approval', task: 'some-task' }
    ],
    output: {}
  };
  const res = validateWorkflowManifest(manifest);
  assert.equal(res.valid, false);
  assert.ok(res.errors.some(e => e.code === 'UNSUPPORTED_CAPABILITY'), 'Expected UNSUPPORTED_CAPABILITY error');
});

test('Excessive node count (>50) is rejected with NODE_COUNT_EXCEEDED', () => {
  const nodes = Array.from({ length: 51 }, (_, i) => ({
    id: `step_${i}`,
    type: 'task',
    task: 'noop-task',
  }));
  const manifest = {
    manifestVersion: 1,
    name: 'large-workflow',
    inputSchema: {},
    outputSchema: {},
    nodes,
    output: {}
  };
  const res = validateWorkflowManifest(manifest);
  assert.equal(res.valid, false);
  assert.ok(res.errors.some(e => e.code === 'NODE_COUNT_EXCEEDED'), 'Expected NODE_COUNT_EXCEEDED error');
});

test('Deep schema nesting (>32) is rejected with SCHEMA_NESTING_EXCEEDED', () => {
  let nested = { leaf: 'string' };
  for (let i = 0; i < 35; i++) {
    nested = { nested };
  }
  const manifest = {
    manifestVersion: 1,
    name: 'deep-schema-workflow',
    inputSchema: nested,
    outputSchema: {},
    nodes: [
      { id: 'step1', type: 'task', task: 'noop' }
    ],
    output: {}
  };
  const res = validateWorkflowManifest(manifest);
  assert.equal(res.valid, false);
  assert.ok(res.errors.some(e => e.code === 'SCHEMA_NESTING_EXCEEDED'), 'Expected SCHEMA_NESTING_EXCEEDED error');
});
