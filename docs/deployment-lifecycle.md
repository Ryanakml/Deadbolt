# Deployment lifecycle and worker preflight

`POST /v1/deployments?environment=<environment-id>` registers a locally-built
manifest; it never uploads or executes customer code. The control plane parses
and validates the manifest, calculates its RFC 8785 canonical SHA-256 manifest
hash, and stores the immutable manifest, task definitions, workflow graph, and
bundle digest in one transaction. Repeating the same canonical manifest returns
the existing registration. Reusing a bundle digest with altered immutable
manifest content is rejected.

Registration starts as `REGISTERED`. If a live, unrevoked worker session in the
same environment advertises that bundle digest, it becomes `AVAILABLE`.
`ACTIVE` means a workflow channel points to that registration for _new_ runs;
existing runs keep their creation deployment and bundle digest.

`POST /v1/workflows/{name}/activate?environment=<environment-id>` requires a
revision. Activation locks the environment before the channel, checks that the
workflow is contained in the requested registration, and emits a transactional
outbox event. Production requires two compatible live workers. Development and
staging may explicitly pass `allowSingleWorker: true` with one compatible worker;
the successful response warns that there is no failover. An active pointer is
not silently removed if workers later disappear—operators receive a warning and
must restore a compatible worker before new work can be safely admitted.

The API requires the existing scoped tenant authorization: `deployments:register`
for registration, `deployments:activate:staging` outside production, and
`deployments:activate:production` in production. Machine keys must use their
own environment. Customer secret values are never part of this API or manifest;
only the SDK-declared secret names are stored.

Migrations `00008_deployment_lifecycle.sql`, `00009_deployment_definition_completeness.sql`,
and `00010_deployment_compatibility_warning.sql` are additive:

- `00008_deployment_lifecycle.sql` adds the deployment status column (`REGISTERED`, `AVAILABLE`, `ACTIVE`) and lookup/immutability indexes.
- `00009_deployment_definition_completeness.sql` adds `task_definitions.idempotency_window_ms` and `workflow_definitions.output_mapping` to persist complete normalized definitions.
- `00010_deployment_compatibility_warning.sql` adds `deployments.compatibility_warning_at` for observable compatibility loss when active deployments lose their last compatible worker.

Binary rollback must not run down migrations; older binaries ignore the additive columns and indexes.
