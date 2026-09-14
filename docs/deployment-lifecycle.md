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

Migration `00008_deployment_lifecycle.sql` is additive: it adds a status and
indexes only. Binary rollback must not run its down migration; an older binary
can ignore the additive column and indexes.
