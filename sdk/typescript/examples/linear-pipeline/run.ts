import process from "node:process";
import { DeadboltClient } from "@runtime/sdk";

export async function runPipelineExample() {
  const client = new DeadboltClient({
    baseUrl: process.env.DEADBOLT_API_URL || "http://localhost:8080",
    apiKey: process.env.DEADBOLT_API_KEY || "test-api-key",
    environment: "staging",
  });

  const requestId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;

  console.log(`Triggering workflow run with Idempotency-Key: ${requestId}`);

  // Create run requires an explicit Idempotency-Key header per Blueprint §14.2 & §20.1
  const run = await client.runs.create({
    workflow: "customer-onboarding",
    input: {
      email: "jane.doe@example.com",
      name: "Jane Doe",
    },
    idempotencyKey: requestId,
  });

  // HTTP 202 Accepted represents persisted acceptance in the control plane, not completion
  console.log(`Run accepted by control plane! Run ID: ${run.id}`);
  console.log(`Initial Status: ${run.status} (isAccepted: ${run.isAccepted})`);

  console.log("Polling for final run outcome...");
  try {
    const result = await client.runs.pollResult(run.id, {
      intervalMs: 1000,
      timeoutMs: 30000,
    });
    console.log("Workflow completed successfully!");
    console.log("Committed Output:", JSON.stringify(result, null, 2));
  } catch (err) {
    console.error("Workflow run finished with error or timed out:", err);
  }
}

if (
  process.argv[1]?.endsWith("run.ts") ||
  process.argv[1]?.endsWith("run.js")
) {
  runPipelineExample().catch(console.error);
}
