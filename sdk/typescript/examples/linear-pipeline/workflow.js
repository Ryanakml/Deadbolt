import { defineWorkflow, input, output } from "@runtime/sdk";
import { validateSignup, provisionAccount, sendWelcomeEmail } from "./tasks.js";
/**
 * Linear Customer Onboarding Workflow: A -> B -> C
 *
 * Node A ("validate"):   validates incoming email/name.
 * Node B ("provision"):  creates account using outputs from A.
 * Node C ("send-email"): sends welcome email using accountId from B and email from A.
 */
export const customerOnboardingWorkflow = defineWorkflow({
  name: "customer-onboarding",
  inputSchema: {
    type: "object",
    properties: {
      email: { type: "string" },
      name: { type: "string" },
    },
    required: ["email", "name"],
    additionalProperties: false,
  },
  outputSchema: {
    type: "object",
    properties: {
      accountId: { type: "string" },
      deliveryId: { type: "string" },
    },
    required: ["accountId", "deliveryId"],
    additionalProperties: false,
  },
  nodes: [
    {
      id: "validate",
      type: "task",
      task: validateSignup,
      input: {
        email: input("/email"),
        name: input("/name"),
      },
    },
    {
      id: "provision",
      type: "task",
      task: provisionAccount,
      after: ["validate"],
      input: {
        userId: output("validate", "/userId"),
        email: output("validate", "/email"),
      },
    },
    {
      id: "send-email",
      type: "task",
      task: sendWelcomeEmail,
      after: ["provision"],
      input: {
        accountId: output("provision", "/accountId"),
        email: output("validate", "/email"),
      },
    },
  ],
  output: {
    accountId: output("provision", "/accountId"),
    deliveryId: output("send-email", "/deliveryId"),
  },
});
