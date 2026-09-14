import { defineTask } from "@runtime/sdk";
export const validateSignup = defineTask({
  name: "validate-signup",
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
      userId: { type: "string" },
      email: { type: "string" },
      normalizedName: { type: "string" },
    },
    required: ["userId", "email", "normalizedName"],
    additionalProperties: false,
  },
  recovery: "safe", // Repeatable read-only / validation operation
  timeoutMs: 10000,
  handler: async (input, ctx) => {
    ctx.logger.info("Validating signup request", { email: input.email });
    return {
      userId: `usr_${ctx.stepId}_${Date.now()}`,
      email: input.email.toLowerCase().trim(),
      normalizedName: input.name.trim(),
    };
  },
});
export const provisionAccount = defineTask({
  name: "provision-account",
  inputSchema: {
    type: "object",
    properties: {
      userId: { type: "string" },
      email: { type: "string" },
    },
    required: ["userId", "email"],
    additionalProperties: false,
  },
  outputSchema: {
    type: "object",
    properties: {
      accountId: { type: "string" },
      status: { type: "string" },
    },
    required: ["accountId", "status"],
    additionalProperties: false,
  },
  recovery: "safe",
  timeoutMs: 20000,
  handler: async (input, ctx) => {
    ctx.logger.info("Provisioning customer account", { userId: input.userId });
    return {
      accountId: `acc_${input.userId}`,
      status: "ACTIVE",
    };
  },
});
export const sendWelcomeEmail = defineTask({
  name: "send-welcome-email",
  inputSchema: {
    type: "object",
    properties: {
      accountId: { type: "string" },
      email: { type: "string" },
    },
    required: ["accountId", "email"],
    additionalProperties: false,
  },
  outputSchema: {
    type: "object",
    properties: {
      deliveryId: { type: "string" },
      sentAt: { type: "string" },
    },
    required: ["deliveryId", "sentAt"],
    additionalProperties: false,
  },
  recovery: "idempotent", // External communication requires explicit deduplication
  timeoutMs: 15000,
  idempotencyWindowMs: 60000, // Valid window >= 5000 + 15000
  handler: async (input, ctx) => {
    // OperationId is stable across retries and used for downstream deduplication
    ctx.logger.info("Sending welcome email with stable operationId", {
      operationId: ctx.operationId,
      email: input.email,
    });
    return {
      deliveryId: `del_${ctx.operationId}`,
      sentAt: new Date().toISOString(),
    };
  },
});
