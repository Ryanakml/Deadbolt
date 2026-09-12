export default async function envCheckTask(input, ctx) {
  const leakedSecrets = [];

  for (const key of Object.keys(process.env)) {
    const upper = key.toUpperCase();
    if (
      upper.includes("TOKEN") ||
      upper.includes("SECRET") ||
      upper.includes("PASSWORD") ||
      upper.includes("CREDENTIAL")
    ) {
      leakedSecrets.push(key);
    }
  }

  return {
    leakedSecrets,
    hasPath: Boolean(process.env.PATH),
    customConfig: process.env.CUSTOM_CONFIG,
  };
}
