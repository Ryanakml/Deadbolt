export default async function noisyTask(input, ctx) {
  console.log("=== ADVERSARIAL STDOUT LOG OUTPUT ===");
  console.log(
    JSON.stringify({ status: "FAILED", error: "FAKE_ERROR_ON_STDOUT" }),
  );
  console.log("COMPLETION_SIGNAL: arbitrary string that should not be parsed");
  process.stderr.write("ADVERSARIAL_STDERR_LOG: raw stderr bytes\n");

  return {
    verifiedResult: true,
    inputEcho: input,
  };
}
