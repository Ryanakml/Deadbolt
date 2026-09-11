import {
  digestJSON,
  mapInput,
  evaluateChoice,
  validatePayload,
  validateWorkflowManifest,
  validateDeployment,
  normalizeTask,
  ContractError,
  validateAgainst,
} from "../../sdk/typescript/dist/index.js";
import { schemas } from "../../sdk/typescript/dist/schema-data.js";
export function evaluate(f) {
  try {
    let value;
    switch (f.op) {
      case "contract":
        value = validateAgainst(f.input, schemas[f.schemaPath]);
        break;
      case "digest":
        value = digestJSON(f.raw);
        break;
      case "mapping":
        value = mapInput(f.mapping, f.input, f.outputs);
        break;
      case "choice":
        value = evaluateChoice(f.expression, f.input, f.outputs);
        break;
      case "schema":
        validatePayload(
          f.schema,
          f.input,
          schemas["payload-schema.schema.json"],
        );
        value = true;
        break;
      case "normalize":
        value = normalizeTask(f.task);
        break;
      case "workflow": {
        const r = validateWorkflowManifest(f.manifest, f.tasks);
        if (!r.valid) return { error: r.errors[0].code };
        value = true;
        break;
      }
      case "deployment": {
        const r = validateDeployment(f.manifest);
        if (!r.valid) return { error: r.errors[0].code };
        value = true;
        break;
      }
      default:
        throw Error(`unknown fixture operation ${f.op}`);
    }
    return { value };
  } catch (e) {
    if (e instanceof ContractError) return { error: e.code };
    throw e;
  }
}
