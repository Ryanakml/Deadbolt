import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const addon = require("./native-addon.node");

export default function nativeTask() {
  return { native: addon.targetArch() };
}
