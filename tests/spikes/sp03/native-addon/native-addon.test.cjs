const assert = require("node:assert/strict");
const addon = require("./native-addon.node");

assert.equal(addon.targetArch(), "native-addon-loaded");
assert.match(process.platform, /^linux$/);
assert.match(process.arch, /^(x64|arm64)$/);
