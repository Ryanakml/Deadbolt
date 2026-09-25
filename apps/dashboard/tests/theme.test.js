import { test } from "node:test";
import assert from "node:assert/strict";

import { applyDashboardTheme, resolveInitialTheme } from "../dist/theme.js";

function themeFixture() {
  const classes = new Set();
  const attributes = new Map();
  return {
    classes,
    attributes,
    root: {
      classList: {
        toggle(name, force) {
          if (force) classes.add(name);
          else classes.delete(name);
          return force;
        },
      },
    },
    toggle: {
      setAttribute(name, value) {
        attributes.set(name, value);
      },
    },
  };
}

test("stored light theme overrides a dark operating-system preference", () => {
  assert.equal(resolveInitialTheme("light", true), "light");
  assert.equal(resolveInitialTheme("dark", false), "dark");
  assert.equal(resolveInitialTheme(null, true), "dark");
});

test("applying a theme keeps dark/light classes and aria state exclusive", () => {
  const fixture = themeFixture();

  applyDashboardTheme(fixture.root, fixture.toggle, "dark");
  assert.deepEqual([...fixture.classes], ["dark"]);
  assert.equal(fixture.attributes.get("aria-pressed"), "true");

  applyDashboardTheme(fixture.root, fixture.toggle, "light");
  assert.deepEqual([...fixture.classes], ["light"]);
  assert.equal(fixture.attributes.get("aria-pressed"), "false");
});
