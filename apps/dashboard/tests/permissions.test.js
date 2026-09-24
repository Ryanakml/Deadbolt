import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  roleCanControlRuns,
  sessionCanControlRuns,
  visibleRunControls,
} from "../dist/permissions.js";

function sessionWithRole(role, status = "ACTIVE", orgId = "org-1") {
  return {
    user: { id: "u-1", email: "dev@example.com", name: "Dev" },
    active_organization_id: orgId,
    memberships: [
      {
        OrganizationID: orgId,
        OrganizationName: "Acme",
        Role: role,
        Status: status,
      },
    ],
  };
}

describe("runs:control permission gating (Blueprint §23.2)", () => {
  test("canonical roles mirror backend rbac: viewer denied, others admitted", () => {
    assert.equal(roleCanControlRuns("viewer"), false);
    assert.equal(roleCanControlRuns("Viewer"), false);
    assert.equal(roleCanControlRuns("developer"), true);
    assert.equal(roleCanControlRuns("operator"), true);
    assert.equal(roleCanControlRuns("admin"), true);
    assert.equal(roleCanControlRuns("owner"), true);
  });

  test("unknown, empty, and missing roles fail closed", () => {
    assert.equal(roleCanControlRuns("superuser"), false);
    assert.equal(roleCanControlRuns(""), false);
    assert.equal(roleCanControlRuns(null), false);
    assert.equal(roleCanControlRuns(undefined), false);
  });

  test("developer session in the active org may control runs", () => {
    assert.equal(
      sessionCanControlRuns(sessionWithRole("developer"), "org-1"),
      true,
    );
    assert.equal(
      sessionCanControlRuns(sessionWithRole("operator"), "org-1"),
      true,
    );
    assert.equal(
      sessionCanControlRuns(sessionWithRole("admin"), "org-1"),
      true,
    );
  });

  test("viewer session never controls runs (read-only identity)", () => {
    assert.equal(
      sessionCanControlRuns(sessionWithRole("viewer"), "org-1"),
      false,
    );
  });

  test("inactive membership or org mismatch fails closed", () => {
    assert.equal(
      sessionCanControlRuns(sessionWithRole("developer", "SUSPENDED"), "org-1"),
      false,
    );
    assert.equal(
      sessionCanControlRuns(sessionWithRole("developer"), "org-2"),
      false,
    );
    assert.equal(sessionCanControlRuns(null, "org-1"), false);
    assert.equal(
      sessionCanControlRuns(sessionWithRole("developer"), null),
      false,
    );
    assert.equal(sessionCanControlRuns({ memberships: [] }, "org-1"), false);
  });

  test("authorized identity sees state-appropriate CTAs", () => {
    assert.deepEqual(visibleRunControls("RUNNING", true), {
      pause: true,
      resume: false,
      cancel: true,
    });
    assert.deepEqual(visibleRunControls("QUEUED", true), {
      pause: true,
      resume: false,
      cancel: true,
    });
    assert.deepEqual(visibleRunControls("WAITING", true), {
      pause: true,
      resume: false,
      cancel: true,
    });
    assert.deepEqual(visibleRunControls("PAUSING", true), {
      pause: false,
      resume: true,
      cancel: true,
    });
    assert.deepEqual(visibleRunControls("PAUSED", true), {
      pause: false,
      resume: true,
      cancel: true,
    });
  });

  test("terminal and cancelling runs offer no mutation CTAs", () => {
    for (const status of ["SUCCEEDED", "FAILED", "CANCELLED", "CANCELLING"]) {
      assert.deepEqual(visibleRunControls(status, true), {
        pause: false,
        resume: false,
        cancel: false,
      });
    }
  });

  test("read-only identity sees no mutation CTAs in any state", () => {
    for (const status of [
      "QUEUED",
      "RUNNING",
      "WAITING",
      "PAUSING",
      "PAUSED",
      "CANCELLING",
      "SUCCEEDED",
      "FAILED",
      "CANCELLED",
    ]) {
      assert.deepEqual(visibleRunControls(status, false), {
        pause: false,
        resume: false,
        cancel: false,
      });
    }
  });
});
