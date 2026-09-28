import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  roleCanControlRuns,
  sessionCanControlRuns,
  visibleRunControls,
  roleCanDecideApprovals,
  sessionCanDecideApprovals,
  APPROVALS_DECIDE_CAPABILITY,
  RUNS_CONTROL_CAPABILITY,
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

// Approval decisions are a separate capability from run control. Blueprint
// §24.2 grants approvals:decide to operator, admin, and owner while explicitly
// withholding it from developer, so a developer who can pause a run must still
// never be offered an approval decision.
describe("approval decision permissions", () => {
  test("the two capabilities are distinct and neither implies the other", () => {
    assert.notEqual(APPROVALS_DECIDE_CAPABILITY, RUNS_CONTROL_CAPABILITY);
    assert.equal(APPROVALS_DECIDE_CAPABILITY, "approvals:decide");
  });

  test("only operator, admin, and owner may decide approvals", () => {
    for (const role of ["operator", "admin", "owner", "Owner", "ADMIN"]) {
      assert.equal(roleCanDecideApprovals(role), true, role);
    }
    for (const role of ["viewer", "developer", "", null, undefined]) {
      assert.equal(roleCanDecideApprovals(role), false, String(role));
    }
  });

  test("a developer can control runs but never decide approvals", () => {
    const session = sessionWithRole("developer");
    assert.equal(sessionCanControlRuns(session, "org-1"), true);
    assert.equal(sessionCanDecideApprovals(session, "org-1"), false);
  });

  test("an operator decides approvals and controls runs", () => {
    const session = sessionWithRole("operator");
    assert.equal(sessionCanControlRuns(session, "org-1"), true);
    assert.equal(sessionCanDecideApprovals(session, "org-1"), true);
  });

  test("an inactive membership fails closed for approvals", () => {
    const session = sessionWithRole("owner", "SUSPENDED");
    assert.equal(sessionCanDecideApprovals(session, "org-1"), false);
  });

  test("a membership in another organization grants nothing", () => {
    const session = sessionWithRole("owner", "ACTIVE", "org-other");
    assert.equal(sessionCanDecideApprovals(session, "org-1"), false);
  });

  test("a missing session or organization grants nothing", () => {
    assert.equal(sessionCanDecideApprovals(null, "org-1"), false);
    assert.equal(
      sessionCanDecideApprovals(sessionWithRole("owner"), null),
      false,
    );
  });
});
