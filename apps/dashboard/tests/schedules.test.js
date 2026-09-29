import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { DashboardApiClient } from "../dist/api.js";
import {
  roleCanManageSchedules,
  sessionCanManageSchedules,
  sessionCanControlRuns,
  SCHEDULES_WRITE_CAPABILITY,
  RUNS_CONTROL_CAPABILITY,
  APPROVALS_DECIDE_CAPABILITY,
} from "../dist/permissions.js";

function sessionWithRole(role, status = "ACTIVE", orgId = "org-1") {
  return {
    user: { id: "u-1", email: "op@example.com", name: "Op" },
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

function installFetch(handler) {
  const orig = globalThis.fetch;
  const seen = [];
  globalThis.fetch = async (input, init = {}) => {
    seen.push([String(input), init]);
    return handler(String(input), init);
  };
  return {
    seen,
    restore() {
      globalThis.fetch = orig;
    },
  };
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status });
}

function scheduleFixture() {
  return {
    id: "sched-1",
    workflow: "nightly-report",
    environment: "env-1",
    cron: "*/5 * * * *",
    timezone: "UTC",
    deploymentId: null,
    overlapPolicy: "skip-overlap",
    misfirePolicy: "coalesce-one",
    paused: false,
    revision: 3,
    nextDueAt: new Date(Date.now() + 60000).toISOString(),
    lastOccurrenceAt: null,
  };
}

describe("schedules client wire", () => {
  test("listSchedules hits the environment-scoped URL and unwraps items", async () => {
    const sched = scheduleFixture();
    const net = installFetch((url) => {
      assert.ok(url.includes("/v1/schedules?"));
      assert.ok(url.includes("environment=env-1"));
      return jsonResponse({ items: [sched], nextCursor: null });
    });
    try {
      const client = new DashboardApiClient();
      const items = await client.listSchedules("env-1");
      assert.equal(items.length, 1);
      assert.equal(items[0].workflow, "nightly-report");
      assert.equal(items[0].overlapPolicy, "skip-overlap");
      assert.equal(items[0].misfirePolicy, "coalesce-one");
    } finally {
      net.restore();
    }
  });

  test("createSchedule sends CSRF + Idempotency-Key and the create body", async () => {
    const sched = scheduleFixture();
    const net = installFetch((url, init) => {
      assert.ok(url.includes("/v1/schedules?"));
      assert.ok(url.includes("environment=env-1"));
      assert.equal(init.method, "POST");
      assert.ok("X-CSRF-Token" in init.headers);
      assert.equal(init.headers["Idempotency-Key"], "create-key-1");
      assert.deepEqual(JSON.parse(init.body), {
        workflow: "nightly-report",
        cron: "*/5 * * * *",
        timezone: "UTC",
      });
      return jsonResponse(sched);
    });
    try {
      const client = new DashboardApiClient();
      const out = await client.createSchedule(
        "env-1",
        { workflow: "nightly-report", cron: "*/5 * * * *", timezone: "UTC" },
        "create-key-1",
      );
      assert.equal(out.id, "sched-1");
      assert.equal(net.seen.length, 1);
    } finally {
      net.restore();
    }
  });

  test("createSchedule mints an Idempotency-Key when none is passed", async () => {
    const net = installFetch(() => jsonResponse(scheduleFixture()));
    try {
      const client = new DashboardApiClient();
      await client.createSchedule("env-1", {
        workflow: "w",
        cron: "* * * * *",
        timezone: "UTC",
      });
      const key = net.seen[0][1].headers["Idempotency-Key"];
      assert.ok(key && typeof key === "string" && key.length > 0);
    } finally {
      net.restore();
    }
  });

  test("updateSchedule sends expectedRevision + configuration", async () => {
    const sched = { ...scheduleFixture(), revision: 4 };
    const net = installFetch((url, init) => {
      assert.ok(url.includes("/v1/schedules/sched-1?"));
      assert.ok(url.includes("environment=env-1"));
      assert.equal(init.method, "PATCH");
      assert.ok("X-CSRF-Token" in init.headers);
      assert.equal(init.headers["Idempotency-Key"], "edit-key-1");
      assert.deepEqual(JSON.parse(init.body), {
        expectedRevision: 3,
        configuration: {
          workflow: "nightly-report",
          cron: "0 * * * *",
          timezone: "UTC",
        },
      });
      return jsonResponse(sched);
    });
    try {
      const client = new DashboardApiClient();
      const out = await client.updateSchedule(
        "sched-1",
        "env-1",
        3,
        { workflow: "nightly-report", cron: "0 * * * *", timezone: "UTC" },
        "edit-key-1",
      );
      assert.equal(out.revision, 4);
    } finally {
      net.restore();
    }
  });

  test("deleteSchedule sends an empty body with mutation headers", async () => {
    const net = installFetch((url, init) => {
      assert.ok(url.includes("/v1/schedules/sched-1?"));
      assert.ok(url.includes("environment=env-1"));
      assert.equal(init.method, "DELETE");
      assert.ok("X-CSRF-Token" in init.headers);
      assert.ok(init.headers["Idempotency-Key"]);
      assert.deepEqual(JSON.parse(init.body), {});
      return jsonResponse({ deleted: true, id: "sched-1" });
    });
    try {
      const client = new DashboardApiClient();
      const out = await client.deleteSchedule("sched-1", "env-1", "del-key-1");
      assert.deepEqual(out, { deleted: true, id: "sched-1" });
    } finally {
      net.restore();
    }
  });

  test("pauseSchedule and resumeSchedule send expectedRevision", async () => {
    const paused = { ...scheduleFixture(), paused: true, revision: 4 };
    const resumed = { ...scheduleFixture(), paused: false, revision: 5 };
    const net = installFetch((url, init) => {
      assert.ok("X-CSRF-Token" in init.headers);
      assert.ok(init.headers["Idempotency-Key"]);
      if (url.includes("/pause?")) {
        assert.equal(init.method, "POST");
        assert.deepEqual(JSON.parse(init.body), { expectedRevision: 3 });
        return jsonResponse(paused);
      }
      assert.ok(url.includes("/resume?"));
      assert.equal(init.method, "POST");
      assert.deepEqual(JSON.parse(init.body), { expectedRevision: 4 });
      return jsonResponse(resumed);
    });
    try {
      const client = new DashboardApiClient();
      const p = await client.pauseSchedule("sched-1", "env-1", 3, "pause-k");
      assert.equal(p.paused, true);
      const r = await client.resumeSchedule("sched-1", "env-1", 4, "resume-k");
      assert.equal(r.paused, false);
      assert.equal(net.seen.length, 2);
    } finally {
      net.restore();
    }
  });

  test("listScheduleOccurrences sends limit/cursor and returns items + cursor", async () => {
    const occ = {
      id: "occ-1",
      scheduleId: "sched-1",
      dueAt: new Date().toISOString(),
      revision: 3,
      status: "SKIPPED",
      skippedReason: "SKIPPED_QUOTA",
      skippedCount: 2,
      runId: null,
    };
    const net = installFetch((url) => {
      assert.ok(url.includes("/v1/schedules/sched-1/occurrences?"));
      assert.ok(url.includes("environment=env-1"));
      assert.ok(url.includes("limit=25"));
      assert.ok(url.includes("cursor=abc"));
      return jsonResponse({ items: [occ], nextCursor: "next-1" });
    });
    try {
      const client = new DashboardApiClient();
      const resp = await client.listScheduleOccurrences(
        "sched-1",
        "env-1",
        25,
        "abc",
      );
      assert.equal(resp.items.length, 1);
      assert.equal(resp.items[0].skippedReason, "SKIPPED_QUOTA");
      assert.equal(resp.nextCursor, "next-1");
    } finally {
      net.restore();
    }
  });

  test("failures carry Failed to ... (HTTP ...) without response payloads", async () => {
    const net = installFetch(() => new Response("nope", { status: 409 }));
    try {
      const client = new DashboardApiClient();
      await assert.rejects(
        client.pauseSchedule("sched-1", "env-1", 3, "k"),
        /Failed to pause schedule \(HTTP 409\)/,
      );
    } finally {
      net.restore();
    }
  });
});

describe("schedules:write permission gating", () => {
  test("capability is distinct from runs:control and approvals:decide", () => {
    assert.equal(SCHEDULES_WRITE_CAPABILITY, "schedules:write");
    assert.notEqual(SCHEDULES_WRITE_CAPABILITY, RUNS_CONTROL_CAPABILITY);
    assert.notEqual(SCHEDULES_WRITE_CAPABILITY, APPROVALS_DECIDE_CAPABILITY);
  });

  test("only operator, admin, and owner may manage schedules", () => {
    for (const role of ["operator", "admin", "owner", "Owner", "ADMIN"]) {
      assert.equal(roleCanManageSchedules(role), true, role);
    }
    for (const role of ["viewer", "developer", "", null, undefined]) {
      assert.equal(roleCanManageSchedules(role), false, String(role));
    }
    assert.equal(roleCanManageSchedules("superuser"), false);
  });

  test("a developer controls runs but never manages schedules", () => {
    const session = sessionWithRole("developer");
    assert.equal(sessionCanControlRuns(session, "org-1"), true);
    assert.equal(sessionCanManageSchedules(session, "org-1"), false);
  });

  test("operator, admin, and owner sessions manage schedules", () => {
    for (const role of ["operator", "admin", "owner"]) {
      assert.equal(
        sessionCanManageSchedules(sessionWithRole(role), "org-1"),
        true,
        role,
      );
    }
  });

  test("viewer, developer, unknown, inactive, and wrong-org fail closed", () => {
    assert.equal(
      sessionCanManageSchedules(sessionWithRole("viewer"), "org-1"),
      false,
    );
    assert.equal(
      sessionCanManageSchedules(sessionWithRole("developer"), "org-1"),
      false,
    );
    assert.equal(
      sessionCanManageSchedules(sessionWithRole("superuser"), "org-1"),
      false,
    );
    assert.equal(
      sessionCanManageSchedules(
        sessionWithRole("operator", "SUSPENDED"),
        "org-1",
      ),
      false,
    );
    assert.equal(
      sessionCanManageSchedules(sessionWithRole("operator"), "org-2"),
      false,
    );
    assert.equal(sessionCanManageSchedules(null, "org-1"), false);
    assert.equal(
      sessionCanManageSchedules(sessionWithRole("operator"), null),
      false,
    );
    assert.equal(
      sessionCanManageSchedules({ memberships: [] }, "org-1"),
      false,
    );
  });
});
