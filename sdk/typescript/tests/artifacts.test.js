import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  artifactRef,
  isArtifactRef,
  uploadArtifact,
  isArtifactUploadMarker,
  ARTIFACT_REF_KEY,
  ARTIFACT_UPLOAD_MARKER_KEY,
} from "../dist/index.js";
import { DeadboltClient } from "../dist/index.js";

describe("artifacts", () => {
  test("artifactRef builds the typed reference", () => {
    assert.deepEqual(artifactRef("abc-123"), { [ARTIFACT_REF_KEY]: "abc-123" });
    assert.throws(() => artifactRef(""), TypeError);
  });

  test("isArtifactRef accepts only the exact shape", () => {
    assert.equal(isArtifactRef({ [ARTIFACT_REF_KEY]: "abc" }), true);
    assert.equal(isArtifactRef({ [ARTIFACT_REF_KEY]: "abc", extra: 1 }), false);
    assert.equal(isArtifactRef({ [ARTIFACT_REF_KEY]: 42 }), false);
    assert.equal(isArtifactRef({ other: "abc" }), false);
    assert.equal(isArtifactRef(null), false);
    assert.equal(isArtifactRef("abc"), false);
  });

  test("uploadArtifact encodes bytes as a marker", () => {
    const marker = uploadArtifact(new Uint8Array([1, 2, 3]), "text/plain");
    assert.equal(isArtifactUploadMarker(marker), true);
    const inner = marker[ARTIFACT_UPLOAD_MARKER_KEY];
    assert.equal(inner.contentType, "text/plain");
    assert.equal(Buffer.from(inner.data, "base64").toString("hex"), "010203");
  });

  test("uploadArtifact defaults content type and rejects empty input", () => {
    const marker = uploadArtifact(new Uint8Array([9]));
    assert.equal(
      marker[ARTIFACT_UPLOAD_MARKER_KEY].contentType,
      "application/octet-stream",
    );
    assert.equal(isArtifactUploadMarker(marker), true);
    assert.throws(() => uploadArtifact(new Uint8Array([])), TypeError);
    assert.equal(isArtifactUploadMarker({}), false);
    assert.equal(
      isArtifactUploadMarker({ [ARTIFACT_UPLOAD_MARKER_KEY]: {} }),
      false,
    );
  });

  test("client artifacts methods hit the scoped endpoints with idempotency keys", async () => {
    const seen = [];
    const mockFetch = async (url, init) => {
      seen.push([url, init]);
      if (url.endsWith("/v1/artifacts")) {
        return {
          ok: true,
          status: 200,
          text: async () =>
            JSON.stringify({
              id: "art-1",
              uploadUrl: "https://store.example/put",
              expiresAt: "2026-09-20T00:05:00Z",
            }),
        };
      }
      if (url.endsWith("/finalize")) {
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify({ id: "art-1", status: "READY" }),
        };
      }
      return {
        ok: true,
        status: 200,
        text: async () =>
          JSON.stringify({
            id: "art-1",
            downloadUrl: "https://store.example/get",
            expiresAt: "2026-09-20T00:05:00Z",
          }),
      };
    };
    const client = new DeadboltClient({
      baseUrl: "https://api.deadbolt.internal",
      apiKey: "apikey-tenant-test-123",
      fetch: mockFetch,
    });
    const created = await client.artifacts.create({
      runId: "run-1",
      attemptId: "att-1",
      ownershipEpoch: 1,
      sizeBytes: 10,
      sha256: "0".repeat(64),
      idempotencyKey: "art-key-1",
    });
    assert.equal(created.id, "art-1");
    assert.equal(seen[0][1].headers["Idempotency-Key"], "art-key-1");
    assert.ok(seen[0][0].endsWith("/v1/artifacts"));
    const finalized = await client.artifacts.finalize({
      id: "art-1",
      attemptId: "att-1",
      ownershipEpoch: 1,
      sha256: "0".repeat(64),
      idempotencyKey: "art-key-2",
    });
    assert.equal(finalized.status, "READY");
    assert.ok(seen[1][0].endsWith("/v1/artifacts/art-1/finalize"));
    const dl = await client.artifacts.downloadUrl("art-1");
    assert.ok(dl.downloadUrl.startsWith("https://store.example/"));
  });
});
