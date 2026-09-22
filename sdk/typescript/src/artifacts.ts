/**
 * Typed artifact references (Blueprint §18.2).
 *
 * Results larger than 256 KiB never enter inline JSONB. Inside a task
 * handler, `uploadArtifact(bytes)` builds an upload marker that the worker
 * agent publishes through the scoped artifact APIs; the committed step
 * output is then the typed reference `{ $artifact: "<id>" }`, which flows
 * through outputs opaquely and downloads through the client below.
 */

export const ARTIFACT_REF_KEY = "$artifact";
export const ARTIFACT_UPLOAD_MARKER_KEY = "$artifactUpload";

export interface ArtifactReference {
  $artifact: string;
}

export interface ArtifactUploadMarker {
  $artifactUpload: {
    data: string;
    contentType: string;
  };
}

export function artifactRef(id: string): ArtifactReference {
  if (typeof id !== "string" || id.length === 0) {
    throw new TypeError("artifact id is required");
  }
  return { [ARTIFACT_REF_KEY]: id } as ArtifactReference;
}

export function isArtifactRef(value: unknown): value is ArtifactReference {
  if (typeof value !== "object" || value === null) return false;
  const keys = Object.keys(value);
  return (
    keys.length === 1 &&
    keys[0] === ARTIFACT_REF_KEY &&
    typeof (value as Record<string, unknown>)[ARTIFACT_REF_KEY] === "string"
  );
}

function toBase64(data: Uint8Array): string {
  if (typeof Buffer !== "undefined") {
    return Buffer.from(data).toString("base64");
  }
  let binary = "";
  for (let i = 0; i < data.length; i++) {
    binary += String.fromCharCode(data[i]);
  }
  if (typeof btoa !== "undefined") {
    return btoa(binary);
  }
  throw new Error("no base64 encoder available");
}

export function uploadArtifact(
  data: Uint8Array,
  contentType = "application/octet-stream",
): ArtifactUploadMarker {
  if (!(data instanceof Uint8Array) || data.length === 0) {
    throw new TypeError("non-empty bytes are required");
  }
  return {
    [ARTIFACT_UPLOAD_MARKER_KEY]: {
      data: toBase64(data),
      contentType,
    },
  } as ArtifactUploadMarker;
}

export function isArtifactUploadMarker(
  value: unknown,
): value is ArtifactUploadMarker {
  if (typeof value !== "object" || value === null) return false;
  const keys = Object.keys(value);
  if (keys.length !== 1 || keys[0] !== ARTIFACT_UPLOAD_MARKER_KEY) return false;
  const inner = (value as Record<string, unknown>)[
    ARTIFACT_UPLOAD_MARKER_KEY
  ] as Record<string, unknown>;
  return (
    typeof inner === "object" &&
    inner !== null &&
    typeof inner.data === "string" &&
    inner.data.length > 0
  );
}
