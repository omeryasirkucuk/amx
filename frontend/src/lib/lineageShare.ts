/**
 * Lineage share-link encode/decode.
 *
 * Compresses the in-memory ``LineagePayload`` with pako (deflate) and
 * base64url-encodes the result so it slots into a URL fragment
 * without escaping headaches. The decoder is the inverse; both sides
 * stay frontend-only so a shared link works without any AMX install
 * on the receiver's side (when paired with a public Studio).
 *
 * Designed as a hash fragment (#) rather than a query parameter so
 * the payload is never sent to the server — sensitive table names
 * stay on the client.
 */

import pako from "pako";

import type { LineagePayload } from "./api";

const VERSION = "v1";

export function encodeLineageShare(payload: LineagePayload): string {
  const json = JSON.stringify(payload);
  const compressed = pako.deflate(json);
  const b64 = bytesToBase64(compressed);
  return `${VERSION}:${b64}`;
}

export function decodeLineageShare(blob: string): LineagePayload | null {
  if (!blob) return null;
  const [version, b64] = blob.split(":", 2);
  if (version !== VERSION || !b64) return null;
  try {
    const compressed = base64ToBytes(b64);
    const json = pako.inflate(compressed, { to: "string" });
    const parsed = JSON.parse(json) as LineagePayload;
    if (!Array.isArray(parsed.nodes) || !Array.isArray(parsed.edges)) return null;
    return parsed;
  } catch {
    return null;
  }
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64ToBytes(b64: string): Uint8Array {
  const normalised = b64.replace(/-/g, "+").replace(/_/g, "/");
  const padding = normalised.length % 4 === 0 ? "" : "=".repeat(4 - (normalised.length % 4));
  const binary = atob(normalised + padding);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}
