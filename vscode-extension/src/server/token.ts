// Bearer-token generation for extension-owned Studio servers.
// Mirrors Python's secrets.token_urlsafe(32): 32 random bytes,
// base64url, no padding.
import { randomBytes } from "node:crypto";

export function generateToken(): string {
  return randomBytes(32).toString("base64url");
}
