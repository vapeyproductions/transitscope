import { createHmac, timingSafeEqual } from "node:crypto";

export const OPS_SESSION_COOKIE = "transitscope_ops_session";

function configuredPassword(): string {
  return process.env.OPS_PASSWORD ?? "monitor";
}
function sessionSecret(): string {
  return process.env.OPS_SESSION_SECRET ?? configuredPassword();
}

function digest(value: string): Buffer {
  return createHmac("sha256", sessionSecret()).update(value).digest();
}

function securelyMatches(actual: string, expected: string): boolean {
  return timingSafeEqual(digest(actual), digest(expected));
}

export function isOpsPassword(password: string): boolean {
  return securelyMatches(password, configuredPassword());
}

export function opsSessionToken(): string {
  return digest("transitscope-ops-session-v1").toString("hex");
}

export function isOpsSessionToken(token: string | undefined): boolean {
  if (!token) return false;
  return securelyMatches(token, opsSessionToken());
}
