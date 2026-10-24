import { NextRequest, NextResponse } from "next/server";
import {
  isOpsPassword,
  OPS_SESSION_COOKIE,
  opsSessionToken,
} from "../../../lib/ops-auth";

export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  let body: { password?: string } = {};
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ detail: "Password is required." }, { status: 400 });
  }

  if (typeof body.password !== "string" || !isOpsPassword(body.password)) {
    return NextResponse.json({ detail: "Incorrect password." }, { status: 401 });
  }

  const response = NextResponse.json({ ok: true });
  response.cookies.set({
    name: OPS_SESSION_COOKIE,
    value: opsSessionToken(),
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: 60 * 60 * 8,
  });
  return response;
}
export async function DELETE() {
  const response = NextResponse.json({ ok: true });
  response.cookies.set({
    name: OPS_SESSION_COOKIE,
    value: "",
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: 0,
  });
  return response;
}
