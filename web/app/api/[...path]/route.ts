import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";
import { isOpsSessionToken, OPS_SESSION_COOKIE } from "../../../lib/ops-auth";

export const dynamic = "force-dynamic";

type RouteContext = {
  params: Promise<{ path: string[] }>;
};

async function proxy(request: NextRequest, { params }: RouteContext) {
  const backend = process.env.API_BACKEND_URL?.replace(/\/$/, "");
  if (!backend) {
    return NextResponse.json({ detail: "API_BACKEND_URL is not configured." }, { status: 503 });
  }

  const { path } = await params;
  const isOpsEndpoint = path[0] === "monitoring" && (path[1] === "overview" || path[1] === "control");
  if (isOpsEndpoint) {
    const session = (await cookies()).get(OPS_SESSION_COOKIE)?.value;
    if (!isOpsSessionToken(session)) {
      return NextResponse.json({ detail: "ML Ops access requires an authenticated session." }, { status: 401 });
    }
  }
  const target = `${backend}/api/${path.join("/")}${request.nextUrl.search}`;
  const headers = new Headers();
  const accept = request.headers.get("accept");
  const contentType = request.headers.get("content-type");
  if (accept) headers.set("accept", accept);
  if (contentType) headers.set("content-type", contentType);
  if (isOpsEndpoint) {
    headers.set("x-transitscope-ops-token", process.env.OPS_CONTROL_TOKEN ?? process.env.OPS_PASSWORD ?? "monitor");
  }

  const method = request.method.toUpperCase();
  const body = method === "GET" || method === "HEAD" ? undefined : await request.arrayBuffer();
  const response = await fetch(target, {
    method,
    headers,
    body,
    cache: "no-store",
  });

  return new NextResponse(response.body, {
    status: response.status,
    headers: {
      "content-type": response.headers.get("content-type") ?? "application/json",
      "cache-control": "no-store",
    },
  });
}

export const GET = proxy;
export const POST = proxy;
