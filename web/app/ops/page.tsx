import { cookies } from "next/headers";
import OpsDashboardClient from "./OpsDashboardClient";
import OpsLogin from "./OpsLogin";
import { isOpsSessionToken, OPS_SESSION_COOKIE } from "../../lib/ops-auth";

export const dynamic = "force-dynamic";

export default async function OpsPage() {
  const session = (await cookies()).get(OPS_SESSION_COOKIE)?.value;
  if (!isOpsSessionToken(session)) return <OpsLogin />;
  return <OpsDashboardClient />;
}
