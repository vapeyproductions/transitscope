import { cookies } from "next/headers";
import { isOpsSessionToken, OPS_SESSION_COOKIE } from "../../lib/ops-auth";
import OpsDashboardClient from "./OpsDashboardClient";
import OpsLogin from "./OpsLogin";

export const dynamic = "force-dynamic";
export const metadata = {
  title: "TransitScope · ML Ops Monitoring",
  description: "Authenticated ML engineering monitoring for TransitScope's staged astronomy evidence pipeline.",
};

export default async function OpsPage() {
  const session = (await cookies()).get(OPS_SESSION_COOKIE)?.value;
  return isOpsSessionToken(session) ? <OpsDashboardClient /> : <OpsLogin />;
}
