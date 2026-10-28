import OpsDashboardClient from "./OpsDashboardClient";

export const dynamic = "force-dynamic";

export default function OpsPage() {
  return <OpsDashboardClient publicView />;
}
