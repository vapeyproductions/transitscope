import OpsDashboardClient from "./OpsDashboardClient";

export const dynamic = "force-dynamic";
export const metadata = {
  title: "TransitScope · ML Ops Monitoring",
  description: "Live ML engineering monitoring for TransitScope's staged astronomy evidence pipeline.",
};

export default function OpsPage() {
  return <OpsDashboardClient />;
}
