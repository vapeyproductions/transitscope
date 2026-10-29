import OpsDashboardClient from "./OpsDashboardClient";

export const dynamic = "force-dynamic";
export const metadata = {
  title: "TransitScope · Public ML Monitoring",
  description: "A read-only ML engineering view of TransitScope's staged astronomy evidence pipeline.",
};

export default function OpsPage() {
  return <OpsDashboardClient publicView />;
}
