"use client";

import { useState } from "react";
import { MonitoringPanel } from "../page";
import PortfolioBrief from "./PortfolioBrief";

export default function OpsDashboardClient({ publicView = false }: { publicView?: boolean }) {
  const [refreshKey, setRefreshKey] = useState(0);

  return <main className="observatory-shell ops-page-shell">
    <header className="topbar">
      <div className="brand-lockup"><div className="brand-mark" aria-hidden="true">✦</div><div><p className="eyebrow">TransitScope // {publicView ? "public ML monitoring" : "portfolio monitoring"}</p><h1>ML Ops dashboard</h1></div></div>
      <div className="topbar-actions"><a className="button button-quiet" href="/">← Observatory</a><a className="button button-quiet" href="#portfolio-overview">Portfolio brief</a><button className="button button-quiet" type="button" onClick={() => setRefreshKey((current) => current + 1)}>↻ Refresh</button></div>
    </header>
    <PortfolioBrief publicView={publicView} />
    <MonitoringPanel refreshKey={refreshKey} readOnly={publicView} />
    <footer className="footer-note"><span className="footer-pulse" aria-hidden="true" /><span>ML Ops telemetry · {publicView ? "public read-only view" : "operator access"}</span></footer>
  </main>;
}
