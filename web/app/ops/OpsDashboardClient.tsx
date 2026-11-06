"use client";

import { useState } from "react";
import PortfolioBrief from "./PortfolioBrief";
import SystemHealthPanel from "./SystemHealthPanel";

export default function OpsDashboardClient() {
  const [refreshKey, setRefreshKey] = useState(0);

  return <main className="observatory-shell ops-page-shell">
    <header className="topbar">
      <div className="brand-lockup"><div className="brand-mark" aria-hidden="true">✦</div><div><p className="eyebrow">TransitScope // operator monitoring</p><h1>ML Ops dashboard</h1></div></div>
      <div className="topbar-actions"><a className="button button-quiet" href="/">← Observatory</a><a className="button button-quiet" href="#portfolio-architecture">Architecture</a><button className="button button-quiet" type="button" onClick={() => setRefreshKey((current) => current + 1)}>↻ Refresh</button></div>
    </header>
    <PortfolioBrief />
    <SystemHealthPanel refreshKey={refreshKey} />
    <footer className="footer-note"><span className="footer-pulse" aria-hidden="true" /><span>ML Ops telemetry · operator access</span></footer>
  </main>;
}
