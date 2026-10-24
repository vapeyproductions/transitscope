"use client";

import { useState } from "react";
import { MonitoringPanel } from "../page";

export default function OpsPage() {
  const [refreshKey, setRefreshKey] = useState(0);

  return <main className="observatory-shell ops-page-shell">
    <header className="topbar">
      <div className="brand-lockup"><div className="brand-mark" aria-hidden="true">✦</div><div><p className="eyebrow">TransitScope // portfolio monitoring</p><h1>ML Ops dashboard</h1></div></div>
      <div className="topbar-actions"><a className="button button-quiet" href="/">← Observatory</a><button className="button button-quiet" type="button" onClick={() => setRefreshKey((current) => current + 1)}>↻ Refresh</button></div>
    </header>
    <section className="ops-page-heading"><p className="eyebrow accent">Portfolio monitoring view · direct links are open</p><p className="ops-page-note">Read-only telemetry for model performance, data health, worker state, and adaptive sampling.</p></section>
    <MonitoringPanel refreshKey={refreshKey} />
    <footer className="footer-note"><span className="footer-pulse" aria-hidden="true" /><span>ML Ops telemetry · read-only</span></footer>
  </main>;
}
