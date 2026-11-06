"use client";

import { useState } from "react";
import { MonitoringPanel, MonitoringSystemHealthPanel } from "../page";
import PortfolioBrief from "./PortfolioBrief";

export default function OpsDashboardClient() {
  const [refreshKey, setRefreshKey] = useState(0);
  const [activeTab, setActiveTab] = useState<"overview" | "health">("overview");

  return <main className="observatory-shell ops-page-shell">
    <header className="topbar">
      <div className="brand-lockup"><div className="brand-mark" aria-hidden="true">✦</div><div><p className="eyebrow">TransitScope // operator monitoring</p><h1>ML Ops dashboard</h1></div></div>
      <div className="topbar-actions"><a className="button button-quiet" href="/">← Observatory</a><a className="button button-quiet" href="#portfolio-architecture">Architecture</a><button className="button button-quiet" type="button" onClick={() => setRefreshKey((current) => current + 1)}>↻ Refresh</button></div>
    </header>
    <PortfolioBrief />
    <nav className="ops-monitor-tabs" aria-label="Monitoring views" role="tablist">
      <button className={`ops-monitor-tab ${activeTab === "overview" ? "active" : ""}`} type="button" role="tab" aria-selected={activeTab === "overview"} onClick={() => setActiveTab("overview")}>Pipeline dashboard</button>
      <button className={`ops-monitor-tab ${activeTab === "health" ? "active" : ""}`} type="button" role="tab" aria-selected={activeTab === "health"} onClick={() => setActiveTab("health")}>System health · source of truth</button>
    </nav>
    {activeTab === "health" ? <MonitoringSystemHealthPanel refreshKey={refreshKey} /> : <MonitoringPanel refreshKey={refreshKey} />}
    <footer className="footer-note"><span className="footer-pulse" aria-hidden="true" /><span>ML Ops telemetry · operator access</span></footer>
  </main>;
}
