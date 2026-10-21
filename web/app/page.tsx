"use client";

import { useState } from "react";

const tabs = [
  { id: "overall", label: "Overall", available: false },
  { id: "stage1", label: "Stage 1", available: true },
  { id: "stage2", label: "Stage 2", available: false },
  { id: "stage3", label: "Stage 3", available: false },
];

export default function HomePage() {
  const [activeTab, setActiveTab] = useState("stage1");

  return (
    <main className="observatory-shell">
      <div className="ambient-glow ambient-glow-one" aria-hidden="true" />
      <div className="ambient-glow ambient-glow-two" aria-hidden="true" />

      <header className="topbar">
        <div className="brand-lockup">
          <div className="brand-mark" aria-hidden="true">✦</div>
          <div>
            <p className="eyebrow">TransitScope // live assessment lab</p>
            <h1>Potential planet observatory</h1>
          </div>
        </div>
        <div className="topbar-actions">
          <span className="data-state"><span className="status-dot" /> Awaiting data</span>
          <button className="button button-quiet" type="button" disabled>
            ↻ <span>Refresh</span>
          </button>
        </div>
      </header>

      <section className="intro-row" aria-labelledby="observatory-title">
        <div>
          <p className="eyebrow accent">A ranked search through stellar brightness</p>
          <h2 id="observatory-title">Watch evidence accumulate.</h2>
          <p className="intro-copy">
            TransitScope assesses transit-like dimming in public observations. A high rank is
            a reason to investigate—not a scientific confirmation.
          </p>
        </div>
        <div className="mode-toggle" aria-label="Dashboard mode">
          <button className="mode-button active" type="button">Live feed</button>
          <button className="mode-button" type="button" disabled>Replay <span>soon</span></button>
        </div>
      </section>

      <nav className="tab-row" aria-label="Assessment rankings">
        {tabs.map((tab) => (
          <button
            className={`tab-button ${activeTab === tab.id ? "active" : ""}`}
            disabled={!tab.available}
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            type="button"
          >
            {tab.label}
            {!tab.available && <span className="tab-note">soon</span>}
          </button>
        ))}
      </nav>

      <section className="stage-heading">
        <div>
          <div className="stage-kicker"><span className="stage-number">01</span> Broad screening</div>
          <h3>{activeTab === "stage1" ? "Stage 1 ranking" : "Ranking view"}</h3>
          <p>Searching for periodic, transit-like brightness dips across the target universe.</p>
        </div>
        <button className="button button-outline" type="button" disabled>
          Replay <span className="button-note">available after first run</span>
        </button>
      </section>

      <section className="metrics-grid" aria-label="Processing summary">
        <article className="metric-card">
          <span className="metric-label">Target universe</span>
          <strong>126,000</strong>
          <span className="metric-detail">stars queued for staged assessment</span>
        </article>
        <article className="metric-card">
          <span className="metric-label">Processed</span>
          <strong>0</strong>
          <span className="metric-detail">awaiting the first MAST probe</span>
        </article>
        <article className="metric-card">
          <span className="metric-label">Active candidates</span>
          <strong>0</strong>
          <span className="metric-detail">no observations published yet</span>
        </article>
        <article className="metric-card metric-card-status">
          <span className="metric-label">Pipeline status</span>
          <strong><span className="status-dot muted" /> Standby</strong>
          <span className="metric-detail">last update: not started</span>
        </article>
      </section>

      <section className="ranking-panel" aria-labelledby="ranking-title">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Top candidates · live ranking</p>
            <h3 id="ranking-title">No candidates published yet</h3>
          </div>
          <span className="panel-meta">0 / 100 displayed</span>
        </div>

        <div className="table-wrap">
          <div className="ranking-table ranking-table-header" aria-hidden="true">
            <span>Rank</span>
            <span>Candidate</span>
            <span>Status</span>
            <span>Current activity</span>
            <span>Movement</span>
            <span>Last update</span>
          </div>
          <div className="empty-state" role="status">
            <div className="empty-orbit" aria-hidden="true"><span /></div>
            <h4>The observatory is waiting for its first observation.</h4>
            <p>
              The ranking will populate when the MAST/TESS ingestion probe completes. Empty is
              a real state here—not a hidden placeholder result.
            </p>
            <div className="empty-state-meta"><span className="status-dot" /> No current activity</div>
          </div>
        </div>
      </section>

      <footer className="footer-note">
        <span className="footer-pulse" aria-hidden="true" />
        <span>Data source: MAST / TESS · Processing state: awaiting ingestion · Evidence is provisional</span>
      </footer>
    </main>
  );
}

