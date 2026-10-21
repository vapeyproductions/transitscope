"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

type RankingView = "overall" | "stage1" | "stage2" | "stage3";
type CandidateStatus = "Promising" | "Needs More Data" | "Inconclusive" | "Known Match";

type DashboardStatus = {
  data_version: number | null;
  processed_target_count: number;
  total_target_count: number;
  active_candidate_count: number;
  last_ingest_at: string | null;
  worker: { state?: string; last_heartbeat?: string | null };
};

type RankingRow = {
  rank: number;
  candidate_id: string;
  target_id: string;
  stage: string;
  status: CandidateStatus;
  evidence_score: number | null;
  activity: string;
  rank_delta: number;
  movement: string;
  last_evaluated_at: string | null;
};

type RankingResponse = {
  view: RankingView;
  data_version: number | null;
  sequence_number: number | null;
  occurred_at: string | null;
  processed_target_count: number;
  total_target_count: number;
  rows: RankingRow[];
};

type CandidateDetail = {
  candidate_id: string;
  target_id: string;
  stage: string;
  status: CandidateStatus;
  evidence_score: number | null;
  overall_score: number | null;
  rank_overall: number | null;
  rank_stage: number | null;
  activity: string;
  last_evaluated_at: string | null;
  next_action: string;
  target: {
    provider_target_id: string;
    ra_deg: number | null;
    dec_deg: number | null;
    sector: number | null;
    cadence: string | null;
    source_product_family: string | null;
  } | null;
  evaluation: {
    model_version: string;
    features: Record<string, unknown>;
    metrics: Record<string, unknown>;
    data_sufficiency: string;
  } | null;
};

type LightCurveResponse = {
  points: Array<{ time: number; normalized_flux: number }>;
};

type HistoryResponse = {
  events: Array<{
    sequence_number: number;
    stage: string;
    event_type: string;
    payload: Record<string, unknown>;
    occurred_at: string | null;
  }>;
};

const tabs: Array<{ id: RankingView; label: string; available: boolean }> = [
  { id: "overall", label: "Overall", available: false },
  { id: "stage1", label: "Stage 1", available: true },
  { id: "stage2", label: "Stage 2", available: false },
  { id: "stage3", label: "Stage 3", available: false },
];

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    cache: "no-store",
    headers: { Accept: "application/json", ...(init?.headers ?? {}) },
  });
  const payload = await response.json();
  if (!response.ok) {
    const message = payload?.detail?.message ?? "The observatory API did not respond successfully.";
    throw new Error(message);
  }
  return payload as T;
}

function formatDate(value: string | null | undefined): string {
  if (!value) return "Not evaluated";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

function statusClass(value: string): string {
  return value.toLowerCase().replaceAll(" ", "-");
}

function movementLabel(row: RankingRow): string {
  if (row.movement === "new") return "↗ New";
  if (row.movement === "sustained_top") return "🔥 Sustained top";
  if (row.movement === "rapid_rise") return "↗ Rapid rise";
  if (row.movement === "rising") return `↑ ${Math.abs(row.rank_delta)} place${Math.abs(row.rank_delta) === 1 ? "" : "s"}`;
  if (row.movement === "falling") return `↓ ${Math.abs(row.rank_delta)} place${Math.abs(row.rank_delta) === 1 ? "" : "s"}`;
  return "→ Unchanged";
}

function LightCurveChart({ points }: { points: LightCurveResponse["points"] }) {
  if (!points.length) return <div className="chart-empty">Normalized series is not available yet.</div>;
  const width = 760;
  const height = 220;
  const padding = 18;
  const minTime = points[0].time;
  const maxTime = points[points.length - 1].time || minTime + 1;
  const values = points.map((point) => point.normalized_flux);
  const minFlux = Math.min(...values);
  const maxFlux = Math.max(...values);
  const fluxRange = maxFlux - minFlux || 0.001;
  const path = points.map((point, index) => {
    const x = padding + ((point.time - minTime) / (maxTime - minTime || 1)) * (width - padding * 2);
    const y = height - padding - ((point.normalized_flux - minFlux) / fluxRange) * (height - padding * 2);
    return `${index === 0 ? "M" : "L"}${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(" ");
  return <div className="chart-wrap"><svg aria-label="Normalized light curve" role="img" viewBox={`0 0 ${width} ${height}`}><line className="chart-baseline" x1={padding} x2={width - padding} y1={height / 2} y2={height / 2} /><path className="chart-line" d={path} /></svg><div className="chart-axis"><span>{minTime.toFixed(2)} BJD−2457000</span><span>{maxTime.toFixed(2)}</span></div></div>;
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="detail-metric"><span>{label}</span><strong>{value}</strong></div>;
}

function CandidateModal({ candidate, curve, history, onClose }: { candidate: CandidateDetail; curve: LightCurveResponse | null; history: HistoryResponse | null; onClose: () => void }) {
  const features = candidate.evaluation?.features ?? {};
  const numberFeature = (key: string, digits = 2) => {
    const value = features[key];
    return typeof value === "number" ? value.toFixed(digits) : "—";
  };
  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose(); }}>
    <section className="candidate-modal" role="dialog" aria-modal="true" aria-labelledby="candidate-modal-title">
      <header className="modal-header"><div><p className="eyebrow accent">Candidate investigation · Stage 1</p><h3 id="candidate-modal-title">{candidate.target_id}</h3><p className="modal-subtitle">{candidate.target?.source_product_family ?? "MAST / TESS observation"} · model {candidate.evaluation?.model_version ?? "—"}</p></div><button className="modal-close" type="button" onClick={onClose} aria-label="Close candidate details">×</button></header>
      <div className="modal-summary"><span className={`status-pill status-${statusClass(candidate.status)}`}>{candidate.status}</span><span className="evidence-chip">Evidence score <strong>{candidate.evidence_score?.toFixed(3) ?? "—"}</strong></span><span className="modal-summary-note">{candidate.activity}</span></div>
      <div className="modal-content-grid">
        <div className="modal-main-column">
          <div className="modal-section"><div className="section-heading"><div><p className="eyebrow">Normalized observation</p><h4>Light curve</h4></div><span className="section-note">{curve?.points.length ?? 0} plotted points</span></div><LightCurveChart points={curve?.points ?? []} /></div>
          <div className="modal-section"><div className="section-heading"><div><p className="eyebrow">Interpretable screen</p><h4>Evidence metrics</h4></div></div><div className="detail-metrics"><Metric label="Best period" value={`${numberFeature("period_days")} days`} /><Metric label="Transit depth" value={`${(Number(features.depth ?? 0) * 100).toFixed(3)}%`} /><Metric label="BLS SNR" value={numberFeature("bls_snr")} /><Metric label="Events observed" value={numberFeature("observed_transit_count", 0)} /><Metric label="Coverage" value={`${numberFeature("coverage_days")} days`} /><Metric label="Usable data" value={`${(Number(features.usable_data_fraction ?? 0) * 100).toFixed(1)}%`} /></div></div>
        </div>
        <aside className="modal-side-column">
          <div className="detail-card"><p className="eyebrow">Current assessment</p><dl className="detail-list"><div><dt>Stage rank</dt><dd>#{candidate.rank_stage ?? "—"}</dd></div><div><dt>Last evaluation</dt><dd>{formatDate(candidate.last_evaluated_at)}</dd></div><div><dt>Data sufficiency</dt><dd>{candidate.evaluation?.data_sufficiency ?? "—"}</dd></div><div><dt>Sector</dt><dd>{candidate.target?.sector ?? "—"}</dd></div></dl><div className="next-action"><span>Next evidence</span><p>{candidate.next_action}</p></div></div>
          <div className="detail-card"><p className="eyebrow">Pipeline history</p><div className="timeline">{history?.events.length ? history.events.map((event) => <div className="timeline-event" key={`${event.sequence_number}-${event.event_type}`}><span className="timeline-dot" /><div><strong>{event.event_type.replaceAll("_", " ")}</strong><span>{formatDate(event.occurred_at)}</span></div></div>) : <p className="muted-copy">No pipeline events recorded.</p>}</div></div>
        </aside>
      </div>
      <footer className="modal-footer"><span>Coordinates: {candidate.target?.ra_deg?.toFixed(4) ?? "—"}°, {candidate.target?.dec_deg?.toFixed(4) ?? "—"}°</span><span>Evidence is provisional · not scientific confirmation</span></footer>
    </section>
  </div>;
}

export default function HomePage() {
  const [activeTab, setActiveTab] = useState<RankingView>("stage1");
  const [status, setStatus] = useState<DashboardStatus | null>(null);
  const [ranking, setRanking] = useState<RankingResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [candidate, setCandidate] = useState<CandidateDetail | null>(null);
  const [curve, setCurve] = useState<LightCurveResponse | null>(null);
  const [history, setHistory] = useState<HistoryResponse | null>(null);
  const [modalLoading, setModalLoading] = useState(false);

  const loadDashboard = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextStatus, nextRanking] = await Promise.all([fetchJson<DashboardStatus>("/api/status"), fetchJson<RankingResponse>(`/api/rankings?view=${activeTab}&limit=100`)]);
      setStatus(nextStatus);
      setRanking(nextRanking);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "The observatory could not load published data.");
    } finally {
      setLoading(false);
    }
  }, [activeTab]);

  useEffect(() => { void loadDashboard(); }, [loadDashboard]);

  const closeCandidate = useCallback(() => {
    setSelectedId(null);
    setCandidate(null);
    setCurve(null);
    setHistory(null);
  }, []);

  const openCandidate = useCallback(async (candidateId: string) => {
    setSelectedId(candidateId);
    setModalLoading(true);
    setError(null);
    try {
      const [detail, nextCurve, nextHistory] = await Promise.all([fetchJson<{ candidate: CandidateDetail }>(`/api/candidates/${candidateId}`), fetchJson<LightCurveResponse>(`/api/candidates/${candidateId}/light-curve`), fetchJson<HistoryResponse>(`/api/candidates/${candidateId}/history`)]);
      setCandidate(detail.candidate);
      setCurve(nextCurve);
      setHistory(nextHistory);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Candidate details are unavailable.");
      closeCandidate();
    } finally {
      setModalLoading(false);
    }
  }, [closeCandidate]);

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    closeCandidate();
    try {
      await fetchJson("/api/refresh", { method: "POST" });
      await loadDashboard();
    } finally {
      setRefreshing(false);
    }
  }, [closeCandidate, loadDashboard]);

  const rows = ranking?.rows ?? [];
  const dataState = status?.data_version ? `Published v${status.data_version}` : "Awaiting data";
  const pipelineState = status?.data_version ? "Published" : status?.worker?.state === "idle" ? "Standby" : "Starting";
  const panelTitle = loading && !ranking ? "Loading published state" : rows.length ? `${rows.length} candidate${rows.length === 1 ? "" : "s"} published` : "No candidates published yet";
  const processedCount = status?.processed_target_count ?? ranking?.processed_target_count ?? 0;
  const targetCount = status?.total_target_count ?? ranking?.total_target_count ?? 126000;
  const activeCandidateCount = status?.active_candidate_count ?? rows.length;
  const footerState = status?.last_ingest_at ? `Last published: ${formatDate(status.last_ingest_at)}` : "Awaiting first publication";

  const modal = useMemo(() => {
    if (!selectedId) return null;
    if (modalLoading || !candidate) return <div className="modal-backdrop" role="presentation"><section className="candidate-modal modal-loading" role="dialog" aria-modal="true" aria-label="Loading candidate details"><span className="status-dot" /><p>Loading candidate evidence…</p></section></div>;
    return <CandidateModal candidate={candidate} curve={curve} history={history} onClose={closeCandidate} />;
  }, [candidate, closeCandidate, curve, history, modalLoading, selectedId]);

  return <main className="observatory-shell">
    <div className="ambient-glow ambient-glow-one" aria-hidden="true" /><div className="ambient-glow ambient-glow-two" aria-hidden="true" />
    <header className="topbar"><div className="brand-lockup"><div className="brand-mark" aria-hidden="true">✦</div><div><p className="eyebrow">TransitScope // live assessment lab</p><h1>Potential planet observatory</h1></div></div><div className="topbar-actions"><span className="data-state"><span className={`status-dot ${status?.data_version ? "" : "muted"}`} /> {dataState}</span><button className="button button-quiet" type="button" onClick={() => void handleRefresh()} disabled={refreshing || loading}>↻ <span>{refreshing ? "Checking" : "Refresh"}</span></button></div></header>
    <section className="intro-row" aria-labelledby="observatory-title"><div><p className="eyebrow accent">A ranked search through stellar brightness</p><h2 id="observatory-title">Watch evidence accumulate.</h2><p className="intro-copy">TransitScope assesses transit-like dimming in public observations. A high rank is a reason to investigate—not a scientific confirmation.</p></div><div className="mode-toggle" aria-label="Dashboard mode"><button className="mode-button active" type="button">Live feed</button><button className="mode-button" type="button" disabled>Replay <span>soon</span></button></div></section>
    <nav className="tab-row" aria-label="Assessment rankings">{tabs.map((tab) => <button className={`tab-button ${activeTab === tab.id ? "active" : ""}`} disabled={!tab.available} key={tab.id} onClick={() => tab.available && setActiveTab(tab.id)} type="button">{tab.label}{!tab.available && <span className="tab-note">soon</span>}</button>)}</nav>
    <section className="stage-heading"><div><div className="stage-kicker"><span className="stage-number">01</span> Broad screening</div><h3>Stage 1 ranking</h3><p>Searching for periodic, transit-like brightness dips across the target universe.</p></div><button className="button button-outline" type="button" disabled>Replay <span className="button-note">available after first run</span></button></section>
    <section className="metrics-grid" aria-label="Processing summary"><article className="metric-card"><span className="metric-label">Target universe</span><strong>{targetCount.toLocaleString()}</strong><span className="metric-detail">stars queued for staged assessment</span></article><article className="metric-card"><span className="metric-label">Processed</span><strong>{processedCount.toLocaleString()}</strong><span className="metric-detail">targets with published evaluations</span></article><article className="metric-card"><span className="metric-label">Active candidates</span><strong>{activeCandidateCount.toLocaleString()}</strong><span className="metric-detail">currently visible in this assessment</span></article><article className="metric-card metric-card-status"><span className="metric-label">Pipeline status</span><strong><span className={`status-dot ${status?.data_version ? "" : "muted"}`} /> {pipelineState}</strong><span className="metric-detail">{footerState}</span></article></section>
    <section className="ranking-panel" aria-labelledby="ranking-title"><div className="panel-header"><div><p className="eyebrow">Top candidates · live ranking</p><h3 id="ranking-title">{panelTitle}</h3></div><span className="panel-meta">{rows.length} / 100 displayed · v{ranking?.data_version ?? "—"}</span></div>{error ? <div className="error-state" role="alert"><span className="status-dot muted" /><h4>Published state unavailable</h4><p>{error}</p><button className="button button-outline" type="button" onClick={() => void loadDashboard()}>Try again</button></div> : loading && !ranking ? <div className="empty-state" role="status"><div className="loading-ring" aria-hidden="true" /><h4>Reading the latest published state.</h4><p>The API is checking the worker’s durable publication boundary. No model is run during a dashboard load.</p></div> : rows.length ? <div className="table-wrap"><div className="ranking-table ranking-table-header" aria-hidden="true"><span>Rank</span><span>Candidate</span><span>Status</span><span>Current activity</span><span>Movement</span><span>Last update</span></div><div className="ranking-rows">{rows.map((row) => <button className="ranking-table ranking-table-row" key={row.candidate_id} type="button" onClick={() => void openCandidate(row.candidate_id)} aria-label={`Open details for ${row.target_id}`}><span className="rank-value">{row.rank}</span><span className="candidate-cell"><strong>{row.target_id}</strong><small>{row.candidate_id}</small></span><span><span className={`status-pill status-${statusClass(row.status)}`}>{row.status}</span><small className="score-label">Evidence {row.evidence_score?.toFixed(3) ?? "—"}</small></span><span className="activity-cell"><span className="activity-pulse" />{row.activity}</span><span className={`movement movement-${row.movement}`}>{movementLabel(row)}</span><span className="last-update">{formatDate(row.last_evaluated_at)}</span></button>)}</div></div> : <div className="empty-state" role="status"><div className="empty-orbit" aria-hidden="true"><span /></div><h4>The observatory is waiting for its first observation.</h4><p>The ranking will populate when a MAST/TESS evaluation is published. Empty is a real state here—not a hidden placeholder result.</p><div className="empty-state-meta"><span className="status-dot muted" /> No current activity</div></div>}</section>
    <footer className="footer-note"><span className="footer-pulse" aria-hidden="true" /><span>Data source: MAST / TESS · {footerState} · Evidence is provisional</span></footer>
    {modal}
  </main>;
}
