"use client";

import { useCallback, useEffect, useState } from "react";

type AssessmentStage = "stage1" | "stage2" | "stage3";
type SystemHealthStatus = "healthy" | "warning" | "critical" | "unknown" | "paused";

type SystemHealthCheck = {
  id: string;
  label: string;
  status: SystemHealthStatus;
  summary: string;
  details: string[];
  source: string;
  latency_ms?: number | null;
  checked_at: string;
};

type SystemHealthStage = {
  stage: AssessmentStage;
  status: SystemHealthStatus;
  summary: string;
  details: string[];
  retained_candidates: number;
  priority_candidates: number;
  jobs_in_latest_queue: number;
  evaluated_in_window: number;
  handoffs_in_window: number;
  last_evaluated_at: string | null;
};

type SystemHealthResponse = {
  checked_at: string;
  next_check_at: string;
  refresh_interval_seconds: number;
  stale_after_seconds: number;
  overall_status: SystemHealthStatus;
  alerts: SystemHealthCheck[];
  checks: SystemHealthCheck[];
  pipeline: {
    data_version: number | null;
    universe: { configured_count?: number; processed_count?: number; remaining_count?: number };
    stages: SystemHealthStage[];
    latest_cycle_id?: string | null;
  };
  read_state?: string;
  served_from_cache?: boolean;
  source_of_truth: string[];
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "";

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    cache: "no-store",
    headers: { Accept: "application/json", ...(init?.headers ?? {}) },
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.detail?.message ?? "The live health check failed.");
  }
  return payload as T;
}

function formatDate(value: string | null | undefined): string {
  if (!value) return "Not recorded";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

function stageName(stage: string): string {
  if (stage === "stage1") return "Stage 1";
  if (stage === "stage2") return "Stage 2";
  if (stage === "stage3") return "Stage 3";
  return stage;
}

function systemHealthStatusLabel(status: SystemHealthStatus): string {
  if (status === "healthy") return "Healthy";
  if (status === "paused") return "Paused";
  if (status === "critical") return "Critical";
  if (status === "warning") return "Needs attention";
  return "Unknown";
}

function SystemHealthStatusPill({ status }: { status: SystemHealthStatus }) {
  return <span className={`system-health-status system-health-status-${status}`}><span className="status-dot" />{systemHealthStatusLabel(status)}</span>;
}

export default function SystemHealthPanel({ refreshKey }: { refreshKey: number }) {
  const [snapshot, setSnapshot] = useState<SystemHealthResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadHealth = useCallback(async () => {
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), 12000);
    setLoading(true);
    try {
      const nextSnapshot = await fetchJson<SystemHealthResponse>("/api/monitoring/system-health", { signal: controller.signal });
      setSnapshot(nextSnapshot);
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error && cause.name === "AbortError" ? "The live health check timed out." : cause instanceof Error ? cause.message : "The live health check failed.");
    } finally {
      window.clearTimeout(timeoutId);
      setLoading(false);
    }
  }, [refreshKey]);

  useEffect(() => {
    void loadHealth();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void loadHealth();
    }, 15000);
    return () => window.clearInterval(timer);
  }, [loadHealth]);

  if (loading && !snapshot) {
    return <section className="system-health-panel operations-panel"><div className="empty-state"><div className="loading-ring" aria-hidden="true" /><h4>Running source-of-truth checks.</h4><p>Checking the API, worker heartbeat, control gates, published state, providers, queues, and Railway status.</p></div></section>;
  }
  if (!snapshot) {
    return <section className="system-health-panel operations-panel"><div className="error-state" role="alert"><span className="status-dot muted" /><h4>System health is unavailable</h4><p>{error ?? "The health endpoint has not returned a check yet."}</p><button className="button button-outline" type="button" onClick={() => void loadHealth()}>Try again</button></div></section>;
  }

  const stale = Date.now() - new Date(snapshot.checked_at).getTime() > snapshot.stale_after_seconds * 1000;
  const alerts = snapshot.alerts;
  return <section className="system-health-panel operations-panel" aria-labelledby="system-health-title">
    <header className="system-health-hero">
      <div><p className="eyebrow accent">Operational truth layer</p><h2 id="system-health-title">System health and pipeline diagnostics</h2><p>Fresh checks of the worker, control, publication, provider, queue, and platform signals used to diagnose incidents.</p></div>
      <div className={`system-health-overall system-health-status-${snapshot.overall_status}`}><span className="status-dot" /><strong>{systemHealthStatusLabel(snapshot.overall_status)}</strong><small>{loading ? "Refreshing now" : `Polls every ${snapshot.refresh_interval_seconds}s`}</small></div>
    </header>
    <div className="system-health-meta"><span><strong>Last checked</strong> {formatDate(snapshot.checked_at)}</span><span><strong>Next check</strong> {formatDate(snapshot.next_check_at)}</span><span><strong>Data version</strong> {snapshot.pipeline.data_version ?? "—"}</span><span><strong>Read state</strong> {snapshot.read_state ?? "published"}{snapshot.served_from_cache ? " · cached projection" : ""}</span></div>
    {error && <div className="system-health-refresh-warning" role="status"><span className="status-dot" /><strong>Live refresh failed.</strong><span>{error} Showing the last successful check from {formatDate(snapshot.checked_at)} until the next check succeeds.</span></div>}
    {stale && !error && <div className="system-health-refresh-warning" role="status"><span className="status-dot" /><strong>Check is stale.</strong><span>The last response is older than the declared {snapshot.stale_after_seconds}-second freshness window.</span></div>}
    <section className="system-health-alerts" aria-labelledby="system-health-alerts-title"><div className="ops-section-heading"><div><p className="eyebrow">Action queue</p><h3 id="system-health-alerts-title">Active alerts</h3></div><span className="section-note">{alerts.length ? `${alerts.length} check${alerts.length === 1 ? "" : "s"} need attention` : "No active alerts"}</span></div>{alerts.length ? <div className="system-health-alert-list">{alerts.map((check) => <article className={`system-health-alert system-health-alert-${check.status}`} key={check.id}><SystemHealthStatusPill status={check.status} /><div><strong>{check.label}</strong><p>{check.summary}</p><small>Source: {check.source}</small></div></article>)}</div> : <div className="system-health-clear"><span className="status-dot" />Every check returned healthy at the last poll.</div>}</section>
    <section className="system-health-checks" aria-labelledby="system-health-checks-title"><div className="ops-section-heading"><div><p className="eyebrow">Live checks</p><h3 id="system-health-checks-title">What is being checked right now</h3></div><span className="section-note">Each card includes its authoritative source</span></div><div className="system-health-check-grid">{snapshot.checks.map((check) => <article className={`system-health-check-card system-health-card-${check.status}`} key={check.id}><div className="system-health-card-heading"><strong>{check.label}</strong><SystemHealthStatusPill status={check.status} /></div><p>{check.summary}</p>{check.details.length ? <ul>{check.details.map((detail) => <li key={detail}>{detail}</li>)}</ul> : null}<footer><span>{check.source}</span>{check.latency_ms != null && <span>{check.latency_ms.toFixed(0)} ms</span>}</footer></article>)}</div></section>
    <section className="system-health-pipeline" aria-labelledby="system-health-pipeline-title"><div className="ops-section-heading"><div><p className="eyebrow">Waterfall state</p><h3 id="system-health-pipeline-title">Stage-by-stage pipeline check</h3></div><span className="section-note">Queue counts are from the latest durable cycle telemetry</span></div><div className="system-health-stage-grid">{snapshot.pipeline.stages.map((stage) => <article className={`system-health-stage-card system-health-card-${stage.status}`} key={stage.stage}><div className="system-health-card-heading"><strong>{stageName(stage.stage)}</strong><SystemHealthStatusPill status={stage.status} /></div><p>{stage.summary}</p><div className="system-health-stage-metrics"><span><strong>{stage.retained_candidates.toLocaleString()}</strong>retained</span><span><strong>{stage.priority_candidates.toLocaleString()}</strong>priority</span><span><strong>{stage.jobs_in_latest_queue.toLocaleString()}</strong>latest jobs</span><span><strong>{stage.evaluated_in_window.toLocaleString()}</strong>assessed</span></div><ul>{stage.details.map((detail) => <li key={detail}>{detail}</li>)}</ul><small>Last assessed: {formatDate(stage.last_evaluated_at)}</small></article>)}</div></section>
    <footer className="system-health-footer"><div><strong>Source of truth</strong><span>{snapshot.source_of_truth.join(" · ")}</span></div><span>Last cycle: {snapshot.pipeline.latest_cycle_id ?? "not recorded"}</span></footer>
  </section>;
}
