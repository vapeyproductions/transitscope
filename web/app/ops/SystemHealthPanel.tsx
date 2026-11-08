"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type WorkerProgress = {
  state?: string | null;
  updated_at?: string | null;
  candidate_id?: string | null;
  target_id?: string | null;
  reason?: string | null;
};

type WorkerStatus = {
  state?: string | null;
  mode?: string | null;
  last_heartbeat?: string | null;
  last_progress_at?: string | null;
  last_progress_state?: string | null;
  cycle_id?: string | null;
  cycle_started_at?: string | null;
  cycle_elapsed_seconds?: number | null;
  progress?: WorkerProgress | null;
  heartbeat_age_seconds?: number | null;
  progress_age_seconds?: number | null;
  last_stage1_publish_at?: string | null;
  last_stage1_published_count?: number | null;
  stage1_publish_age_seconds?: number | null;
  pipeline_paused?: boolean;
  health?: string | null;
  health_message?: string | null;
};

type LiveStatusResponse = {
  data_version?: number | null;
  processed_target_count?: number | null;
  total_target_count?: number | null;
  active_candidate_count?: number | null;
  active_star_count?: number | null;
  served_from_cache?: boolean;
  read_state?: string | null;
  checked_at?: string | null;
  worker?: WorkerStatus | null;
  stage_summary?: Record<string, {
    retained_candidates?: number | null;
    status_counts?: Record<string, number>;
    last_activity_at?: string | null;
  }> | null;
};

type PanelTone = "healthy" | "warning" | "critical" | "paused";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "";
const REFRESH_INTERVAL_MS = 15_000;

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    cache: "no-store",
    headers: { Accept: "application/json", ...(init?.headers ?? {}) },
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.detail?.message ?? "The live status check failed.");
  }
  return payload as T;
}

function formatDate(value: string | null | undefined): string {
  if (!value) return "Not recorded";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatMetric(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : Math.round(value).toLocaleString();
}

function formatAge(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return "not recorded";
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s ago`;
  const hours = Math.floor(seconds / 3600);
  return `${hours}h ${Math.floor((seconds % 3600) / 60)}m ago`;
}

function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function derivedAge(seconds: number | null | undefined, timestamp: string | null | undefined, now: number): number | null {
  if (seconds != null && Number.isFinite(seconds)) return Math.max(0, seconds);
  if (!timestamp) return null;
  const parsed = new Date(timestamp).getTime();
  return Number.isFinite(parsed) ? Math.max(0, (now - parsed) / 1000) : null;
}

function humanize(value: string | null | undefined): string {
  if (!value) return "Not reported";
  return value
    .replace(/^processing_/, "")
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function stageLabel(value: string | null | undefined): string {
  const match = value?.match(/stage[123]/i);
  return match ? `Stage ${match[0].slice(-1)}` : "No stage reported";
}

function stageName(value: string): string {
  const match = value.match(/stage([123])/i);
  return match ? `Stage ${match[1]}` : humanize(value);
}

function statusLabel(tone: PanelTone): string {
  if (tone === "healthy") return "Running normally";
  if (tone === "paused") return "Paused";
  if (tone === "critical") return "Worker not responding";
  return "Needs attention";
}

function StatusPill({ tone }: { tone: PanelTone }) {
  return <span className={`monitor-status-pill monitor-status-${tone}`}><span className="status-dot" />{statusLabel(tone)}</span>;
}

function Metric({ label, value, detail, tone = "default" }: { label: string; value: string; detail: string; tone?: "default" | PanelTone }) {
  return <article className={`monitor-metric monitor-metric-${tone}`}>
    <span>{label}</span>
    <strong>{value}</strong>
    <small>{detail}</small>
  </article>;
}

export default function SystemHealthPanel({ refreshKey }: { refreshKey: number }) {
  const [snapshot, setSnapshot] = useState<LiveStatusResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadStatus = useCallback(async () => {
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), 8_000);
    setLoading(true);
    try {
      const nextSnapshot = await fetchJson<LiveStatusResponse>("/api/status", { signal: controller.signal });
      setSnapshot(nextSnapshot);
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error && cause.name === "AbortError" ? "The live status check timed out." : cause instanceof Error ? cause.message : "The live status check failed.");
    } finally {
      window.clearTimeout(timeoutId);
      setLoading(false);
    }
  }, [refreshKey]);

  useEffect(() => {
    void loadStatus();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") {
        void loadStatus();
      }
    }, REFRESH_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [loadStatus]);

  const view = useMemo(() => {
    const now = Date.now();
    const worker = snapshot?.worker ?? {};
    const heartbeatAge = derivedAge(worker.heartbeat_age_seconds, worker.last_heartbeat, now);
    const progressAge = derivedAge(worker.progress_age_seconds, worker.last_progress_at ?? worker.progress?.updated_at, now);
    const workerResponding = heartbeatAge == null || heartbeatAge <= 120;
    const paused = worker.pipeline_paused === true;
    const publicationBusy = snapshot?.read_state === "publication_busy" || snapshot?.served_from_cache === true;
    const slowProgress = progressAge != null && progressAge > 120;
    const critical = !workerResponding || /stopped|error|failed/i.test(worker.state ?? "");
    const tone: PanelTone = paused ? "paused" : critical ? "critical" : publicationBusy || slowProgress ? "warning" : "healthy";
    const diagnosis = paused
      ? "The pipeline is paused. No new work should be expected until the control gate is reopened."
      : critical
        ? "The worker is not responding. This is the first thing to investigate."
        : publicationBusy
          ? "Worker alive, but public data is stale."
          : progressAge != null && progressAge > 600
            ? "The worker has not made progress within the restart threshold."
            : slowProgress
              ? "Worker alive, but the current job is taking a long time."
              : "Worker alive and publishing normally.";
    const explanation = paused
      ? ""
      : critical
        ? worker.health_message ?? "Heartbeat telemetry is outside the healthy window."
        : publicationBusy
          ? "The worker is still processing, but the public API is serving the previous committed snapshot while publication is busy."
          : slowProgress
            ? "The heartbeat is fresh, but progress telemetry has not advanced recently. The active job or its downstream publication may be the bottleneck."
            : "The worker heartbeat and progress telemetry are within the expected window.";

    return { worker, heartbeatAge, progressAge, paused, publicationBusy, slowProgress, tone, diagnosis, explanation };
  }, [snapshot]);

  if (loading && !snapshot) {
    return <section className="system-health-panel operations-panel"><div className="monitor-loading"><div className="loading-ring" aria-hidden="true" /><strong>Checking live pipeline status…</strong><span>Reading the worker heartbeat and latest published snapshot.</span></div></section>;
  }

  if (!snapshot) {
    return <section className="system-health-panel operations-panel"><div className="monitor-error" role="alert"><span className="status-dot muted" /><div><strong>Live pipeline status is unavailable.</strong><span>{error ?? "The status endpoint has not returned a snapshot yet."}</span></div><button className="button button-outline" type="button" onClick={() => void loadStatus()}>Try again</button></div></section>;
  }

  const worker = view.worker;
  const publicCount = snapshot.processed_target_count ?? null;
  const totalCount = snapshot.total_target_count ?? null;
  const workerCount = worker.last_stage1_published_count ?? null;
  const currentStage = stageLabel(worker.state);

  return <section className="system-health-panel operations-panel" aria-labelledby="system-health-title">
    <header className="monitor-header">
      <div>
        <p className="eyebrow accent">Live Pipeline Status</p>
        <h2 id="system-health-title">What is happening right now</h2>
        <p>Worker state, published data, and the current bottleneck in one view.</p>
      </div>
      <div className="monitor-header-status"><StatusPill tone={view.tone} /><small>{loading ? "Refreshing now…" : "Auto-refreshes every 15 seconds"}</small></div>
    </header>

    <div className="monitor-meta">
      <span><strong>Last checked</strong> {formatDate(snapshot.checked_at)}</span>
      <span><strong>Next check</strong> about 15 seconds</span>
      <span><strong>Snapshot</strong> {snapshot.data_version ?? "not recorded"}</span>
      <span><strong>Browser data</strong> {snapshot.served_from_cache || snapshot.read_state === "publication_busy" ? "cached while publishing" : "current"}</span>
    </div>

    {error && <div className="monitor-refresh-warning" role="status"><span className="status-dot" /><strong>Refresh failed.</strong><span>Showing the last successful snapshot until the next check succeeds.</span></div>}

    <div className={`monitor-diagnosis monitor-diagnosis-${view.tone}`}>
      <div><StatusPill tone={view.tone} /><strong>{view.diagnosis}</strong></div>
      <p>{view.explanation}</p>
    </div>

    <div className="monitor-metrics" aria-label="Current pipeline metrics">
      <Metric label="Worker" value={worker.state ? humanize(worker.state) : "Not reported"} detail={`${worker.mode ?? "mode unknown"} · heartbeat ${formatAge(view.heartbeatAge)}`} tone={view.tone === "critical" ? "critical" : "default"} />
      <Metric label="Pipeline" value={view.paused ? "Paused" : currentStage} detail={worker.cycle_id ? `Cycle ${worker.cycle_id}` : "No cycle reported"} tone={view.paused ? "paused" : "default"} />
      <Metric label="Processed targets" value={publicCount == null || totalCount == null ? "—" : `${formatMetric(publicCount)} / ${formatMetric(totalCount)}`} detail="Status projection · processed manifest targets" />
      <Metric label="Worker telemetry" value={formatMetric(workerCount)} detail="Last Stage 1 publish" tone={view.publicationBusy ? "warning" : "default"} />
      <Metric label="Active candidates" value={formatMetric(snapshot.active_candidate_count)} detail="Current candidate records" />
      <Metric label="Ranked stars" value={formatMetric(snapshot.active_star_count)} detail="Distinct stars in active rankings" />
      <Metric label="Cycle running" value={formatDuration(worker.cycle_elapsed_seconds)} detail={`Last progress ${formatAge(view.progressAge)}`} tone={view.slowProgress ? "warning" : "default"} />
    </div>
  </section>;
}
