"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "";

type RankingView = "overall" | "stage1" | "stage2" | "stage3";
type AssessmentStage = "stage1" | "stage2" | "stage3";
type CandidateStatus = "Promising" | "Needs More Data" | "Inconclusive" | "Known Match" | "Duplicate Signal";
type AssessmentSort = "rank" | "last_assessed_desc" | "last_assessed_asc";
type RankingControls = { stage: AssessmentStage | null; status: CandidateStatus | null; sort: AssessmentSort };

type DashboardStatus = {
  data_version: number | null;
  processed_target_count: number;
  total_target_count: number;
  active_candidate_count: number;
  last_ingest_at: string | null;
  worker: { state?: string; last_heartbeat?: string | null; next_update_at?: string | null };
};

type RankingRow = {
  rank: number;
  candidate_id: string;
  target_id: string;
  stage: string;
  status: CandidateStatus;
  evidence_score: number | null;
  overall_score?: number | null;
  ranking_rationale?: string;
  activity: string;
  rank_delta: number;
  movement: string;
  last_evaluated_at: string | null;
  signal_index?: number | null;
  signal_resolution?: "independent" | "primary" | "pending_alias_review" | "released_for_independent_review" | string;
  alias_of_candidate_id?: string | null;
  promotion_eligible?: boolean;
};

type RankingResponse = {
  view: RankingView;
  data_version: number | null;
  sequence_number: number | null;
  occurred_at: string | null;
  processed_target_count: number;
  total_target_count: number;
  page?: number;
  page_size?: number;
  display_cap?: number;
  total_ranked_count?: number;
  total_star_count?: number;
  page_count?: number;
  rows: RankingRow[];
  removed?: ReplayRemoval[];
};

type ReplayRemoval = {
  candidate_id: string;
  target_id: string;
  stage: string;
  status: "Known Match";
  known_match_id?: string | null;
  signal_group_id?: string | null;
  signal_resolution?: "independent" | "primary" | "pending_alias_review" | "released_for_independent_review" | string;
  alias_of_candidate_id?: string | null;
  promotion_eligible?: boolean;
  rank: number | null;
  activity: string;
  removal_reason: string;
  occurred_at: string | null;
  signal_index?: number | null;
};

type ReplayMeta = {
  view: RankingView;
  data_version: number | null;
  initial_sequence: number;
  first_sequence: number | null;
  last_sequence: number;
  snapshot_count: number;
  available_sequences: number[];
  first_occurred_at: string | null;
  last_occurred_at: string | null;
};

type ReplayState = {
  active: boolean;
  playing: boolean;
  cursor: number;
  maxSequence: number;
  speed: number;
  meta: ReplayMeta | null;
  snapshot: RankingResponse | null;
  loading: boolean;
  error: string | null;
};

type CandidateDetail = {
  candidate_id: string;
  target_id: string;
  stage: string;
  status: CandidateStatus;
  evidence_score: number | null;
  ranking_rationale?: string;
  overall_score: number | null;
  rank_overall: number | null;
  rank_stage: number | null;
  activity: string;
  last_evaluated_at: string | null;
  known_match_id?: string | null;
  signal_index?: number | null;
  signal_group_id?: string | null;
  signal_resolution?: "independent" | "primary" | "pending_alias_review" | "released_for_independent_review" | string;
  alias_of_candidate_id?: string | null;
  promotion_eligible?: boolean;
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
    evaluation_id?: string;
    model_version: string;
    input_product_ids: string[];
    features: Record<string, unknown>;
    metrics: Record<string, unknown>;
    classification?: CandidateStatus;
    data_sufficiency: string;
    created_at?: string | null;
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

type IdentifiedPerformance = {
  status: CandidateStatus | null;
  evidence_score: number | null;
  model_version: string | null;
  data_sufficiency: string | null;
  evaluated_at: string | null;
  coverage_days: number | null;
  observed_event_count: number | null;
  holdout_f1: number | null;
  model_lift: number | null;
  metrics: Record<string, unknown>;
};

type IdentifiedRow = {
  candidate_id: string;
  target_id: string;
  signal_index?: number | null;
  identified_stage: string;
  identified_at: string | null;
  known_match_id: string | null;
  overall_score: number | null;
  activity: string;
  identified_as: {
    object_name: string | null;
    catalog_id: string | null;
    table: string | null;
    disposition: string | null;
    match_kind: string | null;
    match_score: number | null;
    period_days: number | null;
    coordinate_separation_deg: number | null;
    period_delta_fraction: number | null;
  };
  performance: Partial<Record<AssessmentStage, IdentifiedPerformance>>;
};

type IdentifiedResponse = {
  data_version: number | null;
  count: number;
  rows: IdentifiedRow[];
};

type MonitoringStage = {
  stage: AssessmentStage;
  threshold: number | null;
  priority_label: string;
  priority_candidates: number;
  total_candidates: number;
  evaluated_candidate_count: number;
  counts: Record<string, number>;
  last_evaluated_at: string | null;
};

type MonitoringModel = {
  stage: AssessmentStage;
  model_version: string;
  evaluation_count: number;
  status_counts: Record<string, number>;
  data_sufficiency_counts: Record<string, number>;
  mean_evidence_score: number | null;
  mean_holdout_f1: number | null;
  mean_model_lift: number | null;
  last_evaluated_at: string | null;
};

type MonitoringBucket = {
  bucket: string;
  total: number;
  processed: number;
  observed: number;
  promising: number;
  needs_more_data: number;
  inconclusive: number;
  known_match: number;
  known_match_rate: number;
  recommendation: string;
};

type MonitoringCluster = {
  ra_min_deg: number;
  dec_min_deg: number;
  observed: number;
  promising: number;
  needs_more_data: number;
  inconclusive: number;
  known_match: number;
  recommendation: string;
};

type CatalogHealth = {
  status?: string | null;
  provider_status?: string | null;
  matches?: number;
  record_count?: number;
  fetched_at?: string | null;
  complete_projection?: boolean;
  stages?: Array<{ stage?: string | null; status?: string | null; matches?: number; catalog_status?: string | null }>;
};

type MonitoringOverview = {
  generated_at: string;
  data_version: number | null;
  control: {
    pipeline_paused: boolean;
    autostart_enabled: boolean;
    reason: string;
  };
  worker: { state?: string; last_heartbeat?: string | null; reason?: string; catalog_health?: CatalogHealth };
  universe: {
    configured_count: number;
    manifest_count: number;
    processed_count: number;
    queued_count?: number;
    unavailable_count?: number;
    remaining_count: number;
    progress_percent: number;
    initial_stage1_pool: number;
  };
  stages: Record<AssessmentStage, MonitoringStage>;
  models: MonitoringModel[];
  sampling: {
    policy: string;
    adaptive_enabled: boolean;
    cursor: number;
    universe_count: number;
    processed_count: number;
    remaining_count: number;
    updated_at?: string | null;
    buckets: MonitoringBucket[];
    spatial_clusters: MonitoringCluster[];
    explanation: string;
  };
  data_health: {
    providers: Array<{
      provider: string;
      runs: number;
      successful_runs: number;
      failed_runs: number;
      products_downloaded: number;
      products_failed: number;
      last_finished_at: string | null;
    }>;
    coverage: {
      candidate_count: number;
      product_count: number;
      window_count: number;
      last_updated_at: string | null;
    };
    recent_batches: Array<{
      batch_id: string;
      stage: string;
      selection_policy: string;
      requested_count: number;
      selected_count: number;
      evaluated_count: number;
      failed_count: number;
      status: string;
      started_at: string | null;
      finished_at: string | null;
    }>;
    recent_events: Array<{
      sequence_number: number;
      stage: string;
      event_type: string;
      occurred_at: string | null;
      model_run_id: string | null;
    }>;
  };
};

const tabs: Array<{ id: RankingView; label: string }> = [
  { id: "overall", label: "Overall" },
  { id: "stage1", label: "Stage 1" },
  { id: "stage2", label: "Stage 2" },
  { id: "stage3", label: "Stage 3" },
];

const assessmentStages: AssessmentStage[] = ["stage1", "stage2", "stage3"];
const statusFilterOptions: CandidateStatus[] = ["Promising", "Needs More Data", "Inconclusive", "Duplicate Signal"];

function createRankingControls(): Record<RankingView, RankingControls> {
  const controls: RankingControls = { stage: null, status: null, sort: "rank" };
  return { overall: { ...controls }, stage1: { ...controls }, stage2: { ...controls }, stage3: { ...controls } };
}

function createReplayStates(): Record<RankingView, ReplayState> {
  const state: ReplayState = {
    active: false,
    playing: false,
    cursor: 0,
    maxSequence: 0,
    speed: 1,
    meta: null,
    snapshot: null,
    loading: false,
    error: null,
  };
  return { overall: { ...state }, stage1: { ...state }, stage2: { ...state }, stage3: { ...state } };
}

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

function humanizeState(value: string | undefined): string {
  return value ? value.replaceAll("_", " ").replace(/\b\w/g, (character) => character.toUpperCase()) : "Unknown";
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

function stageName(stage: string): string {
  if (stage === "stage1") return "Stage 1";
  if (stage === "stage2") return "Stage 2";
  if (stage === "stage3") return "Stage 3";
  return stage;
}

function displayStatus(status: string, stage?: string): string {
  return status === "Promising" && stage === "stage3" ? "Prime Candidate" : status;
}

function RankingColumnControl({
  label,
  active,
  open,
  options,
  ariaLabel,
  onToggle,
  onSelect,
  onClear,
}: {
  label: string;
  active: boolean;
  open: boolean;
  options: Array<{ value: string; label: string }>;
  ariaLabel: string;
  onToggle: () => void;
  onSelect: (value: string) => void;
  onClear: () => void;
}) {
  return <div className="column-control" onClick={(event) => event.stopPropagation()}>
    <button className={`column-control-trigger${active ? " active" : ""}`} type="button" aria-haspopup="menu" aria-expanded={open} aria-label={ariaLabel} onClick={onToggle}>
      <span>{label}</span><span className="column-control-arrow" aria-hidden="true">⌄</span>
    </button>
    {active && <button className="column-control-clear" type="button" aria-label={`Clear ${label} selection`} onClick={onClear}>×</button>}
    {open && <div className="column-control-menu" role="menu">
      {options.map((option) => <button className="column-control-option" key={option.value} type="button" role="menuitem" onClick={() => onSelect(option.value)}>{option.label}</button>)}
    </div>}
  </div>;
}

function signalLetter(value: unknown): string | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  let index = Math.trunc(value);
  if (index < 1) return null;
  let label = "";
  while (index > 0) {
    index -= 1;
    label = String.fromCharCode(65 + (index % 26)) + label;
    index = Math.floor(index / 26);
  }
  return label;
}

function signalLabel(candidateId: string, features?: Record<string, unknown>, signalIndex?: number | null): string | null {
  const index = signalIndex ?? features?.signal_index;
  const letter = signalLetter(index);
  if (letter) return `Signal ${letter}`;

  const numericMatch = candidateId.match(/-signal-(\d+)$/);
  if (numericMatch) return `Signal ${signalLetter(Number(numericMatch[1])) ?? "A"}`;

  // Stable-hash identities predate the durable signal ordinal. This fallback
  // keeps legacy rows readable until their signal metadata is available.
  const stable = candidateId.match(/-signal-([a-f0-9]{8,})$/i);
  if (stable) {
    const fallbackIndex = (Number.parseInt(stable[1].slice(0, 6), 16) % 26) + 1;
    return `Signal ${signalLetter(fallbackIndex)}`;
  }
  return "Signal A";
}

function candidateDisplayLabel(targetId: string, candidateId: string, features?: Record<string, unknown>, signalIndex?: number | null): string {
  return `${targetId} - ${signalLabel(candidateId, features, signalIndex) ?? "Signal A"}`;
}

function signalResolutionLabel(resolution?: string): string | null {
  if (resolution === "primary") return "Primary signal"
  if (resolution === "pending_alias_review") return "Queued for independence assessment"
  if (resolution === "pending_independence_review") return "Independence assessment needs more data"
  if (resolution === "duplicate_signal") return "Excluded as duplicate signal"
  if (resolution === "released_for_independent_review") return "Released for independent review"
  return null
}

function stageCopy(view: RankingView): { number: string; kicker: string; title: string; description: string } {
  if (view === "overall") {
    return { number: "00", kicker: "Observatory overview", title: "Overall ranking", description: "The active candidates with the strongest combined evidence across the assessment pipeline." };
  }
  if (view === "stage2") {
    return { number: "02", kicker: "Historical repeatability", title: "Stage 2 ranking", description: "Testing whether a transit-like signal repeats across a longer historical window." };
  }
  if (view === "stage3") {
    return { number: "03", kicker: "Chronological holdout", title: "Stage 3 ranking", description: "Training on two observations and testing whether the next one can be predicted." };
  }
  return { number: "01", kicker: "Broad screening", title: "Stage 1 ranking", description: "Searching for periodic, transit-like brightness dips across the target universe." };
}

function objectValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function numberLabel(value: number | null | undefined, digits = 1): string {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "—";
}

function identifiedPerformanceStats(performance: IdentifiedPerformance): string {
  const stats: string[] = [];
  if (typeof performance.coverage_days === "number") stats.push(`${numberLabel(performance.coverage_days)} d coverage`);
  if (typeof performance.observed_event_count === "number") stats.push(`${numberLabel(performance.observed_event_count, 0)} events`);
  if (typeof performance.holdout_f1 === "number") stats.push(`F1 ${numberLabel(performance.holdout_f1, 2)}`);
  if (typeof performance.model_lift === "number") stats.push(`lift ${performance.model_lift >= 0 ? "+" : ""}${numberLabel(performance.model_lift, 2)}`);
  return stats.join(" · ") || "No secondary stats";
}

function LightCurveChart({ points, emptyMessage, signalFeatures }: { points: LightCurveResponse["points"]; emptyMessage: string; signalFeatures?: Record<string, unknown> }) {
  if (!points.length) return <div className="chart-empty">{emptyMessage}</div>;
  const width = 760;
  const height = 220;
  const padding = 18;
  const minTime = points[0].time;
  const maxTime = points[points.length - 1].time || minTime + 1;
  const values = points.map((point) => point.normalized_flux);
  const minFlux = Math.min(...values);
  const maxFlux = Math.max(...values);
  const fluxRange = maxFlux - minFlux || 0.001;
  const cadenceGaps = points.slice(1)
    .map((point, index) => point.time - points[index].time)
    .filter((delta) => Number.isFinite(delta) && delta > 0)
    .sort((left, right) => left - right);
  const typicalCadence = cadenceGaps.length
    ? cadenceGaps[Math.floor(cadenceGaps.length / 2)]
    : 0;
  const gapThreshold = typicalCadence > 0 ? typicalCadence * 10 : Infinity;
  const paths: string[] = [];
  let currentPath = "";
  points.forEach((point, index) => {
    const x = padding + ((point.time - minTime) / (maxTime - minTime || 1)) * (width - padding * 2);
    const y = height - padding - ((point.normalized_flux - minFlux) / fluxRange) * (height - padding * 2);
    const timeGap = index > 0 ? point.time - points[index - 1].time : 0;
    const startsNewObservation = index === 0 || timeGap > gapThreshold;
    if (startsNewObservation) {
      if (currentPath) paths.push(currentPath);
      currentPath = `M${x.toFixed(2)} ${y.toFixed(2)}`;
    } else {
      currentPath += ` L${x.toFixed(2)} ${y.toFixed(2)}`;
    }
  });
  if (currentPath) paths.push(currentPath);
  const period = typeof signalFeatures?.period_days === "number" ? signalFeatures.period_days : null;
  const transitTime = typeof signalFeatures?.transit_time === "number" ? signalFeatures.transit_time : null;
  const markerTimes: number[] = [];
  if (period && period > 0 && transitTime !== null) {
    let marker = transitTime + Math.ceil((minTime - transitTime) / period) * period;
    while (marker <= maxTime && markerTimes.length < 120) {
      markerTimes.push(marker);
      marker += period;
    }
  }
  return <div className="chart-wrap"><svg aria-label="Normalized light curve with selected signal windows" role="img" viewBox={`0 0 ${width} ${height}`}><line className="chart-baseline" x1={padding} x2={width - padding} y1={height / 2} y2={height / 2} />{markerTimes.map((marker) => { const x = padding + ((marker - minTime) / (maxTime - minTime || 1)) * (width - padding * 2); return <line className="chart-transit-marker" key={marker} x1={x} x2={x} y1={padding} y2={height - padding} />; })}{paths.map((path, index) => <path className="chart-line" d={path} key={index} />)}</svg><div className="chart-axis"><span>{minTime.toFixed(2)} BJD−2457000</span><span className="chart-gap-note">{markerTimes.length ? "Vertical markers show this signal's predicted transits" : "Observation gaps are not connected"}</span><span>{maxTime.toFixed(2)}</span></div></div>;
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="detail-metric"><span>{label}</span><strong>{value}</strong></div>;
}

function CandidateModal({ candidate, curve, history, replaySequence, onClose }: { candidate: CandidateDetail; curve: LightCurveResponse | null; history: HistoryResponse | null; replaySequence: number | null; onClose: () => void }) {
  const closeRef = useRef<HTMLButtonElement>(null);
  const features = candidate.evaluation?.features ?? {};
  const metrics = candidate.evaluation?.metrics ?? {};
  const holdout = objectValue(features.holdout_metrics ?? metrics.holdout_metrics);
  const baseline = objectValue(features.baseline_metrics ?? metrics.baseline_metrics);
  const inputProducts = candidate.evaluation?.input_product_ids ?? [];
  const productCount = typeof features.product_count === "number" ? features.product_count : inputProducts.length;
  const windowCount = Array.isArray(features.windows) ? features.windows.length : 0;
  const insufficientStage3 = candidate.stage === "stage3" && candidate.evaluation?.data_sufficiency === "insufficient";
  const emptyCurveMessage = candidate.stage === "stage3" && productCount > 0
    ? "Stage 3 input data was examined, but no normalized curve was published for this insufficient-data evaluation."
    : "Normalized series is not available yet.";
  const numberFeature = (key: string, digits = 2) => {
    const value = features[key];
    return typeof value === "number" ? value.toFixed(digits) : "—";
  };
  const percentFeature = (key: string, digits = 1) => {
    const value = features[key];
    return typeof value === "number" ? `${(value * 100).toFixed(digits)}%` : "—";
  };
  const nestedNumber = (source: Record<string, unknown>, key: string, digits = 2) => typeof source[key] === "number" ? Number(source[key]).toFixed(digits) : "—";
  const displayMetrics = candidate.status === "Known Match" ? [
    ["Catalog match", candidate.known_match_id ?? "Mapped"],
    ["Last evidence", candidate.evidence_score?.toFixed(3) ?? "—"],
    ["Assessment stage", stageName(candidate.stage)],
  ] : candidate.stage === "stage2" ? [
    ["Coverage", `${numberFeature("coverage_days")} days`],
    ["Observed events", numberFeature("observed_event_count", 0)],
    ["Timing consistency", numberFeature("timing_consistency")],
    ["Depth consistency", numberFeature("depth_consistency")],
    ["Shape consistency", numberFeature("shape_consistency")],
    ["Data sufficiency", candidate.evaluation?.data_sufficiency ?? "—"],
  ] : candidate.stage === "stage3" ? [
    ["Stage 1 evidence", nestedNumber(metrics, "stage1_score", 3)],
    ["Stage 2 evidence", nestedNumber(metrics, "stage2_score", 3)],
    ["Products analyzed", productCount ? String(productCount) : "—"],
    ["Samples examined", numberFeature("total_samples", 0)],
    ["Holdout F1", insufficientStage3 ? "Not computed" : nestedNumber(holdout, "f1")],
    ["Precision", insufficientStage3 ? "Not computed" : nestedNumber(holdout, "precision")],
    ["Recall", insufficientStage3 ? "Not computed" : nestedNumber(holdout, "recall")],
    ["Event recovery", insufficientStage3 ? "Not computed" : nestedNumber(holdout, "event_recovery_rate")],
    ["Baseline F1", insufficientStage3 ? "Not computed" : nestedNumber(baseline, "f1")],
    ["Model lift", insufficientStage3 ? "Not computed" : numberFeature("model_vs_baseline_f1_delta")],
  ] : [
    ["Signal", signalLabel(candidate.candidate_id, features, candidate.signal_index) ?? "Signal A"],
    ["Best period", `${numberFeature("period_days")} days`],
    ["Transit depth", `${(Number(features.depth ?? 0) * 100).toFixed(3)}%`],
    ["BLS SNR", numberFeature("bls_snr")],
    ["Events observed", numberFeature("observed_transit_count", 0)],
    ["Coverage", `${numberFeature("coverage_days")} days`],
    ["Usable data", percentFeature("usable_data_fraction")],
  ];

  useEffect(() => {
    closeRef.current?.focus();
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const visibleHistory = history?.events.filter((event) => replaySequence === null || event.sequence_number <= replaySequence) ?? [];
  const signal = signalLabel(candidate.candidate_id, features, candidate.signal_index);
  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose(); }}>
    <section className="candidate-modal" role="dialog" aria-modal="true" aria-labelledby="candidate-modal-title">
      <header className="modal-header"><div><p className="eyebrow accent">Candidate investigation · {stageName(candidate.stage)}</p><h3 id="candidate-modal-title">{candidate.target_id}{signal ? ` - ${signal}` : ""}</h3><p className="modal-subtitle">{candidate.target?.source_product_family ?? "MAST / TESS observation"}</p></div><button ref={closeRef} className="modal-close" type="button" onClick={onClose} aria-label="Close candidate details">×</button></header>
      <div className="modal-summary"><span className="stage-status-stage">{stageName(candidate.stage)}</span>{" "}<span className={`status-pill status-${statusClass(displayStatus(candidate.status, candidate.stage))}`}>{displayStatus(candidate.status, candidate.stage)}</span>{signalResolutionLabel(candidate.signal_resolution) && <span className="evidence-chip">{signalResolutionLabel(candidate.signal_resolution)}</span>}<span className="modal-summary-note">{candidate.ranking_rationale ?? candidate.activity}</span>{replaySequence !== null && <span className="replay-chip">Replay view</span>}</div>
      <div className="modal-content-grid">
        <div className="modal-main-column">
          <div className="modal-section"><div className="section-heading"><div><p className="eyebrow">Normalized observation</p><h4>Light curve</h4></div><span className="section-note">{curve?.points.length ?? 0} plotted points</span></div><LightCurveChart points={curve?.points ?? []} signalFeatures={features} emptyMessage={emptyCurveMessage} /></div>
          <div className="modal-section"><div className="section-heading"><div><p className="eyebrow">Interpretable evidence</p><h4>Assessment metrics</h4></div></div><div className="detail-metrics">{displayMetrics.map(([label, value]) => <Metric key={label} label={label} value={value} />)}</div></div>
          {candidate.stage === "stage3" && <div className="next-action stage3-context"><span>Stage 3 context</span><p>{insufficientStage3 ? "This candidate was admitted after its Stage 2 result was eligible for Stage 3. The Stage 3 test did not have a recoverable event in every chronological holdout, so no predictive metrics were produced yet." : "This candidate has a completed chronological holdout assessment."}</p><p>{productCount ? `${productCount} MAST / TESS products · ${numberFeature("total_samples", 0)} samples · ${windowCount || "—"} chronological windows.` : "No Stage 3 products are attached to this evaluation yet."}</p></div>}
        </div>
        <aside className="modal-side-column">
          <div className="detail-card"><p className="eyebrow">Current assessment</p><dl className="detail-list"><div><dt>Overall rank</dt><dd>#{candidate.rank_overall ?? "—"}</dd></div><div><dt>Stage rank</dt><dd>#{candidate.rank_stage ?? "—"}</dd></div><div><dt>Last evaluation</dt><dd>{formatDate(candidate.last_evaluated_at)}</dd></div><div><dt>Data sufficiency</dt><dd>{candidate.evaluation?.data_sufficiency ?? "—"}</dd></div><div><dt>Sector</dt><dd>{candidate.target?.sector ?? "—"}</dd></div></dl><div className="next-action"><span>Next evidence</span><p>{candidate.next_action}</p></div></div>
          <div className="detail-card"><p className="eyebrow">Pipeline history</p><div className="timeline">{visibleHistory.length ? visibleHistory.map((event) => <div className="timeline-event" key={`${event.sequence_number}-${event.event_type}`}><span className="timeline-dot" /><div><strong>{event.event_type.replaceAll("_", " ")}</strong><span>{formatDate(event.occurred_at)}</span></div></div>) : <p className="muted-copy">No pipeline events recorded at this replay position.</p>}</div></div>
        </aside>
      </div>
      <footer className="modal-footer"><span>Coordinates: {candidate.target?.ra_deg?.toFixed(4) ?? "—"}°, {candidate.target?.dec_deg?.toFixed(4) ?? "—"}°</span><span>Evidence is provisional · not scientific confirmation</span></footer>
    </section>
  </div>;
}

function IdentifiedPanel({ onOpenCandidate, refreshKey }: { onOpenCandidate: (candidateId: string) => void; refreshKey: number }) {
  const [response, setResponse] = useState<IdentifiedResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    void fetchJson<IdentifiedResponse>("/api/identified?limit=100")
      .then((nextResponse) => {
        if (!cancelled) setResponse(nextResponse);
      })
      .catch((cause) => {
        if (!cancelled) setError(cause instanceof Error ? cause.message : "Known-object records are unavailable.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [refreshKey]);

  const rows = response?.rows ?? [];
  return <section className="identified-panel ranking-panel" aria-labelledby="identified-title">
    <header className="identified-header"><div><p className="eyebrow accent">Catalog cross-reference · retained history</p><h3 id="identified-title">Identified objects</h3><p>Known catalog matches are removed from active rankings, but remain here with the evidence that led to the mapping.</p></div><div className="identified-count"><strong>{loading ? "—" : response?.count ?? 0}</strong><span>known matches</span></div></header>
    {error ? <div className="error-state" role="alert"><span className="status-dot muted" /><h4>Identified records unavailable</h4><p>{error}</p></div> : loading ? <div className="empty-state" role="status"><div className="loading-ring" aria-hidden="true" /><h4>Reading catalog matches.</h4><p>Loading persisted match events and stage performance. No model is run for this view.</p></div> : rows.length ? <div className="identified-table-wrap"><div className="identified-table identified-table-header" aria-hidden="true"><span>Star</span><span>Identified as</span><span>Step found</span><span>Performance by step</span><span>Match evidence</span><span>Identified</span></div><div className="identified-rows">{rows.map((row) => { const identified = row.identified_as; return <button className="identified-table identified-row" key={row.candidate_id} type="button" onClick={() => onOpenCandidate(row.candidate_id)} aria-label={`Open identified object details for ${candidateDisplayLabel(row.target_id, row.candidate_id, undefined, row.signal_index)}`}><span className="identified-target"><strong>{candidateDisplayLabel(row.target_id, row.candidate_id, undefined, row.signal_index)}</strong><small>{row.candidate_id}</small></span><span className="identified-as"><strong>{identified.object_name ?? row.known_match_id ?? "Catalog object"}</strong><small>{identified.table ?? "Catalog"}{identified.disposition ? ` · ${identified.disposition}` : ""} · {identified.match_kind ?? "catalog cross-reference"}</small></span><span className="identified-stage"><span className="stage-badge">{stageName(row.identified_stage)}</span><small>{row.activity}</small></span><span className="performance-stack">{assessmentStages.map((stage) => { const performance = row.performance[stage]; return <span className={`performance-chip ${performance ? statusClass(performance.status ?? "") : "performance-chip-empty"}`} key={stage}><strong>{stageName(stage)}</strong><span>{performance ? `Evidence ${numberLabel(performance.evidence_score)}` : "Not run"}</span>{performance && <small>{performance.status ?? "—"} · {identifiedPerformanceStats(performance)}</small>}</span>; })}</span><span className="match-proof"><strong>{numberLabel(identified.match_score, 0)} / 100 · Overall {numberLabel(row.overall_score)}</strong><small>{typeof identified.period_delta_fraction === "number" ? `ΔP ${(identified.period_delta_fraction * 100).toFixed(2)}%` : "Period comparison —"} · {typeof identified.coordinate_separation_deg === "number" ? `${identified.coordinate_separation_deg.toFixed(4)}° sep` : "Coordinate comparison —"}</small></span><span className="identified-date">{formatDate(row.identified_at)}</span></button>; })}</div></div> : <div className="empty-state" role="status"><div className="empty-orbit" aria-hidden="true"><span /></div><h4>No known catalog matches yet.</h4><p>When the catalog cross-reference identifies an active candidate, it will leave the rankings and appear here with its persisted performance trail.</p></div>}
    <footer className="identified-footer"><span className="status-dot muted" /> Click a row to inspect its full light curve and pipeline history. Catalog identification is a cross-reference, not scientific confirmation.</footer>
  </section>;
}

function monitoringStatusLabel(status: string): string {
  if (status === "Needs More Data") return "Needs data";
  if (status === "Known Match") return "Known";
  return status;
}

export function MonitoringPanel({ refreshKey }: { refreshKey: number }) {
  const [overview, setOverview] = useState<MonitoringOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pollKey, setPollKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    void fetchJson<MonitoringOverview>("/api/monitoring/overview")
      .then((nextOverview) => {
        if (!cancelled) setOverview(nextOverview);
      })
      .catch((cause) => {
        if (!cancelled) setError(cause instanceof Error ? cause.message : "ML operations data is unavailable.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [pollKey, refreshKey]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") {
        setPollKey((current) => current + 1);
      }
    }, 15000);
    return () => {
      window.clearInterval(timer);
    };
  }, []);

  const stages = overview ? Object.values(overview.stages) : [];
  const catalogHealth = overview?.worker.catalog_health;
  const catalogBusy = overview?.worker.state === "matching_catalog";
  const catalogSummary = catalogBusy
    ? "Scanning archive"
    : catalogHealth?.status === "matched"
    ? `${catalogHealth.matches ?? 0} match${catalogHealth.matches === 1 ? "" : "es"}`
    : catalogHealth?.status === "no_matches"
      ? "No matches"
      : "Awaiting first scan";
  const catalogDetail = catalogHealth?.record_count
    ? `${catalogHealth.record_count.toLocaleString()} archive rows · ${catalogHealth.complete_projection ? "complete matching projection" : "bounded projection"}`
    : catalogHealth?.provider_status
      ? `Archive status: ${catalogHealth.provider_status}`
      : "No catalog scan recorded yet";

  if (loading && !overview) {
    return <section className="operations-panel"><div className="empty-state"><div className="loading-ring" aria-hidden="true" /><h4>Reading ML operations state.</h4><p>This view only reads persisted metrics. It never starts the worker.</p></div></section>;
  }
  if (error && !overview) {
    return <section className="operations-panel"><div className="error-state" role="alert"><span className="status-dot muted" /><h4>ML operations data unavailable</h4><p>{error}</p></div></section>;
  }
  if (!overview) return null;

  return <section className="operations-panel" aria-labelledby="operations-title">
    <header className="operations-header">
      <div><p className="eyebrow accent">ML engineer view · read-only control plane</p><h3 id="operations-title">Pipeline operations</h3><p>Monitor the 126,000-target search, its evidence models, data health, and Stage 3-driven waterfall dispatch.</p></div>
      <div className={`operations-gate ${overview.control.pipeline_paused ? "paused" : "armed"}`}><span className="status-dot" />{overview.control.pipeline_paused ? "Processing paused" : "Worker armed"}<small>{overview.control.reason}</small></div>
    </header>
    <div className="operations-grid operations-grid-primary">
      <article className="ops-card ops-card-wide"><span className="metric-label">Universe progress</span><div className="ops-big-number"><strong>{overview.universe.processed_count.toLocaleString()}</strong><span> / {overview.universe.configured_count.toLocaleString()} processed</span></div><div className="progress-track"><span style={{ width: `${Math.max(0, Math.min(100, overview.universe.progress_percent))}%` }} /></div><div className="ops-inline-stats"><span><strong>{overview.universe.remaining_count.toLocaleString()}</strong> remaining</span><span><strong>{overview.universe.manifest_count.toLocaleString()}</strong> manifested</span><span><strong>{overview.universe.initial_stage1_pool.toLocaleString()}</strong> Stage 1 pool</span></div></article>
      <article className="ops-card"><span className="metric-label">Worker heartbeat</span><strong className="ops-card-value">{humanizeState(overview.worker.state)}</strong><span className="metric-detail">{overview.worker.last_heartbeat ? formatDate(overview.worker.last_heartbeat) : "No heartbeat yet"}</span></article>
      <article className="ops-card"><span className="metric-label">Coverage health</span><strong className="ops-card-value">{overview.data_health.coverage.product_count.toLocaleString()}</strong><span className="metric-detail">products across {overview.data_health.coverage.candidate_count.toLocaleString()} candidates</span><span className="ops-card-note">{overview.data_health.coverage.window_count.toLocaleString()} windows observed</span></article>
      <article className="ops-card"><span className="metric-label">Prime-candidate catalog check</span><strong className="ops-card-value">{catalogSummary}</strong><span className="metric-detail">{catalogDetail}</span><span className="ops-card-note">{catalogHealth?.fetched_at ? `Last scan ${formatDate(catalogHealth.fetched_at)} · ` : ""}Only Stage 3 Prime Candidates are checked against the complete archive projection.</span></article>
    </div>
    <section className="ops-section"><div className="ops-section-heading"><div><p className="eyebrow">Stage prioritization</p><h4>Queue policy and classifications</h4></div><span className="section-note">Stage 1 and 2 use ranking windows; Stage 3 works its Needs More Data queue in rank order.</span></div><div className="stage-ops-grid">{stages.map((stage) => <article className="stage-ops-card" key={stage.stage}><div className="stage-ops-card-heading"><strong>{stageName(stage.stage)}</strong><span>{stage.priority_label}</span></div><div className="stage-ops-inline-note">{stage.priority_candidates.toLocaleString()} currently prioritized · {stage.total_candidates.toLocaleString()} candidate signals retained</div><div className="stage-counts">{["Promising", "Needs More Data", "Inconclusive", "Known Match"].map((status) => <span key={status}><strong>{stage.counts[status] ?? 0}</strong>{monitoringStatusLabel(status)}</span>)}</div><small>Last evaluation: {formatDate(stage.last_evaluated_at)}</small></article>)}</div></section>
    <section className="ops-section"><div className="ops-section-heading"><div><p className="eyebrow">Model registry</p><h4>Algorithms and evidence quality</h4></div><span className="section-note">Metrics are aggregates of persisted evaluations, not new runs.</span></div>{overview.models.length ? <div className="model-registry">{overview.models.map((model) => <article className="model-card" key={`${model.stage}-${model.model_version}`}><div className="model-card-heading"><span className="stage-badge">{stageName(model.stage)}</span><strong>{stageName(model.stage)} model</strong></div><div className="model-stat-grid"><span><small>Evaluations</small><strong>{model.evaluation_count}</strong></span><span><small>Evidence</small><strong>{numberLabel(model.mean_evidence_score, 3)}</strong></span><span><small>Holdout F1</small><strong>{numberLabel(model.mean_holdout_f1, 2)}</strong></span><span><small>Model lift</small><strong>{numberLabel(model.mean_model_lift, 2)}</strong></span></div><p>{Object.entries(model.status_counts).map(([status, count]) => `${status}: ${count}`).join(" · ") || "No classifications recorded"}</p><small>Last evaluated {formatDate(model.last_evaluated_at)}</small></article>)}</div> : <div className="ops-empty">No model evaluations are persisted yet.</div>}</section>
    <div className="operations-grid operations-grid-secondary"><section className="ops-section ops-section-half"><div className="ops-section-heading"><div><p className="eyebrow">Adaptive sampling</p><h4>Replacement-target intelligence</h4></div></div><p className="ops-explanation">{overview.sampling.explanation}</p><div className="sampling-summary"><span><strong>{overview.sampling.cursor.toLocaleString()}</strong> cursor</span><span><strong>{overview.sampling.remaining_count.toLocaleString()}</strong> remaining</span><span><strong>{overview.sampling.adaptive_enabled ? "On" : "Observed only"}</strong> adaptive policy</span></div>{overview.sampling.buckets.length ? <div className="sampling-list">{overview.sampling.buckets.slice(0, 8).map((bucket) => <div className="sampling-row" key={bucket.bucket}><div><strong>{bucket.bucket}</strong><small>{bucket.observed} observed · {bucket.processed} processed</small></div><span>{bucket.promising} promising · {bucket.needs_more_data} needs data · {bucket.known_match} known</span><em>{bucket.recommendation}</em></div>)}</div> : <div className="ops-empty">No selection buckets have been observed yet.</div>}</section><section className="ops-section ops-section-half"><div className="ops-section-heading"><div><p className="eyebrow">Sky-cell outcomes</p><h4>Where to explore next</h4></div></div><p className="ops-explanation">Ten-degree RA/Dec cells summarize observed outcomes. They are guidance for the sampler, not a scientific claim about neighboring stars.</p>{overview.sampling.spatial_clusters.length ? <div className="sampling-list">{overview.sampling.spatial_clusters.slice(0, 8).map((cluster) => <div className="sampling-row" key={`${cluster.ra_min_deg}-${cluster.dec_min_deg}`}><div><strong>RA {cluster.ra_min_deg.toFixed(0)}° · Dec {cluster.dec_min_deg.toFixed(0)}°</strong><small>{cluster.observed} observed · {cluster.promising} promising</small></div><span>{cluster.needs_more_data} needs data · {cluster.inconclusive} inconclusive · {cluster.known_match} known</span><em>{cluster.recommendation}</em></div>)}</div> : <div className="ops-empty">Spatial signals will appear after targets with coordinates are assessed.</div>}</section></div>
    <section className="ops-section"><div className="ops-section-heading"><div><p className="eyebrow">Provider and job health</p><h4>Data plane status</h4></div></div><div className="provider-grid">{overview.data_health.providers.length ? overview.data_health.providers.map((provider) => <article className="provider-card" key={provider.provider}><div><strong>{provider.provider}</strong><span className="health-pill">{provider.failed_runs ? "Needs attention" : "Healthy"}</span></div><p>{provider.runs} runs · {provider.successful_runs} successful · {provider.failed_runs} failed</p><small>{provider.products_downloaded} products downloaded · {provider.products_failed} failed</small><small>Last finished {formatDate(provider.last_finished_at)}</small></article>) : <div className="ops-empty">No provider runs recorded yet.</div>}</div></section>
    <div className="operations-grid operations-grid-secondary operations-grid-ledger"><section className="ops-section ops-section-half"><div className="ops-section-heading"><div><p className="eyebrow">Execution ledger</p><h4>Recent batches</h4></div></div>{overview.data_health.recent_batches.length ? <div className="ledger-list">{overview.data_health.recent_batches.slice(0, 5).map((batch) => <div className="ledger-row" key={batch.batch_id}><div><strong>{stageName(batch.stage)} · {batch.status}</strong><small>{batch.selection_policy} · {formatDate(batch.started_at)}</small></div><span>{batch.evaluated_count} evaluated · {batch.failed_count} failed</span></div>)}</div> : <div className="ops-empty">No processing batches have been recorded.</div>}</section><section className="ops-section ops-section-half"><div className="ops-section-heading"><div><p className="eyebrow">Pipeline event stream</p><h4>Recent events</h4></div></div>{overview.data_health.recent_events.length ? <div className="ledger-list">{overview.data_health.recent_events.slice(0, 5).map((event) => <div className="ledger-row" key={`${event.sequence_number}-${event.event_type}`}><div><strong>{event.event_type.replaceAll("_", " ")}</strong><small>{stageName(event.stage)} · {formatDate(event.occurred_at)}</small></div><span>{event.model_run_id ?? "system event"}</span></div>)}</div> : <div className="ops-empty">No pipeline events have been recorded.</div>}</section></div>
    <footer className="operations-footer"><span className="status-dot muted" /> Last metrics read {formatDate(overview.generated_at)} · Refreshing this view never starts processing.</footer>
  </section>;
}

export default function HomePage() {
  const [activeTab, setActiveTab] = useState<RankingView>("overall");
  const [identifiedOpen, setIdentifiedOpen] = useState(false);
  const [identifiedRefreshKey, setIdentifiedRefreshKey] = useState(0);
  const [opsAccessOpen, setOpsAccessOpen] = useState(false);
  const [opsPassword, setOpsPassword] = useState("");
  const [opsPasswordError, setOpsPasswordError] = useState(false);
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
  const [replayStates, setReplayStates] = useState<Record<RankingView, ReplayState>>(createReplayStates);
  const [rankingControls, setRankingControls] = useState<Record<RankingView, RankingControls>>(createRankingControls);
  const [openColumnControl, setOpenColumnControl] = useState<"status" | "assessed" | null>(null);
  const [rankingPage, setRankingPage] = useState(1);
  const [pipelineStatusOpen, setPipelineStatusOpen] = useState(false);
  const dashboardRequestRef = useRef(0);

  const closeCandidate = useCallback(() => {
    setSelectedId(null);
    setCandidate(null);
    setCurve(null);
    setHistory(null);
  }, []);

  const updateReplay = useCallback((view: RankingView, patch: Partial<ReplayState>) => {
    setReplayStates((current) => ({ ...current, [view]: { ...current[view], ...patch } }));
  }, []);

  const loadDashboard = useCallback(async () => {
    const requestId = dashboardRequestRef.current + 1;
    dashboardRequestRef.current = requestId;
    setLoading(true);
    setError(null);
    try {
      const controls = rankingControls[activeTab];
      const query = new URLSearchParams({ view: activeTab, limit: "100", page: String(rankingPage) });
      if (activeTab === "overall" && controls.stage) query.set("stage_filter", controls.stage);
      if (activeTab !== "overall" && controls.status) query.set("status_filter", controls.status);
      if (controls.sort !== "rank") query.set("sort", controls.sort);
      const [nextStatus, nextRanking] = await Promise.all([fetchJson<DashboardStatus>("/api/status"), fetchJson<RankingResponse>(`/api/rankings?${query.toString()}`)]);
      if (requestId !== dashboardRequestRef.current) return;
      setStatus(nextStatus);
      setRanking(nextRanking);
    } catch (cause) {
      if (requestId !== dashboardRequestRef.current) return;
      setError(cause instanceof Error ? cause.message : "The observatory could not load published data.");
    } finally {
      if (requestId === dashboardRequestRef.current) setLoading(false);
    }
  }, [activeTab, rankingControls, rankingPage]);

  useEffect(() => { void loadDashboard(); }, [loadDashboard]);

  const loadReplaySnapshot = useCallback(async (view: RankingView, cursor: number, page = rankingPage) => {
    updateReplay(view, { cursor, loading: true, error: null });
    try {
      const snapshot = await fetchJson<RankingResponse>(`/api/replay/${view}/snapshot?sequence=${cursor}&limit=100&page=${page}`);
      updateReplay(view, { snapshot, loading: false });
    } catch (cause) {
      updateReplay(view, { loading: false, error: cause instanceof Error ? cause.message : "Replay snapshot unavailable." });
    }
  }, [rankingPage, updateReplay]);

  const startReplay = useCallback(async (view: RankingView) => {
    closeCandidate();
    updateReplay(view, { active: true, playing: false, cursor: 0, snapshot: null, loading: true, error: null });
    try {
      const meta = await fetchJson<ReplayMeta>(`/api/replay/${view}/meta`);
      updateReplay(view, { meta, maxSequence: meta.last_sequence });
      await loadReplaySnapshot(view, 0);
    } catch (cause) {
      updateReplay(view, { loading: false, error: cause instanceof Error ? cause.message : "Replay metadata unavailable." });
    }
  }, [closeCandidate, loadReplaySnapshot, updateReplay]);

  const exitReplay = useCallback((view: RankingView) => {
    updateReplay(view, { active: false, playing: false });
  }, [updateReplay]);

  const openCandidate = useCallback(async (candidateId: string) => {
    setSelectedId(candidateId);
    setModalLoading(true);
    setError(null);
    try {
      const [detail, nextHistory] = await Promise.all([
        fetchJson<{ candidate: CandidateDetail }>(`/api/candidates/${candidateId}`),
        fetchJson<HistoryResponse>(`/api/candidates/${candidateId}/history`),
      ]);
      const nextCurve = await fetchJson<LightCurveResponse>(`/api/candidates/${candidateId}/light-curve`).catch(() => ({ points: [] }));
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
    if (!identifiedOpen) exitReplay(activeTab);
    try {
      await fetchJson("/api/refresh", { method: "POST" });
      if (identifiedOpen) setIdentifiedRefreshKey((current) => current + 1);
      else await loadDashboard();
    } finally {
      setRefreshing(false);
    }
  }, [activeTab, closeCandidate, exitReplay, identifiedOpen, loadDashboard]);

  const activeReplay = replayStates[activeTab];
  const activeControls = rankingControls[activeTab];
  const updateRankingControls = (patch: Partial<RankingControls>) => {
    setRankingControls((current) => ({ ...current, [activeTab]: { ...current[activeTab], ...patch } }));
    setRankingPage(1);
    setOpenColumnControl(null);
  };
  useEffect(() => {
    if (!activeReplay.active || !activeReplay.playing) return;
    if (activeReplay.cursor >= activeReplay.maxSequence) {
      updateReplay(activeTab, { playing: false });
      return;
    }
    const timer = window.setTimeout(() => {
      void loadReplaySnapshot(activeTab, activeReplay.cursor + 1, rankingPage);
    }, Math.max(140, 900 / activeReplay.speed));
    return () => window.clearTimeout(timer);
  }, [activeReplay.active, activeReplay.cursor, activeReplay.maxSequence, activeReplay.playing, activeReplay.speed, activeTab, loadReplaySnapshot, rankingPage, updateReplay]);

  const displayRanking = activeReplay.active ? activeReplay.snapshot : ranking;
  const rows = displayRanking?.rows ?? [];
  const removals = activeReplay.active ? displayRanking?.removed ?? [] : [];
  const dataState = status?.data_version ? "Published" : "Awaiting data";
  const pipelineState = status?.data_version ? "Published" : status?.worker?.state === "idle" ? "Standby" : "Starting";
  const copy = stageCopy(activeTab);
  const totalCandidates = displayRanking?.total_ranked_count ?? rows.length;
  const processedCount = activeReplay.active ? displayRanking?.processed_target_count ?? 0 : status?.processed_target_count ?? ranking?.processed_target_count ?? 0;
  const targetCount = status?.total_target_count ?? ranking?.total_target_count ?? 126000;
  const totalStars = displayRanking?.total_star_count ?? new Set(rows.map((row) => row.target_id)).size;
  const panelTitle = loading && !ranking && !activeReplay.active ? "Loading published state" : totalCandidates ? `Total candidates ${totalCandidates.toLocaleString()} · Total stars ${totalStars.toLocaleString()}` : "No candidates published yet";
  const remainingCount = Math.max(0, targetCount - processedCount);
  const activeCandidateCount = status?.active_candidate_count ?? rows.length;
  const footerState = status?.last_ingest_at ? `Last published: ${formatDate(status.last_ingest_at)}` : "Awaiting first publication";
  const nextUpdateAt = status?.worker.next_update_at ?? (status?.last_ingest_at ? new Date(new Date(status.last_ingest_at).getTime() + 5 * 60 * 1000).toISOString() : null);
  const pageCount = displayRanking?.page_count ?? 0;
  const displayCap = displayRanking?.display_cap ?? 500;
  const statusControlActive = activeTab === "overall" ? activeControls.stage !== null : activeControls.status !== null;
  const statusControlLabel = activeTab === "overall" ? (activeControls.stage ? stageName(activeControls.stage) : "Status") : (activeControls.status ? displayStatus(activeControls.status, activeTab) : "Status");
  const statusControlOptions = activeTab === "overall"
    ? assessmentStages.map((stage) => ({ value: stage, label: stageName(stage) }))
    : statusFilterOptions.map((statusOption) => ({ value: statusOption, label: displayStatus(statusOption, activeTab) }));
  const assessmentSortLabel = activeControls.sort === "last_assessed_desc" ? "Most recently assessed" : activeControls.sort === "last_assessed_asc" ? "Oldest assessed first" : "Last assessed";
  const selectRankingPage = (page: number) => {
    setRankingPage(page);
    if (activeReplay.active) void loadReplaySnapshot(activeTab, activeReplay.cursor, page);
  };

  const modal = useMemo(() => {
    if (!selectedId) return null;
    if (modalLoading || !candidate) return <div className="modal-backdrop" role="presentation"><section className="candidate-modal modal-loading" role="dialog" aria-modal="true" aria-label="Loading candidate details"><span className="status-dot" /><p>Loading candidate evidence…</p></section></div>;
    return <CandidateModal candidate={candidate} curve={curve} history={history} replaySequence={activeReplay.active ? activeReplay.cursor : null} onClose={closeCandidate} />;
  }, [activeReplay.active, activeReplay.cursor, candidate, closeCandidate, curve, history, modalLoading, selectedId]);

  return <main className="observatory-shell">
    <div className="ambient-glow ambient-glow-one" aria-hidden="true" /><div className="ambient-glow ambient-glow-two" aria-hidden="true" />
    <header className="topbar"><div className="brand-lockup"><div className="brand-mark" aria-hidden="true">✦</div><div><p className="eyebrow">TransitScope // live assessment lab</p><h1>Explore potential space discoveries</h1></div></div><div className="topbar-actions"><span className="data-state"><span className={`status-dot ${status?.data_version ? "" : "muted"}`} /> {dataState}</span><button className="button button-quiet" type="button" onClick={() => void handleRefresh()} disabled={refreshing || loading}>↻ <span>{refreshing ? "Checking" : "Refresh"}</span></button><button className="button button-quiet tool-button" type="button" aria-label="Open ML Ops dashboard" title="ML Ops dashboard" onClick={() => { setOpsPassword(""); setOpsPasswordError(false); setOpsAccessOpen(true); }}>🔧</button></div></header>
    <nav className="tab-row" aria-label="Assessment rankings">{tabs.map((tab) => <button className={`tab-button ${!identifiedOpen && activeTab === tab.id ? "active" : ""}`} key={tab.id} onClick={() => { setIdentifiedOpen(false); setPipelineStatusOpen(false); setOpenColumnControl(null); setRankingPage(1); setActiveTab(tab.id); }} type="button">{tab.label}</button>)}<button className={`tab-button ${identifiedOpen ? "active" : ""}`} type="button" onClick={() => { closeCandidate(); exitReplay(activeTab); setPipelineStatusOpen(false); setOpenColumnControl(null); setIdentifiedOpen(true); }}>Identified Known Matches</button></nav>
    {!identifiedOpen ? <>
    <section className="stage-heading"><div><div className="stage-kicker"><span className="stage-number">{copy.number}</span> {copy.kicker}</div><h3>{copy.title}</h3><p>{copy.description}</p></div>{activeReplay.active ? <div className="replay-dock-wrap"><div className="replay-dock" aria-label={`${copy.title} replay controls`} title={activeReplay.loading ? "Loading saved replay snapshot" : `Saved replay · ${activeReplay.snapshot?.occurred_at ? formatDate(activeReplay.snapshot.occurred_at) : "Beginning of assessment"}`}><button aria-label={activeReplay.playing ? "Pause replay" : "Play replay"} className="button button-quiet replay-play" type="button" onClick={() => updateReplay(activeTab, { playing: !activeReplay.playing })} disabled={activeReplay.maxSequence === 0}>{activeReplay.playing ? "Ⅱ" : "▶"}</button><label className="replay-speed"> <span>Speed</span><select aria-label="Replay speed" value={activeReplay.speed} onChange={(event) => updateReplay(activeTab, { speed: Number(event.target.value) })}><option value={0.5}>0.5×</option><option value={1}>1×</option><option value={2}>2×</option><option value={4}>4×</option></select></label><input aria-label="Replay position" className="replay-range" type="range" min={0} max={activeReplay.maxSequence} value={activeReplay.cursor} onChange={(event) => void loadReplaySnapshot(activeTab, Number(event.target.value))} /><button className="button button-replay-active replay-exit" type="button" onClick={() => exitReplay(activeTab)}>Exit replay</button></div>{activeReplay.error && <p className="replay-error">{activeReplay.error}</p>}</div> : <div className="stage-actions"><button className="button button-outline" type="button" onClick={() => void startReplay(activeTab)}>Replay</button>{activeTab === "overall" && <button className="button button-outline" type="button" onClick={() => setPipelineStatusOpen((open) => !open)} aria-expanded={pipelineStatusOpen}>{pipelineStatusOpen ? "Hide pipeline status" : "Pipeline status"}</button>}</div>}</section>
    {activeTab === "overall" && pipelineStatusOpen && !activeReplay.active && <section className="metrics-grid" aria-label="Processing summary"><article className="metric-card"><span className="metric-label">Target universe</span><strong>{remainingCount.toLocaleString()} / {targetCount.toLocaleString()}</strong><span className="metric-detail">stars left in target universe exploration</span></article><article className="metric-card"><span className="metric-label">Processed</span><strong>{processedCount.toLocaleString()}</strong><span className="metric-detail">targets with published evaluations</span></article><article className="metric-card"><span className="metric-label">Active candidates</span><strong>{activeCandidateCount.toLocaleString()}</strong><span className="metric-detail">currently visible in this assessment</span></article><article className="metric-card metric-card-status"><span className="metric-label">Pipeline status</span><strong><span className={`status-dot ${status?.data_version ? "" : "muted"}`} /> {pipelineState}</strong><span className="metric-detail">{footerState}</span><span className="metric-detail">Next update: {nextUpdateAt ? formatDate(nextUpdateAt) : "Calculating…"}</span></article></section>}
    <section className="ranking-panel" aria-labelledby="ranking-title"><div className="panel-header"><div><p className="eyebrow">Top candidates · {activeReplay.active ? "saved replay" : "live ranking"}</p><h3 id="ranking-title">{panelTitle}</h3></div><span className="panel-meta">Top {displayCap.toLocaleString()} displayed</span></div>{error ? <div className="error-state" role="alert"><span className="status-dot muted" /><h4>Published state unavailable</h4><p>{error}</p><button className="button button-outline" type="button" onClick={() => void loadDashboard()}>Try again</button></div> : activeReplay.loading && !activeReplay.snapshot ? <div className="empty-state" role="status"><div className="loading-ring" aria-hidden="true" /><h4>Reading saved replay state.</h4><p>The replay is reconstructing this view from persisted ranking snapshots. No model is run during replay.</p></div> : loading && !ranking && !activeReplay.active ? <div className="empty-state" role="status"><div className="loading-ring" aria-hidden="true" /><h4>Reading the latest published state.</h4><p>The API is checking the worker’s durable publication boundary. No model is run during a dashboard load.</p></div> : displayRanking ? <div className="table-wrap"><div className="ranking-table ranking-table-header"><span>Rank</span><span>Candidate</span><RankingColumnControl label={statusControlLabel} active={statusControlActive} open={openColumnControl === "status"} ariaLabel={activeTab === "overall" ? "Filter overall ranking by stage" : "Filter ranking by status"} options={statusControlOptions} onToggle={() => setOpenColumnControl((current) => current === "status" ? null : "status")} onSelect={(value) => activeTab === "overall" ? updateRankingControls({ stage: value as AssessmentStage }) : updateRankingControls({ status: value as CandidateStatus })} onClear={() => activeTab === "overall" ? updateRankingControls({ stage: null }) : updateRankingControls({ status: null })} /><span>Current activity</span><span>Movement</span><RankingColumnControl label={assessmentSortLabel} active={activeControls.sort !== "rank"} open={openColumnControl === "assessed"} ariaLabel="Sort ranking by assessment time" options={[{ value: "last_assessed_desc", label: "Most recently assessed" }, { value: "last_assessed_asc", label: "Oldest assessed first" }]} onToggle={() => setOpenColumnControl((current) => current === "assessed" ? null : "assessed")} onSelect={(value) => updateRankingControls({ sort: value as AssessmentSort })} onClear={() => updateRankingControls({ sort: "rank" })} /></div><div className="ranking-rows">{removals.map((removal) => <div className="ranking-table ranking-table-row replay-removal-row" key={`removed-${displayRanking?.sequence_number ?? 0}-${removal.candidate_id}`} role="status" aria-label={`${candidateDisplayLabel(removal.target_id, removal.candidate_id, undefined, removal.signal_index)} removed as already known`}><span className="rank-value">{removal.rank ?? "—"}</span><span className="candidate-cell"><strong>{candidateDisplayLabel(removal.target_id, removal.candidate_id, undefined, removal.signal_index)}</strong></span><span><span className="stage-status-stage">{stageName(removal.stage)}</span>{" "}<span className="status-pill status-known-match">Already Known</span><small className="score-label">{removal.known_match_id ?? "Catalog match"}</small></span><span className="activity-cell"><span className="activity-pulse removal-pulse" />{removal.activity}</span><span className="movement movement-removed">→ Removed</span><span className="last-update">{formatDate(removal.occurred_at)}</span></div>)}{rows.map((row) => { const resolutionLabel = signalResolutionLabel(row.signal_resolution); const rowStatus = displayStatus(row.status, row.stage); return <button className="ranking-table ranking-table-row" key={row.candidate_id} type="button" onClick={() => void openCandidate(row.candidate_id)} aria-label={`Open details for ${candidateDisplayLabel(row.target_id, row.candidate_id, undefined, row.signal_index)}`}><span className="rank-value">{row.rank}</span><span className="candidate-cell"><strong>{candidateDisplayLabel(row.target_id, row.candidate_id, undefined, row.signal_index)}</strong>{resolutionLabel && <small className="signal-resolution-note">{resolutionLabel}</small>}</span><span><span className="stage-status-stage">{stageName(row.stage)}</span>{" "}<span className={`status-pill status-${statusClass(rowStatus)}`}>{rowStatus}</span></span><span className="activity-cell"><span className="activity-pulse" />{row.activity}</span><span className={`movement movement-${row.movement}`}>{movementLabel(row)}</span><span className="last-update">{formatDate(row.last_evaluated_at)}</span></button>; })}{!rows.length && !removals.length && <div className="filtered-empty-state" role="status"><span className="status-dot muted" aria-hidden="true" /><strong>No candidates match this selection.</strong><span>Try another status or clear the filter to return to the full ranking.</span></div>}</div>{pageCount > 1 && <nav className="ranking-pages" aria-label="Ranking pages"><span>Showing page {displayRanking?.page ?? rankingPage} of {pageCount}</span><div>{Array.from({ length: pageCount }, (_, index) => index + 1).map((page) => <button className={`page-button ${(displayRanking?.page ?? rankingPage) === page ? "active" : ""}`} key={page} type="button" onClick={() => selectRankingPage(page)}>{page}</button>)}</div></nav>}</div> : <div className="empty-state" role="status"><div className="empty-orbit" aria-hidden="true"><span /></div><h4>{activeReplay.active && activeReplay.cursor === 0 ? "The replay begins before the first candidate." : "The observatory is waiting for its first observation."}</h4><p>{activeReplay.active ? "Move the replay forward to watch candidates enter, change rank, advance, or leave the active pool." : "The ranking will populate when a MAST/TESS evaluation is published. Empty is a real state here—not a hidden placeholder result."}</p><div className="empty-state-meta"><span className="status-dot muted" /> No current activity</div></div>}</section>
    </> : <IdentifiedPanel onOpenCandidate={(candidateId) => void openCandidate(candidateId)} refreshKey={identifiedRefreshKey} />}
    <footer className="footer-note"><span className="footer-pulse" aria-hidden="true" /><span>Data source: MAST / TESS · {footerState} · Evidence is provisional</span></footer>
    {modal}
    {opsAccessOpen && <div className="modal-backdrop" role="presentation" onClick={() => setOpsAccessOpen(false)}><section className="candidate-modal access-modal" role="dialog" aria-modal="true" aria-labelledby="ops-access-title" onClick={(event) => event.stopPropagation()}><header className="modal-header"><div><p className="eyebrow accent">Portfolio monitoring view</p><h3 id="ops-access-title">Open ML Ops</h3><p className="modal-subtitle">Enter the monitor password to open the operations dashboard.</p></div><button className="modal-close" type="button" aria-label="Close ML Ops access dialog" onClick={() => setOpsAccessOpen(false)}>×</button></header><form className="access-form" onSubmit={(event) => { event.preventDefault(); if (opsPassword === "monitor") { window.location.assign("/ops"); return; } setOpsPasswordError(true); }}><label className="access-label" htmlFor="ops-password">Password</label><input className="access-input" id="ops-password" type="password" autoFocus value={opsPassword} onChange={(event) => { setOpsPassword(event.target.value); setOpsPasswordError(false); }} /><p className={`access-error ${opsPasswordError ? "" : "access-error-hidden"}`} role={opsPasswordError ? "alert" : undefined}>Incorrect password.</p><div className="access-actions"><button className="button button-outline" type="button" onClick={() => setOpsAccessOpen(false)}>Cancel</button><button className="button button-quiet" type="submit">Open dashboard</button></div></form></section></div>}
  </main>;
}
