"use client";

import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";

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
  processed_history?: Array<{ occurred_at: string | null; processed_target_count: number }>;
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
  served_from_cache?: boolean;
  read_state?: "publication_busy";
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

type MonitoringCycleStage = {
  selected?: number;
  evaluated?: number;
  attempted?: number;
  failed?: number;
  released?: number;
  promising?: number;
  needs_more_data?: number;
  inconclusive?: number;
  known_match?: number;
  errors?: number;
  unchanged?: number;
  handoffs?: number;
  timing_seconds?: number;
  needs_more_data_seconds?: number;
  dispatch_seconds?: number;
};

type MonitoringCycleEvent = {
  candidate_id?: string;
  stage?: string;
  status?: string;
  kind?: string;
  source_candidate_id?: string;
};

type MonitoringRankedSample = {
  candidate_id?: string;
  status?: string;
  rank?: number;
  status_since?: string | null;
};

type MonitoringTop100Point = {
  finished_at: string | null;
  sequence_number?: number | null;
  sample_size: number;
  counts: Record<string, number>;
  percentages: Record<string, number>;
  average_time_seconds: Record<string, number | null>;
};

type MonitoringTop100View = {
  top_n: number;
  status_order: string[];
  current: MonitoringTop100Point;
  rolling_average: {
    sample_count: number;
    percentages: Record<string, number>;
    average_time_seconds: Record<string, number | null>;
  };
  series: MonitoringTop100Point[];
  note: string;
};

type MonitoringQueueEvent = {
  candidate_id?: string;
  stage?: string;
  kind?: "intake" | "handoff" | "reassessment" | string;
  queue_status?: string;
  reason?: string;
  source_candidate_id?: string;
};

type MonitoringStage2Reassessment = {
  threshold?: number;
  priority?: string;
  initial_queue_count?: number;
  last_queue_count?: number;
  remaining_queue_count?: number;
  evaluated_count?: number;
  unchanged_candidate_ids?: string[];
  published_candidate_ids?: string[];
  dispatch_batches?: string[][];
  waterfall_gate_open?: boolean;
  gate_bootstrap_evaluated_count?: number;
  waterfall_gate?: {
    open?: boolean;
    enforced?: boolean;
    policy?: string;
    strict_gate_open?: boolean;
    threshold?: number;
    top_count?: number;
    promising_count?: number;
    non_promising_count?: number;
    reason?: string;
    message?: string;
    paused_stages?: string[];
  };
};

type MonitoringStage2Intake = {
  status?: string;
  priority?: string;
  requested_count?: number;
  evaluated_count?: number;
  candidate_ids?: string[];
  message?: string;
};

type MonitoringCycle = {
  cycle_id: string;
  started_at: string;
  finished_at: string;
  status: string;
  duration_seconds: number;
  stages: Record<string, MonitoringCycleStage>;
  queue: Record<string, Record<string, number>>;
  ranked_samples?: Record<string, MonitoringRankedSample[]>;
  queue_events?: MonitoringQueueEvent[];
  events?: MonitoringCycleEvent[];
  stage2_intake?: MonitoringStage2Intake;
  stage2_reassessment?: MonitoringStage2Reassessment;
};

type MonitoringCycleActivity = {
  window_minutes: number;
  expected_runs: number;
  recent_run_count: number;
  default_start_index: number;
  run_count: number;
  runs: MonitoringCycle[];
  throughput: Record<string, {
    attempted: number;
    evaluated: number;
    promising: number;
    needs_more_data: number;
    inconclusive: number;
    known_match: number;
    errors: number;
    handoffs: number;
    per_hour: number;
    per_minute: number;
  }>;
  timings: Record<string, {
    average_cycle_seconds: number | null;
    needs_more_data_seconds: number;
    average_needs_more_data_seconds: number | null;
    average_dispatch_seconds: number | null;
    handoffs: number;
  }>;
  queue: {
    current: Record<string, Record<string, number>>;
    series: Array<{ finished_at: string; stage1: number; stage2: number; stage3: number }>;
  };
  top100_status: Record<string, MonitoringTop100View>;
  note: string;
};

type ManualMigration = {
  request_id: string;
  from_stage: AssessmentStage | "universe";
  to_stage: AssessmentStage;
  requested_count: number;
  status: "pending" | "processing" | "completed" | "failed" | string;
  created_at: string;
  updated_at: string;
  requested_by?: string;
  selected_count?: number;
  completed_count?: number;
  candidate_ids?: string[];
  message?: string;
  error?: string | null;
};

type MonitoringOverview = {
  generated_at: string;
  data_version: number | null;
  processed_history: Array<{ occurred_at: string | null; processed_target_count: number }>;
  control: {
    pipeline_paused: boolean;
    autostart_enabled: boolean;
    reason: string;
    operator_control?: boolean;
    deployment_gate?: boolean;
    updated_at?: string | null;
  };
  manual_migrations: ManualMigration[];
  cycle_activity: MonitoringCycleActivity;
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

function durationLabel(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  if (value < 1) return `${Math.round(value * 1000)} ms`;
  if (value < 60) return `${value.toFixed(1)} s`;
  return `${Math.floor(value / 60)}m ${Math.round(value % 60)}s`;
}

function countdownLabel(value: string | null | undefined, nowMs: number): string {
  if (!value) return "Calculating…";
  const remainingSeconds = Math.ceil((new Date(value).getTime() - nowMs) / 1000);
  if (!Number.isFinite(remainingSeconds)) return "Calculating…";
  if (remainingSeconds <= 0) return "Due now";
  if (remainingSeconds < 60) return `in ${remainingSeconds}s`;
  const minutes = Math.floor(remainingSeconds / 60);
  const seconds = remainingSeconds % 60;
  return `in ${minutes}m ${seconds.toString().padStart(2, "0")}s`;
}

function identifiedPerformanceStats(performance: IdentifiedPerformance): string {
  const stats: string[] = [];
  if (typeof performance.coverage_days === "number") stats.push(`${numberLabel(performance.coverage_days)} d coverage`);
  if (typeof performance.observed_event_count === "number") stats.push(`${numberLabel(performance.observed_event_count, 0)} events`);
  if (typeof performance.holdout_f1 === "number") stats.push(`F1 ${numberLabel(performance.holdout_f1, 2)}`);
  if (typeof performance.model_lift === "number") stats.push(`lift ${performance.model_lift >= 0 ? "+" : ""}${numberLabel(performance.model_lift, 2)}`);
  return stats.join(" · ") || "No secondary stats";
}

function LightCurveChart({ points, emptyMessage, signalFeatures, transitProgress, transitOverlay }: { points: LightCurveResponse["points"]; emptyMessage: string; signalFeatures?: Record<string, unknown>; transitProgress?: number | null; transitOverlay?: ReactNode }) {
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
  const playbackX = typeof transitProgress === "number"
    ? padding + Math.max(0, Math.min(1, transitProgress)) * (width - padding * 2)
    : null;
  return <div className="chart-wrap"><div className="chart-visual-stage"><svg aria-label="Normalized light curve with selected signal windows" role="img" viewBox={`0 0 ${width} ${height}`}><line className="chart-baseline" x1={padding} x2={width - padding} y1={height / 2} y2={height / 2} />{markerTimes.map((marker) => { const x = padding + ((marker - minTime) / (maxTime - minTime || 1)) * (width - padding * 2); return <line className="chart-transit-marker" key={marker} x1={x} x2={x} y1={padding} y2={height - padding} />; })}{paths.map((path, index) => <path className="chart-line" d={path} key={index} />)}{playbackX !== null && <line className="chart-playback-cursor" x1={playbackX} x2={playbackX} y1={padding} y2={height - padding} />}</svg>{transitOverlay}</div><div className="chart-axis"><span>{minTime.toFixed(2)} BJD−2457000</span><span className="chart-gap-note">{markerTimes.length ? "Vertical markers show this signal's predicted transits" : "Observation gaps are not connected"}</span><span>{maxTime.toFixed(2)}</span></div></div>;
}

function TransitSimulation({ points, signalFeatures, onFrameChange }: { points: LightCurveResponse["points"]; signalFeatures: Record<string, unknown>; onFrameChange?: (index: number, total: number) => void }) {
  const [frameIndex, setFrameIndex] = useState(0);
  const frames = useMemo(() => {
    const maxFrames = 72;
    const sampledPoints = points.length > maxFrames
      ? points.filter((_, index) => index % Math.ceil(points.length / maxFrames) === 0).slice(0, maxFrames)
      : points;
    if (sampledPoints.length) {
      const values = sampledPoints.map((point) => point.normalized_flux);
      const minFlux = Math.min(...values);
      const maxFlux = Math.max(...values);
      const range = maxFlux - minFlux || 1;
      return sampledPoints.map((point) => ({
        time: point.time,
        brightness: 0.58 + ((point.normalized_flux - minFlux) / range) * 0.42,
      }));
    }

    const depth = typeof signalFeatures.depth === "number" && Number.isFinite(signalFeatures.depth)
      ? Math.min(0.28, Math.max(0.025, signalFeatures.depth * 2))
      : 0.08;
    return Array.from({ length: 48 }, (_, index) => {
      const phase = index / 48;
      const distanceFromTransit = Math.abs(((phase + 0.5) % 1) - 0.5);
      const transitShape = distanceFromTransit < 0.075 ? 1 - distanceFromTransit / 0.075 : 0;
      return { time: index, brightness: 1 - transitShape * depth };
    });
  }, [points, signalFeatures]);
  const frame = frames[Math.min(frameIndex, Math.max(0, frames.length - 1))] ?? { time: 0, brightness: 1 };
  const starX = 110;
  const starY = 66;
  const starRadius = 28;
  const orbitAngle = (frameIndex / Math.max(1, frames.length)) * Math.PI * 2 - Math.PI / 2;
  const objectX = starX + 68 * Math.cos(orbitAngle);
  const objectY = starY + 33 * Math.sin(orbitAngle);
  const objectInFront = Math.abs(objectX - starX) < starRadius * 1.25 && Math.abs(objectY - starY) < starRadius * 0.62;
  const displayBrightness = Math.max(0, Math.min(100, frame.brightness * 100));

  useEffect(() => {
    setFrameIndex(0);
  }, [points, signalFeatures]);

  useEffect(() => {
    if (frames.length < 2) return undefined;
    const timer = window.setInterval(() => {
      setFrameIndex((current) => (current + 1) % frames.length);
    }, 180);
    return () => window.clearInterval(timer);
  }, [frames.length]);

  useEffect(() => {
    onFrameChange?.(frameIndex, frames.length);
  }, [frameIndex, frames.length, onFrameChange]);

  const planet = <circle className="transit-simulation-object" cx={objectX} cy={objectY} r="7" />;
  const star = <><circle className="transit-simulation-glow" cx={starX} cy={starY} r={starRadius + 11} opacity={0.14 + frame.brightness * 0.22} /><circle className="transit-simulation-star" cx={starX} cy={starY} r={starRadius} opacity={0.38 + frame.brightness * 0.62} /><circle className="transit-simulation-star-core" cx={starX - 10} cy={starY - 10} r="9" opacity={0.2 + frame.brightness * 0.3} /></>;
  return <div className="transit-overlay" aria-label="Illustrative transit replay"><svg className="transit-overlay-scene" role="img" aria-label="Animated star and orbiting object synchronized to the candidate light curve" viewBox="0 0 220 130"><defs><radialGradient id="transit-star-gradient"><stop offset="0%" stopColor="#fff6c9" /><stop offset="62%" stopColor="#ffd978" /><stop offset="100%" stopColor="#e99c43" /></radialGradient></defs><ellipse className="transit-simulation-orbit" cx={starX} cy={starY} rx="68" ry="33" />{!objectInFront && planet}{star}{objectInFront && planet}</svg><div className="transit-overlay-readout"><span>Frame {frameIndex + 1} / {frames.length}</span><strong>{displayBrightness.toFixed(1)}% brightness</strong></div></div>;
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="detail-metric"><span>{label}</span><strong>{value}</strong></div>;
}

function CandidateModal({ candidate, curve, history, replaySequence, onClose }: { candidate: CandidateDetail; curve: LightCurveResponse | null; history: HistoryResponse | null; replaySequence: number | null; onClose: () => void }) {
  const closeRef = useRef<HTMLButtonElement>(null);
  const [transitOpen, setTransitOpen] = useState(false);
  const [transitProgress, setTransitProgress] = useState<number | null>(null);
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
  const summaryNote = candidate.status === "Promising" ? null : candidate.ranking_rationale ?? candidate.activity;
  const handleTransitFrameChange = useCallback((index: number, total: number) => {
    setTransitProgress(total > 1 ? index / (total - 1) : 0);
  }, []);
  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose(); }}>
    <section className="candidate-modal" role="dialog" aria-modal="true" aria-labelledby="candidate-modal-title">
      <header className="modal-header"><div><p className="eyebrow accent">Candidate investigation · {stageName(candidate.stage)}</p><h3 id="candidate-modal-title">{candidate.target_id}{signal ? ` - ${signal}` : ""}</h3><p className="modal-subtitle">{candidate.target?.source_product_family ?? "MAST / TESS observation"}</p></div><button ref={closeRef} className="modal-close" type="button" onClick={onClose} aria-label="Close candidate details">×</button></header>
      <div className="modal-summary"><span className="stage-status-stage">{stageName(candidate.stage)}</span>{" "}<span className={`status-pill status-${statusClass(displayStatus(candidate.status, candidate.stage))}`}>{displayStatus(candidate.status, candidate.stage)}</span>{signalResolutionLabel(candidate.signal_resolution) && <span className="evidence-chip">{signalResolutionLabel(candidate.signal_resolution)}</span>}{summaryNote && <span className="modal-summary-note">{summaryNote}</span>}{replaySequence !== null && <span className="replay-chip">Replay view</span>}</div>
      <div className="modal-content-grid">
        <div className="modal-main-column">
          <div className="modal-section"><div className="section-heading"><div><p className="eyebrow">Normalized observation</p><h4>Light curve</h4></div><div className="chart-actions"><span className="section-note">{curve?.points.length ?? 0} plotted points</span><button className={`transit-toggle ${transitOpen ? "active" : ""}`} type="button" aria-pressed={transitOpen} aria-label={transitOpen ? "Hide transit replay" : "See the curve with transit replay"} onClick={() => { setTransitOpen((current) => !current); setTransitProgress(transitOpen ? null : 0); }}><span className="planet-toggle-glyph" aria-hidden="true"><i /></span>See the curve</button></div></div><LightCurveChart points={curve?.points ?? []} signalFeatures={features} emptyMessage={emptyCurveMessage} transitProgress={transitOpen ? transitProgress : null} transitOverlay={transitOpen ? <TransitSimulation points={curve?.points ?? []} signalFeatures={features} onFrameChange={handleTransitFrameChange} /> : null} /></div>
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

function monitoringDotClass(status: string | undefined): string {
  if (status === "Promising") return "promising";
  if (status === "Needs More Data") return "needs-more-data";
  if (status === "Inconclusive") return "inconclusive";
  if (status === "Known Match") return "known-match";
  if (status === "error") return "error";
  return "neutral";
}

function MonitoringTop100StatusPanel({ activity }: { activity: MonitoringCycleActivity }) {
  const views = ["overall", ...assessmentStages];
  const fallbackStatuses = ["Promising", "Needs More Data", "Inconclusive", "Known Match", "Duplicate Signal"];

  const chartPath = (series: MonitoringTop100Point[], status: string, width: number, height: number): string => {
    if (!series.length) return "";
    const padding = 10;
    const maxIndex = Math.max(1, series.length - 1);
    return series.map((point, index) => {
      const x = padding + (index / maxIndex) * (width - padding * 2);
      const percentage = Math.max(0, Math.min(100, point.percentages?.[status] ?? 0));
      const y = height - padding - (percentage / 100) * (height - padding * 2);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    }).join(" ");
  };
  const dwellPath = (series: MonitoringTop100Point[], status: string, width: number, height: number): string => {
    if (!series.length) return "";
    const padding = 10;
    const maxValue = Math.max(1, ...series.map((point) => point.average_time_seconds?.[status] ?? 0));
    const maxIndex = Math.max(1, series.length - 1);
    return series.map((point, index) => {
      const x = padding + (index / maxIndex) * (width - padding * 2);
      const seconds = point.average_time_seconds?.[status] ?? 0;
      const y = height - padding - (seconds / maxValue) * (height - padding * 2);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    }).join(" ");
  };

  return <section className="ops-section top100-status-section" aria-labelledby="top100-status-title">
    <div className="ops-section-heading"><div><p className="eyebrow">Leaderboard composition</p><h4 id="top100-status-title">Top-100 status balance and dwell</h4></div><span className="section-note">Each point is one completed worker cycle · rolling average uses the latest hour</span></div>
    <div className="top100-metrics-grid">
      {views.map((view) => {
        const metric = activity.top100_status?.[view];
        if (!metric) return <article className="top100-metric-card" key={view}><div className="ops-empty">No top-100 telemetry for {view === "overall" ? "Overall" : stageName(view)} yet.</div></article>;
        const current = metric.current ?? { sample_size: 0, counts: {}, percentages: {}, average_time_seconds: {} };
        const rolling = metric.rolling_average ?? { sample_count: 0, percentages: {}, average_time_seconds: {} };
        const statuses = metric.status_order?.length ? metric.status_order : fallbackStatuses;
        const series = metric.series ?? [];
        const chartWidth = 520;
        const chartHeight = 150;
        return <article className="top100-metric-card" key={view}>
          <div className="top100-card-heading"><div><strong>{view === "overall" ? "Overall" : stageName(view)}</strong><small>Current top {metric.top_n}</small></div><span>{current.sample_size.toLocaleString()} sampled</span></div>
          <div className="top100-status-table" role="table" aria-label={`${view} top 100 status metrics`}>
            <div className="top100-status-header" role="row"><span>Status</span><span>Now</span><span>1h avg</span><span>Avg dwell</span></div>
            {statuses.map((status) => <div className="top100-status-row" role="row" key={status}><span><i className={`top100-status-swatch status-${statusClass(status)}`} />{monitoringStatusLabel(status)}</span><strong>{numberLabel(current.percentages?.[status] ?? 0, 1)}%</strong><span>{numberLabel(rolling.percentages?.[status] ?? 0, 1)}%</span><span>{durationLabel(rolling.average_time_seconds?.[status])}</span></div>)}
          </div>
          <div className="top100-chart-heading"><span>All-time percentage trend</span><small>{series.length ? `${series.length} cycles` : "Waiting for cycle history"}</small></div>
          {series.length ? <div className="top100-chart-wrap"><svg aria-label={`${view} top 100 status percentages over time`} role="img" viewBox={`0 0 ${chartWidth} ${chartHeight}`}><line className="top100-chart-guide" x1="10" x2={chartWidth - 10} y1="10" y2="10" /><line className="top100-chart-guide" x1="10" x2={chartWidth - 10} y1={chartHeight / 2} y2={chartHeight / 2} /><line className="top100-chart-guide" x1="10" x2={chartWidth - 10} y1={chartHeight - 10} y2={chartHeight - 10} />{statuses.map((status) => <path className={`top100-chart-line status-${statusClass(status)}`} d={chartPath(series, status, chartWidth, chartHeight)} key={status} />)}</svg><div className="top100-chart-axis"><span>0%</span><span>50%</span><span>100%</span><span>{formatDate(series[series.length - 1]?.finished_at)}</span></div></div> : <div className="top100-chart-empty">Cycle-level top-100 telemetry will appear after the next completed worker cycle.</div>}
          <div className="top100-chart-heading"><span>Average dwell by status</span><small>time continuously represented in the top 100</small></div>
          {series.length ? <div className="top100-chart-wrap"><svg aria-label={`${view} top 100 average status dwell over time`} role="img" viewBox={`0 0 ${chartWidth} ${chartHeight}`}><line className="top100-chart-guide" x1="10" x2={chartWidth - 10} y1="10" y2="10" /><line className="top100-chart-guide" x1="10" x2={chartWidth - 10} y1={chartHeight / 2} y2={chartHeight / 2} /><line className="top100-chart-guide" x1="10" x2={chartWidth - 10} y1={chartHeight - 10} y2={chartHeight - 10} />{statuses.map((status) => <path className={`top100-chart-line status-${statusClass(status)}`} d={dwellPath(series, status, chartWidth, chartHeight)} key={status} />)}</svg><div className="top100-chart-axis"><span>0</span><span>Higher dwell</span><span>{formatDate(series[series.length - 1]?.finished_at)}</span></div></div> : <div className="top100-chart-empty">Dwell-time history will appear after the next completed worker cycle.</div>}
          <div className="top100-card-footer"><span>Rolling window: {rolling.sample_count} cycles</span><span>Average dwell is time continuously held in the status while represented in the top 100.</span></div>
        </article>;
      })}
    </div>
  </section>;
}

function MonitoringCycleActivityPanel({ activity }: { activity: MonitoringCycleActivity }) {
  const runs = activity.runs ?? [];
  const stages: AssessmentStage[] = ["stage1", "stage2", "stage3"];
  const [frameIndex, setFrameIndex] = useState(activity.default_start_index ?? 0);
  const [playing, setPlaying] = useState(runs.length > 1);
  const [speed, setSpeed] = useState(1);
  const latestRunId = runs[runs.length - 1]?.cycle_id ?? null;
  const latestRunRef = useRef<string | null>(null);
  const evaluatedTotal = stages.reduce((total, stage) => total + (activity.throughput[stage]?.evaluated ?? 0), 0);
  const needsTotal = stages.reduce((total, stage) => total + (activity.throughput[stage]?.needs_more_data ?? 0), 0);
  const handoffTotal = stages.reduce((total, stage) => total + (activity.throughput[stage]?.handoffs ?? 0), 0);

  useEffect(() => {
    if (!latestRunId || latestRunRef.current === latestRunId) return;
    latestRunRef.current = latestRunId;
    setFrameIndex(activity.default_start_index ?? Math.max(0, runs.length - (activity.recent_run_count ?? 12)));
    setPlaying(runs.length > 1);
  }, [latestRunId, runs.length]);

  useEffect(() => {
    if (playing && frameIndex >= runs.length - 1) setPlaying(false);
  }, [frameIndex, playing, runs.length]);

  const frame = runs[Math.min(frameIndex, Math.max(0, runs.length - 1))];
  const stage2Intake = frame?.stage2_intake;
  const stage2Priority = frame?.stage2_reassessment;
  const queueEventsFor = (stage: AssessmentStage): MonitoringQueueEvent[] => {
    const recorded = (frame?.queue_events ?? []).filter((event) => event.stage === stage);
    if (recorded.length) return recorded;

    // Older cycle records predate explicit queue telemetry. Infer only the
    // jobs represented by their event stream, never from stage status totals.
    return (frame?.events ?? [])
      .filter((event) => event.stage === stage && event.candidate_id)
      .map((event) => ({
        candidate_id: event.candidate_id,
        stage,
        kind: event.kind === "handoff" ? "handoff" : event.kind === "reassessment" ? "reassessment" : "evaluation",
        queue_status: event.kind === "handoff" ? "Promising" : event.kind === "reassessment" ? "Needs More Data" : "New Job",
        reason: "legacy_cycle_event",
        source_candidate_id: event.source_candidate_id,
      }));
  };
  const representativeDots = (stage: AssessmentStage): Array<{ candidate_id: string; status: string; index: number; rank: number }> => {
    const fallbackEvents = (frame?.events ?? []).filter((event) => event.stage === stage);
    const rankedSamples = frame?.ranked_samples?.[stage] ?? [];
    const cap = stage === "stage1" ? 42 : stage === "stage2" ? 30 : 22;
    if (rankedSamples.length) {
      return rankedSamples.slice(0, cap).map((sample, index) => ({
        candidate_id: sample.candidate_id ?? `${stage}-rank-${index}`,
        status: sample.status ?? "unknown",
        index,
        rank: sample.rank ?? index + 1,
      }));
    }
    return fallbackEvents.slice(0, cap).map((event, index) => ({
      candidate_id: event.candidate_id ?? `${stage}-event-${index}`,
      status: event.status ?? "unknown",
      index,
      rank: index + 1,
    }));
  };
  const rankDotStyle = (stage: AssessmentStage, index: number, speedMultiplier: number): CSSProperties => {
    const capacity = stage === "stage1" ? 42 : stage === "stage2" ? 30 : 22;
    const columns = 7;
    const rows = Math.ceil(capacity / columns);
    const column = index % columns;
    const row = Math.floor(index / columns);
    return {
      left: `${10 + ((column + 0.5) / columns) * 80}%`,
      top: `${8 + ((row + 0.5) / rows) * 80}%`,
      transitionDuration: `${Math.max(320, 900 / speedMultiplier)}ms`,
      transitionDelay: `${Math.min(index, 10) * (55 / speedMultiplier)}ms`,
    };
  };
  const queueDotStyle = (index: number, total: number, speedMultiplier: number): CSSProperties => {
    const columns = Math.min(5, Math.max(1, Math.ceil(total / 4)));
    const column = index % columns;
    const depth = Math.floor(index / columns);
    return {
      left: `${8 + ((column + 0.5) / columns) * 84}%`,
      bottom: `${4 + depth * 10}px`,
      transitionDuration: `${Math.max(320, 850 / speedMultiplier)}ms`,
      transitionDelay: `${Math.min(index, 10) * (55 / speedMultiplier)}ms`,
    };
  };
  const incomingJobs = queueEventsFor("stage1").filter((event) => event.kind === "intake");
  const incomingCount = incomingJobs.length;
  const incomingDots = Math.min(8, Math.max(incomingCount ? 1 : 0, Math.round(incomingCount / 4)));
  const currentStage2Handoffs = queueEventsFor("stage3").filter((event) => event.kind === "handoff").length;
  const frameLabel = frame ? formatDate(frame.finished_at) : "Waiting for first cycle";
  const stageQueueTotal = (stage: AssessmentStage) => queueEventsFor(stage).length;
  const rankedSamplesFor = (stage: AssessmentStage) => frame?.ranked_samples?.[stage] ?? [];
  useEffect(() => {
    if (!playing || runs.length < 2) return;
    const timer = window.setInterval(() => {
      setFrameIndex((current) => Math.min(current + 1, runs.length - 1));
    }, Math.max(650, 2200 / speed));
    return () => window.clearInterval(timer);
  }, [playing, runs.length, speed]);

  return <section className="ops-section cycle-activity-section" aria-labelledby="cycle-activity-title">
    <div className="ops-section-heading"><div><p className="eyebrow">Live worker activity</p><h4 id="cycle-activity-title">Pipeline motion · all saved cycles</h4></div><span className="section-note">{activity.run_count} saved cycles · latest window starts by default</span></div>
    {!runs.length ? <div className="ops-empty">No cycle history has been recorded in this deployment yet. The motion view will begin when the next worker cycle completes.</div> : <>
      <div className="cycle-summary-grid"><article className="cycle-summary-card"><span className="metric-label">Completed cycles</span><strong>{activity.run_count.toLocaleString()}</strong><small>all worker runs retained for replay</small></article><article className="cycle-summary-card"><span className="metric-label">Candidates evaluated</span><strong>{evaluatedTotal.toLocaleString()}</strong><small>latest {activity.window_minutes} minutes · {numberLabel(evaluatedTotal / Math.max(1, activity.window_minutes), 2)} per minute</small></article><article className="cycle-summary-card"><span className="metric-label">Needs more data</span><strong>{needsTotal.toLocaleString()}</strong><small>latest {activity.window_minutes}-minute window</small></article><article className="cycle-summary-card"><span className="metric-label">Downstream handoffs</span><strong>{handoffTotal.toLocaleString()}</strong><small>Stage 3 calls in latest window</small></article></div>
      <div className="pipeline-motion-shell">
        <div className="pipeline-motion-toolbar"><div className="motion-intake-badge">New candidate entering · {incomingCount.toLocaleString()} in this cycle</div><div className="motion-controls"><button className="button button-quiet" type="button" onClick={() => setPlaying((current) => !current)} disabled={runs.length < 2}>{playing ? "Ⅱ Pause" : "▶ Play"}</button><label><span>Speed</span><select aria-label="Motion replay speed" value={speed} onChange={(event) => setSpeed(Number(event.target.value))}><option value={0.5}>0.5×</option><option value={1}>1×</option><option value={2}>2×</option><option value={4}>4×</option></select></label><span className="motion-time">{frameLabel}</span></div></div>
        <div className="pipeline-motion-track" aria-label="Candidate movement from universe through Stage 3">
          <article className="motion-lane motion-universe-lane"><div className="motion-lane-title"><strong>Candidate universe</strong><small>{incomingCount.toLocaleString()} arrivals</small></div><div className="motion-universe-intake"><div className="motion-universe-source"><span>New targets</span><div className="motion-universe-dots">{Array.from({ length: incomingDots }, (_, index) => <span className="motion-dot neutral" key={`arrival-${index}`} title="New candidate from target universe" />)}</div></div><span className="motion-universe-arrow" aria-hidden="true">→</span><span className="motion-universe-destination">Stage 1 queue</span></div><div className="motion-lane-caption">New targets enter Stage 1 only as downstream work opens a job.</div></article>
          {stages.map((stage) => { const dots = representativeDots(stage); const queueJobs = queueEventsFor(stage).slice(0, 20); const summary = frame?.stages[stage] ?? {}; const handoffs = stage === "stage3" ? currentStage2Handoffs : queueJobs.filter((job) => job.kind === "handoff").length; const stage2QueueCaption = stage === "stage2" && stage2Intake ? `${stage2Intake.evaluated_count ?? 0} fresh Stage 1 → Stage 2 candidates called first · Needs More Data reassessment ${stage2Priority?.status === "deferred" ? "deferred" : "available"}.` : stage === "stage2" && stage2Priority ? `${stage2Priority.evaluated_count ?? 0} top-${stage2Priority.threshold ?? 100} jobs called in rank order · ${stage2Priority.unchanged_candidate_ids?.length ?? 0} awaiting new coverage.` : null; return <article className={`motion-lane motion-${stage}-lane`} key={stage}><div className="motion-lane-title"><strong>{stageName(stage)}</strong><small>{dots.length} ranked sample dots · {stageQueueTotal(stage).toLocaleString()} jobs</small></div><div className="motion-lane-queue"><span>Work queue</span><div className="motion-queue-stacks">{queueJobs.map((job, index) => <span className={`motion-dot ${monitoringDotClass(job.queue_status ?? "New Job")} motion-queue-job ${job.kind === "handoff" ? "motion-queue-handoff" : job.kind === "reassessment" ? "motion-queue-reassessment" : "motion-queue-intake"}`} key={`${stage}-${job.candidate_id ?? "job"}`} style={queueDotStyle(index, queueJobs.length, speed)} title={`${job.kind ?? "job"} · ${job.queue_status ?? "queued"}`} />)}</div>{!queueJobs.length && <small className="motion-queue-empty">No jobs called in this cycle</small>}</div><div className="motion-lane-field"><div className="motion-rank-guide" aria-hidden="true"><span>higher rank</span><span>lower rank</span></div><div className="motion-ranked-dots">{dots.map((dot, index) => <span className={`motion-dot ${monitoringDotClass(dot.status)}`} key={`${stage}-${dot.candidate_id}`} style={rankDotStyle(stage, index, speed)} title={`Rank ${dot.rank} · ${dot.status} · representative of ${stageName(stage)} ranking`} />)}{!dots.length && <span className="motion-field-empty">No ranked candidates</span>}</div></div><div className="motion-lane-caption">{stage2QueueCaption ?? (handoffs ? `${handoffs} candidate${handoffs === 1 ? "" : "s"} called downstream this cycle.` : stage === "stage3" ? "Reassess Needs More Data in rank order." : summary.evaluated ? "Rankings updated; no downstream job was called." : "Waiting for a downstream call.")}</div></article>; })}
        </div>
        <div className="pipeline-motion-footer"><div className="pipeline-dot-legend"><span><i className="pipeline-dot promising" />Promising</span><span><i className="pipeline-dot needs-more-data" />Needs more data</span><span><i className="pipeline-dot inconclusive" />Inconclusive</span><span><i className="pipeline-dot known-match" />Known</span></div><label className="motion-scrubber"><span>Replay position</span><input aria-label="Pipeline motion replay position" type="range" min={0} max={Math.max(0, runs.length - 1)} value={frameIndex} onChange={(event) => { setFrameIndex(Number(event.target.value)); setPlaying(false); }} /><small>{frameIndex + 1} / {runs.length}</small></label></div>
      </div>
      <p className="cycle-activity-note">Ranking dots are ordered from highest rank to lowest rank and are intentionally representative rather than one-to-one. Queue dots show jobs called during the selected cycle; Inconclusive candidates remain in their stage ranking and do not enter a queue unless a later job is explicitly called. Drag the replay position to inspect any saved cycle.</p>
      <div className="timing-card-grid">{stages.map((stage) => { const timing = activity.timings[stage] ?? { average_cycle_seconds: null, average_needs_more_data_seconds: null, average_dispatch_seconds: null, handoffs: 0 }; const flow = activity.throughput[stage] ?? { evaluated: 0, promising: 0, needs_more_data: 0, inconclusive: 0, known_match: 0, handoffs: 0 }; return <article className="timing-card" key={stage}><div className="timing-card-heading"><strong>{stageName(stage)}</strong><span>{flow.evaluated.toLocaleString()} assessed</span></div><div className="timing-stat-row"><span><small>Avg stage time</small><strong>{durationLabel(timing.average_cycle_seconds)}</strong></span><span><small>Needs data time</small><strong>{durationLabel(timing.average_needs_more_data_seconds)}</strong></span></div><div className="timing-stat-row"><span><small>Promising</small><strong>{flow.promising}</strong></span><span><small>Inconclusive</small><strong>{flow.inconclusive}</strong></span></div><div className="timing-stat-row"><span><small>Needs more data</small><strong>{flow.needs_more_data}</strong></span><span><small>Handoffs</small><strong>{flow.handoffs}</strong></span></div>{stage === "stage2" && <p>Needs More Data reassessment time: <strong>{durationLabel(timing.average_needs_more_data_seconds)}</strong>.</p>}{stage === "stage3" && <p>Stage 2 handoff processing time: <strong>{durationLabel(timing.average_dispatch_seconds)}</strong> · {timing.handoffs} handoffs.</p>}</article>; })}</div>
    </>}
  </section>;
}

export function MonitoringPanel({ refreshKey }: { refreshKey: number }) {
  const [overview, setOverview] = useState<MonitoringOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pollKey, setPollKey] = useState(0);
  const [controlBusy, setControlBusy] = useState(false);
  const [controlMessage, setControlMessage] = useState<string | null>(null);
  const [migrationBusy, setMigrationBusy] = useState(false);
  const [migrationMessage, setMigrationMessage] = useState<string | null>(null);
  const [migrationRoute, setMigrationRoute] = useState<"universe:stage1" | "stage1:stage2" | "stage2:stage3">("stage1:stage2");
  const [migrationCount, setMigrationCount] = useState("100");

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

  const changeWorkerControl = async (action: "pause" | "resume") => {
    setControlBusy(true);
    setControlMessage(null);
    try {
      const response = await fetchJson<{ control: MonitoringOverview["control"] }>("/api/monitoring/control", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action }),
      });
      setControlMessage(action === "pause" ? "Pause requested; the current cycle will finish before new work stops." : "Resume requested; the worker will start on its next heartbeat.");
      setPollKey((current) => current + 1);
      if (overview) setOverview({ ...overview, control: response.control });
    } catch (cause) {
      setControlMessage(cause instanceof Error ? cause.message : "Worker control request failed.");
    } finally {
      setControlBusy(false);
    }
  };

  const queueManualMigration = async () => {
    const [fromStage, toStage] = migrationRoute.split(":") as [AssessmentStage | "universe", AssessmentStage];
    const count = Number.parseInt(migrationCount, 10);
    if (!Number.isInteger(count) || count < 1 || count > 100) {
      setMigrationMessage("Choose a migration count from 1 to 100.");
      return;
    }
    setMigrationBusy(true);
    setMigrationMessage(null);
    try {
      const migration = await fetchJson<ManualMigration>("/api/monitoring/migrations", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ from_stage: fromStage, to_stage: toStage, count }),
      });
      setMigrationMessage(`${fromStage === "universe" ? "Candidate Universe" : stageName(fromStage)} → ${stageName(toStage)} migration queued for ${migration.requested_count} candidate${migration.requested_count === 1 ? "" : "s"}.`);
      setPollKey((current) => current + 1);
    } catch (cause) {
      setMigrationMessage(cause instanceof Error ? cause.message : "Manual migration request failed.");
    } finally {
      setMigrationBusy(false);
    }
  };

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
  const pipelinePulse = assessmentStages.map((stage) => {
    const throughput = overview?.cycle_activity.throughput[stage] ?? { evaluated: 0, promising: 0, needs_more_data: 0, inconclusive: 0, known_match: 0, handoffs: 0, per_minute: 0 };
    const timing = overview?.cycle_activity.timings[stage] ?? { average_cycle_seconds: null, average_needs_more_data_seconds: null, average_dispatch_seconds: null, handoffs: 0 };
    const queued = overview?.cycle_activity.queue.current?.[stage] ?? {};
    return {
      stage,
      throughput,
      timing,
      queued: Object.values(queued).reduce((sum, count) => sum + Number(count || 0), 0),
      retained: overview?.stages[stage]?.total_candidates ?? 0,
    };
  });
  const slowestStage = [...pipelinePulse].sort((left, right) => (right.timing.average_cycle_seconds ?? 0) - (left.timing.average_cycle_seconds ?? 0))[0];
  const totalQueue = pipelinePulse.reduce((sum, stage) => sum + stage.queued, 0);
  const totalEvaluated = pipelinePulse.reduce((sum, stage) => sum + stage.throughput.evaluated, 0);
  const totalNeedsMoreData = pipelinePulse.reduce((sum, stage) => sum + stage.throughput.needs_more_data, 0);
  const totalHandoffs = pipelinePulse.reduce((sum, stage) => sum + stage.throughput.handoffs, 0);
  const latestStage2Intake = overview.cycle_activity.runs[overview.cycle_activity.runs.length - 1]?.stage2_intake;
  const latestStage2Reassessment = overview.cycle_activity.runs[overview.cycle_activity.runs.length - 1]?.stage2_reassessment;
  const stage2Gate = latestStage2Reassessment?.waterfall_gate;
  const stage2PromisingFirst = stage2Gate?.enforced === false;

  if (loading && !overview) {
    return <section className="operations-panel"><div className="empty-state"><div className="loading-ring" aria-hidden="true" /><h4>Reading ML operations state.</h4><p>This view only reads persisted metrics. It never starts the worker.</p></div></section>;
  }
  if (error && !overview) {
    return <section className="operations-panel"><div className="error-state" role="alert"><span className="status-dot muted" /><h4>ML operations data unavailable</h4><p>{error}</p></div></section>;
  }
  if (!overview) return null;

  return <section className="operations-panel" aria-labelledby="operations-title">
    <div className="operations-opening-panel">
      <section className="pipeline-insight-card" aria-labelledby="operations-title">
        <div className="pipeline-insight-heading"><div><p className="eyebrow accent">Pipeline pulse</p><h3 id="operations-title">Where the waterfall is slowing</h3><p>Throughput, queue pressure, and downstream handoffs from the most recent worker cycles.</p></div><span className={`pipeline-health-indicator ${overview.worker.state === "error" || overview.control.pipeline_paused ? "attention" : "flowing"}`}><span className="status-dot" />{overview.control.pipeline_paused ? "Paused" : overview.worker.state === "error" ? "Needs attention" : "Live flow"}</span></div>
        <div className="pipeline-flow-track" aria-label="Pipeline stage flow summary">{pipelinePulse.map((stage) => { const maxEvaluated = Math.max(1, ...pipelinePulse.map((item) => item.throughput.evaluated)); return <article className={`pipeline-flow-stage ${stage.stage}`} key={stage.stage}><div className="pipeline-flow-heading"><strong>{stageName(stage.stage)}</strong><span>{stage.throughput.evaluated.toLocaleString()} assessed</span></div><div className="pipeline-flow-meter"><span style={{ width: `${Math.max(stage.throughput.evaluated ? 8 : 0, Math.min(100, stage.throughput.evaluated / maxEvaluated * 100))}%` }} /></div><small>{stage.queued.toLocaleString()} jobs called · {stage.throughput.handoffs} handoffs</small></article>; })}</div>
        <div className="pipeline-insight-metrics"><article><span className="metric-label">Slowest lane</span><strong>{slowestStage ? stageName(slowestStage.stage) : "—"}</strong><small>{slowestStage?.timing.average_cycle_seconds ? `${durationLabel(slowestStage.timing.average_cycle_seconds)} average cycle` : "No timing yet"}</small></article><article><span className="metric-label">Flow rate</span><strong>{numberLabel(totalEvaluated / Math.max(1, overview.cycle_activity.window_minutes), 2)} / min</strong><small>{totalEvaluated.toLocaleString()} assessed in the window</small></article><article><span className="metric-label">Needs more data</span><strong>{totalNeedsMoreData.toLocaleString()}</strong><small>reassessments or new evaluations</small></article><article><span className="metric-label">Jobs called</span><strong>{totalQueue.toLocaleString()}</strong><small>{totalHandoffs.toLocaleString()} downstream handoffs in window</small></article><article><span className="metric-label">Stage 2 priority</span><strong>{latestStage2Intake?.evaluated_count ?? 0} fresh called</strong><small>Stage 1 → Stage 2 intake runs before Needs More Data reassessments</small></article><article><span className="metric-label">Stage 2 policy</span><strong>{latestStage2Intake?.status === "deferred" ? "Fresh intake first" : stage2Gate ? (stage2PromisingFirst ? "Promising first" : stage2Gate.open ? "Open" : "Stage 1 + 3 paused") : "Not recorded"}</strong><small>{latestStage2Intake?.status === "deferred" ? "Needs More Data reassessment is deferred until more Stage 1 evidence is processed." : stage2Gate ? (stage2PromisingFirst ? `${stage2Gate.promising_count ?? 0} Promising · Needs More Data retained behind them` : `${stage2Gate.promising_count ?? 0} / ${stage2Gate.threshold ?? 100} Stage 2 top-window candidates Promising`) : "The policy appears after the next worker cycle."}</small></article></div>
        <div className="pipeline-insight-footer"><span><strong>Catalog:</strong> {catalogSummary} · {catalogDetail}</span><span>{overview.cycle_activity.run_count} cycles saved · metrics use latest {overview.cycle_activity.recent_run_count} cycles</span></div>
      </section>
      <aside className="operations-status-rail" aria-label="Worker and data status">
        <article className="ops-mini-card ops-mini-control"><div className={`operations-gate ${overview.control.pipeline_paused ? "paused" : "armed"}`}><span className="status-dot" />{overview.control.pipeline_paused ? "Processing paused" : "Worker armed"}<small>{overview.control.reason}</small></div><div className="operations-control-actions"><button className="button button-outline" type="button" onClick={() => void changeWorkerControl("pause")} disabled={controlBusy || overview.control.pipeline_paused}>Pause</button><button className="button button-quiet" type="button" onClick={() => void changeWorkerControl("resume")} disabled={controlBusy || !overview.control.pipeline_paused || !overview.control.autostart_enabled}>Resume</button></div>{controlMessage && <p className="operations-control-message" role="status">{controlMessage}</p>}{!overview.control.autostart_enabled && <p className="operations-control-note">Resume disabled by deployment configuration.</p>}</article>
        <article className="ops-mini-card"><span className="metric-label">Universe progress</span><strong>{overview.universe.processed_count.toLocaleString()} <small>/ {overview.universe.configured_count.toLocaleString()}</small></strong><div className="progress-track"><span style={{ width: `${Math.max(0, Math.min(100, overview.universe.progress_percent))}%` }} /></div><span className="metric-detail">{overview.universe.remaining_count.toLocaleString()} remaining · {overview.universe.manifest_count.toLocaleString()} manifested</span></article>
        <article className="ops-mini-card"><span className="metric-label">Worker heartbeat</span><strong>{humanizeState(overview.worker.state)}</strong><span className="metric-detail">{overview.worker.last_heartbeat ? formatDate(overview.worker.last_heartbeat) : "No heartbeat yet"}</span></article>
        <article className="ops-mini-card"><span className="metric-label">Coverage health</span><strong>{overview.data_health.coverage.product_count.toLocaleString()} <small>products</small></strong><span className="metric-detail">{overview.data_health.coverage.candidate_count.toLocaleString()} candidates · {overview.data_health.coverage.window_count.toLocaleString()} windows</span></article>
        <article className="ops-mini-card ops-migration-control"><span className="metric-label">Manual waterfall migration</span><label className="ops-control-label" htmlFor="manual-migration-route">Route</label><select id="manual-migration-route" className="ops-control-select" value={migrationRoute} onChange={(event) => setMigrationRoute(event.target.value as "universe:stage1" | "stage1:stage2" | "stage2:stage3")}><option value="universe:stage1">Candidate Universe → Stage 1</option><option value="stage1:stage2">Stage 1 → Stage 2</option><option value="stage2:stage3">Stage 2 → Stage 3</option></select><label className="ops-control-label" htmlFor="manual-migration-count">Candidates</label><input id="manual-migration-count" className="ops-control-input" type="number" min={1} max={100} value={migrationCount} onChange={(event) => setMigrationCount(event.target.value)} /><button className="button button-quiet" type="button" onClick={() => void queueManualMigration()} disabled={migrationBusy}>{migrationBusy ? "Queueing…" : "Queue migration"}</button><small className="ops-control-help">Type the batch size manually. Maximum 100 per request.</small>{migrationMessage && <p className="operations-control-message" role="status">{migrationMessage}</p>}</article>
      </aside>
    </div>
    <section className="ops-section manual-migrations-section"><div className="ops-section-heading"><div><p className="eyebrow">Operator requests</p><h4>Manual migrations</h4></div><span className="section-note">Requests are claimed by the worker and use normal evaluation and replay publishing.</span></div>{overview.manual_migrations?.length ? <div className="manual-migration-list">{overview.manual_migrations.slice(0, 8).map((migration) => <div className="manual-migration-row" key={migration.request_id}><div><strong>{migration.from_stage === "universe" ? "Candidate Universe" : stageName(migration.from_stage)} → {stageName(migration.to_stage)}</strong><small>{migration.completed_count ?? 0} / {migration.requested_count} completed · {formatDate(migration.updated_at)}</small></div><span className={`manual-migration-status ${migration.status}`}>{migration.status}</span><em>{migration.message ?? "Waiting for worker"}</em></div>)}</div> : <div className="ops-empty">No manual migration requests yet.</div>}</section>
    <JobCallDistributionPanel activity={overview.cycle_activity} />
    <ProcessedHistoryChart history={overview.processed_history} currentCount={overview.universe.processed_count} />
    <MonitoringCycleActivityPanel activity={overview.cycle_activity} />
    <MonitoringTop100StatusPanel activity={overview.cycle_activity} />
    <section className="ops-section"><div className="ops-section-heading"><div><p className="eyebrow">Stage prioritization</p><h4>Queue policy and classifications</h4></div><span className="section-note">Stage 1 and 2 use ranking windows; Stage 3 works its Needs More Data queue in rank order.</span></div><div className="stage-ops-grid">{stages.map((stage) => <article className="stage-ops-card" key={stage.stage}><div className="stage-ops-card-heading"><strong>{stageName(stage.stage)}</strong><span>{stage.priority_label}</span></div><div className="stage-ops-inline-note">{stage.priority_candidates.toLocaleString()} currently prioritized · {stage.total_candidates.toLocaleString()} candidate signals retained</div><div className="stage-counts">{["Promising", "Needs More Data", "Inconclusive", "Known Match"].map((status) => <span key={status}><strong>{stage.counts[status] ?? 0}</strong>{monitoringStatusLabel(status)}</span>)}</div><small>Last evaluation: {formatDate(stage.last_evaluated_at)}</small></article>)}</div></section>
    <section className="ops-section"><div className="ops-section-heading"><div><p className="eyebrow">Model registry</p><h4>Algorithms and evidence quality</h4></div><span className="section-note">Metrics are aggregates of persisted evaluations, not new runs.</span></div>{overview.models.length ? <div className="model-registry">{overview.models.map((model) => <article className="model-card" key={`${model.stage}-${model.model_version}`}><div className="model-card-heading"><span className="stage-badge">{stageName(model.stage)}</span><strong>{stageName(model.stage)} model</strong></div><div className="model-stat-grid"><span><small>Evaluations</small><strong>{model.evaluation_count}</strong></span><span><small>Evidence</small><strong>{numberLabel(model.mean_evidence_score, 3)}</strong></span><span><small>Holdout F1</small><strong>{numberLabel(model.mean_holdout_f1, 2)}</strong></span><span><small>Model lift</small><strong>{numberLabel(model.mean_model_lift, 2)}</strong></span></div><p>{Object.entries(model.status_counts).map(([status, count]) => `${status}: ${count}`).join(" · ") || "No classifications recorded"}</p><small>Last evaluated {formatDate(model.last_evaluated_at)}</small></article>)}</div> : <div className="ops-empty">No model evaluations are persisted yet.</div>}</section>
    <div className="operations-grid operations-grid-secondary"><section className="ops-section ops-section-half"><div className="ops-section-heading"><div><p className="eyebrow">Adaptive sampling</p><h4>Replacement-target intelligence</h4></div></div><p className="ops-explanation">{overview.sampling.explanation}</p><div className="sampling-summary"><span><strong>{overview.sampling.cursor.toLocaleString()}</strong> cursor</span><span><strong>{overview.sampling.remaining_count.toLocaleString()}</strong> remaining</span><span><strong>{overview.sampling.adaptive_enabled ? "On" : "Observed only"}</strong> adaptive policy</span></div>{overview.sampling.buckets.length ? <div className="sampling-list">{overview.sampling.buckets.slice(0, 8).map((bucket) => <div className="sampling-row" key={bucket.bucket}><div><strong>{bucket.bucket}</strong><small>{bucket.observed} observed · {bucket.processed} processed</small></div><span>{bucket.promising} promising · {bucket.needs_more_data} needs data · {bucket.known_match} known</span><em>{bucket.recommendation}</em></div>)}</div> : <div className="ops-empty">No selection buckets have been observed yet.</div>}</section><section className="ops-section ops-section-half"><div className="ops-section-heading"><div><p className="eyebrow">Sky-cell outcomes</p><h4>Where to explore next</h4></div></div><p className="ops-explanation">Ten-degree RA/Dec cells summarize observed outcomes. They are guidance for the sampler, not a scientific claim about neighboring stars.</p>{overview.sampling.spatial_clusters.length ? <div className="sampling-list">{overview.sampling.spatial_clusters.slice(0, 8).map((cluster) => <div className="sampling-row" key={`${cluster.ra_min_deg}-${cluster.dec_min_deg}`}><div><strong>RA {cluster.ra_min_deg.toFixed(0)}° · Dec {cluster.dec_min_deg.toFixed(0)}°</strong><small>{cluster.observed} observed · {cluster.promising} promising</small></div><span>{cluster.needs_more_data} needs data · {cluster.inconclusive} inconclusive · {cluster.known_match} known</span><em>{cluster.recommendation}</em></div>)}</div> : <div className="ops-empty">Spatial signals will appear after targets with coordinates are assessed.</div>}</section></div>
    <section className="ops-section"><div className="ops-section-heading"><div><p className="eyebrow">Provider and job health</p><h4>Data plane status</h4></div></div><div className="provider-grid">{overview.data_health.providers.length ? overview.data_health.providers.map((provider) => <article className="provider-card" key={provider.provider}><div><strong>{provider.provider}</strong><span className="health-pill">{provider.failed_runs ? "Needs attention" : "Healthy"}</span></div><p>{provider.runs} runs · {provider.successful_runs} successful · {provider.failed_runs} failed</p><small>{provider.products_downloaded} products downloaded · {provider.products_failed} failed</small><small>Last finished {formatDate(provider.last_finished_at)}</small></article>) : <div className="ops-empty">No provider runs recorded yet.</div>}</div></section>
    <div className="operations-grid operations-grid-secondary operations-grid-ledger"><section className="ops-section ops-section-half"><div className="ops-section-heading"><div><p className="eyebrow">Execution ledger</p><h4>Recent batches</h4></div></div>{overview.data_health.recent_batches.length ? <div className="ledger-list">{overview.data_health.recent_batches.slice(0, 5).map((batch) => <div className="ledger-row" key={batch.batch_id}><div><strong>{stageName(batch.stage)} · {batch.status}</strong><small>{batch.selection_policy} · {formatDate(batch.started_at)}</small></div><span>{batch.evaluated_count} evaluated · {batch.failed_count} failed</span></div>)}</div> : <div className="ops-empty">No processing batches have been recorded.</div>}</section><section className="ops-section ops-section-half"><div className="ops-section-heading"><div><p className="eyebrow">Pipeline event stream</p><h4>Recent events</h4></div></div>{overview.data_health.recent_events.length ? <div className="ledger-list">{overview.data_health.recent_events.slice(0, 5).map((event) => <div className="ledger-row" key={`${event.sequence_number}-${event.event_type}`}><div><strong>{event.event_type.replaceAll("_", " ")}</strong><small>{stageName(event.stage)} · {formatDate(event.occurred_at)}</small></div><span>{event.model_run_id ?? "system event"}</span></div>)}</div> : <div className="ops-empty">No pipeline events have been recorded.</div>}</section></div>
    <footer className="operations-footer"><span className="status-dot muted" /> Last metrics read {formatDate(overview.generated_at)} · Refreshing this view never starts processing.</footer>
  </section>;
}

function ProcessedHistoryChart({
  history,
  currentCount,
}: {
  history: Array<{ occurred_at: string | null; processed_target_count: number }>;
  currentCount: number;
}) {
  const points = history.filter((point) => point.occurred_at && Number.isFinite(point.processed_target_count));
  if (!points.length) {
    return <section className="processed-history-card" aria-label="Cumulative processed target history"><div className="processed-history-heading"><div><p className="eyebrow">Cumulative search progress</p><h4>Targets processed over time</h4></div><strong>{currentCount.toLocaleString()}</strong></div><div className="processed-history-empty">The chart will begin after the next published worker interval.</div></section>;
  }
  const width = 760;
  const height = 170;
  const padding = 14;
  const maximum = Math.max(1, ...points.map((point) => point.processed_target_count));
  const maximumIndex = Math.max(1, points.length - 1);
  const path = points.map((point, index) => {
    const x = padding + (index / maximumIndex) * (width - padding * 2);
    const y = height - padding - (point.processed_target_count / maximum) * (height - padding * 2);
    return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(" ");
  return <section className="processed-history-card" aria-label="Cumulative processed target history"><div className="processed-history-heading"><div><p className="eyebrow">Cumulative search progress</p><h4>Targets processed over time</h4></div><strong>{currentCount.toLocaleString()}</strong></div><div className="processed-history-chart-wrap"><svg aria-label="Cumulative targets processed over all saved publish intervals" role="img" viewBox={`0 0 ${width} ${height}`}><line className="processed-history-guide" x1={padding} x2={width - padding} y1={padding} y2={padding} /><line className="processed-history-guide" x1={padding} x2={width - padding} y1={height / 2} y2={height / 2} /><line className="processed-history-guide" x1={padding} x2={width - padding} y1={height - padding} y2={height - padding} /><path className="processed-history-line" d={path} /></svg><div className="processed-history-axis"><span>0</span><span>{maximum.toLocaleString()}</span><span>{formatDate(points[points.length - 1]?.occurred_at)}</span></div></div><div className="processed-history-footer"><span>{points.length.toLocaleString()} saved publish intervals</span><span>One point per published ranking snapshot · targets with published evaluations</span></div></section>;
}

function JobCallDistributionPanel({ activity }: { activity: MonitoringCycleActivity }) {
  const stages: AssessmentStage[] = ["stage1", "stage2", "stage3"];
  const series = activity.queue.series ?? [];
  const totals = stages.reduce<Record<AssessmentStage, number>>((result, stage) => {
    result[stage] = series.reduce((total, point) => total + Number(point[stage] ?? 0), 0);
    return result;
  }, { stage1: 0, stage2: 0, stage3: 0 });
  const totalJobs = stages.reduce((total, stage) => total + totals[stage], 0);
  const latest = series[series.length - 1];
  const latestTotal = latest ? stages.reduce((total, stage) => total + Number(latest[stage] ?? 0), 0) : 0;
  const chartWidth = 720;
  const chartHeight = 170;
  const padding = 16;
  const chartBottom = chartHeight - padding;
  const maxCycleJobs = Math.max(1, ...series.map((point) => stages.reduce((total, stage) => total + Number(point[stage] ?? 0), 0)));
  const stepWidth = series.length ? (chartWidth - padding * 2) / series.length : chartWidth - padding * 2;
  const colors: Record<AssessmentStage, string> = { stage1: "job-call-stage1", stage2: "job-call-stage2", stage3: "job-call-stage3" };

  return <section className="ops-section job-call-distribution-section" aria-labelledby="job-call-distribution-title"><div className="ops-section-heading"><div><p className="eyebrow">Dispatch distribution</p><h4 id="job-call-distribution-title">Where jobs are being called</h4></div><span className="section-note">Latest {activity.recent_run_count} saved cycles · {activity.window_minutes}-minute operating window</span></div>{!series.length || !totalJobs ? <div className="ops-empty">No queue-call telemetry is available yet. The distribution will populate after a cycle records stage-specific jobs.</div> : <><div className="job-call-summary"><div className="job-call-stack" aria-label="Share of jobs called by stage"><span className="job-call-stack-stage1" style={{ width: `${totals.stage1 / totalJobs * 100}%` }} /><span className="job-call-stack-stage2" style={{ width: `${totals.stage2 / totalJobs * 100}%` }} /><span className="job-call-stack-stage3" style={{ width: `${totals.stage3 / totalJobs * 100}%` }} /></div><div className="job-call-legend">{stages.map((stage) => { const share = totals[stage] / totalJobs * 100; return <span key={stage}><i className={colors[stage]} />{stageName(stage)} <strong>{totals[stage].toLocaleString()}</strong> <small>{share.toFixed(1)}%</small></span>; })}</div></div><div className="job-call-chart-wrap"><svg aria-label="Stage job-call distribution over the latest saved cycles" role="img" viewBox={`0 0 ${chartWidth} ${chartHeight}`}><line className="job-call-guide" x1={padding} x2={chartWidth - padding} y1={padding} y2={padding} /><line className="job-call-guide" x1={padding} x2={chartWidth - padding} y1={chartHeight / 2} y2={chartHeight / 2} /><line className="job-call-guide" x1={padding} x2={chartWidth - padding} y1={chartBottom} y2={chartBottom} />{series.map((point, index) => { const x = padding + index * stepWidth + Math.max(1, stepWidth * 0.16); const barWidth = Math.max(3, stepWidth * 0.68); const cycleTotal = stages.reduce((total, stage) => total + Number(point[stage] ?? 0), 0); let offset = 0; return stages.map((stage) => { const value = Number(point[stage] ?? 0); const height = cycleTotal ? value / maxCycleJobs * (chartBottom - padding) : 0; const y = chartBottom - offset - height; offset += height; return <rect className={colors[stage]} height={height} key={`${point.finished_at}-${stage}`} rx="2" width={barWidth} x={x} y={y}><title>{`${stageName(stage)} · ${value} jobs · ${formatDate(point.finished_at)}`}</title></rect>; }); })}</svg><div className="job-call-chart-axis"><span>Older saved cycle</span><span>Latest cycle · {latestTotal.toLocaleString()} jobs</span></div></div><div className="job-call-table" role="table" aria-label="Stage job-call distribution summary"><div className="job-call-table-row job-call-table-header" role="row"><span>Stage</span><span>Saved-window jobs</span><span>Share</span><span>Latest cycle</span></div>{stages.map((stage) => <div className="job-call-table-row" role="row" key={stage}><span><i className={colors[stage]} />{stageName(stage)}</span><strong>{totals[stage].toLocaleString()}</strong><span>{(totals[stage] / totalJobs * 100).toFixed(1)}%</span><span>{Number(latest?.[stage] ?? 0).toLocaleString()}</span></div>)}</div><p className="job-call-note">Bars show the number and mix of jobs called in each saved cycle. This is queue work—not the number of candidates retained in a ranking.</p></>}</section>;
}

const explainerScenes = [
  {
    kicker: "01 · CAPTURE",
    title: "A transit leaves a measurable fingerprint",
    body: "When an orbiting object crosses between us and its star, a telescope records a tiny, temporary drop in brightness. That repeating dip is the starting clue—not a confirmation.",
  },
  {
    kicker: "02 · COMPARE",
    title: "The clue becomes a signal to test",
    body: "TransitScope turns the brightness trace into a candidate signal, checks whether the pattern repeats across observations, and compares the evidence with what is already known.",
  },
  {
    kicker: "03 · INVESTIGATE",
    title: "Evidence moves through a ranked pipeline",
    body: "A sample from the 126,000-target universe enters Stage 1, then the strongest surviving signals receive deeper tests in Stage 2 and Stage 3. Rankings change as evidence accumulates.",
  },
] as const;

function ExplainerVisual({ sceneIndex }: { sceneIndex: number }) {
  if (sceneIndex === 0) {
    return <div className="explainer-visual explainer-visual-capture" aria-label="Animation showing an orbiting object crossing a star and creating a dip in a light curve" role="img">
      <svg viewBox="0 0 760 260" aria-hidden="true">
        <defs><radialGradient id="explainer-star-gradient"><stop offset="0%" stopColor="#fff8ca" /><stop offset="62%" stopColor="#ffd978" /><stop offset="100%" stopColor="#e69a42" /></radialGradient></defs>
        <g className="explainer-stars"><circle cx="34" cy="33" r="1.4" /><circle cx="76" cy="215" r="1" /><circle cx="180" cy="38" r="1.1" /><circle cx="232" cy="215" r="1.4" /><circle cx="310" cy="30" r="1" /><circle cx="700" cy="35" r="1.2" /><circle cx="724" cy="212" r="1.1" /></g>
        <text className="explainer-svg-label" x="52" y="38">LINE OF SIGHT</text>
        <ellipse className="explainer-orbit" cx="150" cy="132" rx="84" ry="44" />
        <circle className="explainer-star-glow" cx="150" cy="132" r="49" />
        <circle className="explainer-star" cx="150" cy="132" r="35" fill="url(#explainer-star-gradient)" />
        <circle className="explainer-star-highlight" cx="138" cy="120" r="11" />
        <circle className="explainer-planet" cx="234" cy="132" r="9" />
        <path className="explainer-photon-path" d="M271 132 H326" />
        <circle className="explainer-photon explainer-photon-one" cx="278" cy="132" r="3" /><circle className="explainer-photon explainer-photon-two" cx="294" cy="132" r="3" /><circle className="explainer-photon explainer-photon-three" cx="310" cy="132" r="3" />
        <rect className="explainer-chart-card" x="344" y="53" width="375" height="164" rx="11" />
        <text className="explainer-svg-label" x="365" y="78">MEASURED BRIGHTNESS</text>
        <line className="explainer-grid-line" x1="365" x2="700" y1="105" y2="105" /><line className="explainer-grid-line" x1="365" x2="700" y1="156" y2="156" /><line className="explainer-grid-line" x1="365" x2="700" y1="198" y2="198" />
        <path className="explainer-curve" d="M365 126 C393 122 411 123 431 127 C451 131 457 131 474 126 C494 120 509 122 526 127 C543 132 552 164 572 177 C590 188 605 187 620 174 C636 160 645 132 663 126 C677 121 687 122 700 126" />
        <line className="explainer-cursor" x1="472" x2="472" y1="90" y2="205" />
        <text className="explainer-dip-label" x="568" y="202">TRANSIT-LIKE DIP</text>
      </svg>
      <div className="explainer-visual-readout"><span className="explainer-live-dot" /> Object crossing detected <strong>Brightness falling</strong></div>
    </div>;
  }

  if (sceneIndex === 1) {
    return <div className="explainer-visual explainer-visual-compare" aria-label="Animation showing transit signals being compared across observations" role="img">
      <svg viewBox="0 0 760 260" aria-hidden="true">
        <text className="explainer-svg-label" x="40" y="35">REPEATED OBSERVATIONS</text>
        <g className="explainer-mini-card"><rect x="34" y="55" width="205" height="155" rx="10" /><text x="52" y="80">WINDOW A</text><line x1="52" x2="220" y1="112" y2="112" /><path d="M52 131 C80 126 92 127 108 132 C125 138 132 164 146 173 C160 181 175 177 186 158 C197 139 204 128 220 132" /></g>
        <g className="explainer-mini-card"><rect x="278" y="55" width="205" height="155" rx="10" /><text x="296" y="80">WINDOW B</text><line x1="296" x2="464" y1="112" y2="112" /><path d="M296 131 C324 127 337 128 352 132 C369 138 375 164 390 172 C404 180 420 177 431 158 C442 139 449 128 464 132" /></g>
        <g className="explainer-mini-card holdout"><rect x="522" y="55" width="205" height="155" rx="10" /><text x="540" y="80">HOLDOUT C</text><line x1="540" x2="708" y1="112" y2="112" /><path d="M540 132 C564 126 580 127 596 133 C612 140 619 164 634 174 C649 184 664 178 675 159 C686 141 695 130 708 132" /><circle className="explainer-check" cx="696" cy="77" r="9" /></g>
        <path className="explainer-compare-line" d="M243 132 H270 M487 132 H514" /><path className="explainer-compare-arrow" d="M262 125 L270 132 L262 139 M506 125 L514 132 L506 139" />
      </svg>
      <div className="explainer-visual-readout"><span className="explainer-compare-pulse" /> Pattern alignment <strong>Repeated dip timing</strong></div>
    </div>;
  }

  return <div className="explainer-visual explainer-visual-pipeline" aria-label="Animation showing sampled stars moving through three assessment stages" role="img">
    <div className="explainer-universe"><span className="explainer-universe-count">126,000</span><span>target stars</span><div className="explainer-universe-dots">{Array.from({ length: 32 }, (_, index) => <i key={index} style={{ "--dot-delay": `${(index % 8) * 0.08}s`, "--dot-x": `${(index % 8) * 12 + 4}%`, "--dot-y": `${Math.floor(index / 8) * 25 + 10}%` } as CSSProperties} />)}</div></div>
    <div className="explainer-pipeline-arrow" aria-hidden="true">→</div>
    <div className="explainer-stage-track">
      {["Stage 1", "Stage 2", "Stage 3"].map((stage, index) => <div className={`explainer-stage explainer-stage-${index + 1}`} key={stage}><span>{stage}</span><div className="explainer-stage-dots">{Array.from({ length: index === 0 ? 10 : index === 1 ? 6 : 3 }, (_, dot) => <i key={dot} style={{ "--dot-delay": `${dot * 0.13}s` } as CSSProperties} />)}</div><small>{index === 0 ? "screen" : index === 1 ? "repeat" : "holdout"}</small></div>)}
    </div>
    <div className="explainer-visual-readout"><span className="explainer-pipeline-pulse" /> Evidence-weighted ranking <strong>Explore · compare · investigate</strong></div>
  </div>;
}

function HowItWorksModal({ onClose }: { onClose: () => void }) {
  const closeRef = useRef<HTMLButtonElement>(null);
  const [sceneIndex, setSceneIndex] = useState(0);
  const [tourPlaying, setTourPlaying] = useState(true);

  useEffect(() => {
    if (!tourPlaying) return undefined;
    const timer = window.setInterval(() => {
      setSceneIndex((current) => (current + 1) % explainerScenes.length);
    }, 7600);
    return () => window.clearInterval(timer);
  }, [tourPlaying]);

  useEffect(() => {
    closeRef.current?.focus();
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose(); }}>
    <section className="candidate-modal info-modal" role="dialog" aria-modal="true" aria-labelledby="how-it-works-title" onMouseDown={(event) => event.stopPropagation()}>
      <header className="modal-header">
        <div>
          <p className="eyebrow accent">About the observatory</p>
          <h3 id="how-it-works-title">How this site works</h3>
          <p className="modal-subtitle">A live, staged assessment of transit-like signals in public TESS observations.</p>
        </div>
        <button ref={closeRef} className="modal-close" type="button" aria-label="Close how this site works dialog" onClick={onClose}>×</button>
      </header>
      <div className="info-modal-content">
        <section className="explainer-origin"><div><p className="eyebrow accent">Why this idea matters</p><p className="explainer-origin-copy">Earlier in October, people used AI models to help surface a possible hidden exoplanet from public astronomy data. Read the reporting that inspired this observatory:</p></div><div className="explainer-source-links"><a href="https://www.scientificamerican.com/article/amateur-astronomer-harnesses-ai-to-find-a-possible-hidden-exoplanet/" target="_blank" rel="noreferrer">Scientific American <span>↗</span></a><a href="https://gizmodo.com/did-claude-help-an-amateur-astronomer-discover-a-new-planet-2000824393" target="_blank" rel="noreferrer">Gizmodo <span>↗</span></a></div></section>
        <section className="explainer-tour" aria-live="polite"><div className="explainer-tour-heading"><div><p className="eyebrow">A signal becomes a discovery candidate</p><h4>{explainerScenes[sceneIndex].title}</h4></div><span className="explainer-tour-count">{sceneIndex + 1} / {explainerScenes.length}</span></div><ExplainerVisual sceneIndex={sceneIndex} /><div className="explainer-tour-copy"><p>{explainerScenes[sceneIndex].body}</p><div className="explainer-tour-controls"><div className="explainer-scene-buttons" role="tablist" aria-label="How it works tour steps">{explainerScenes.map((scene, index) => <button key={scene.kicker} className={`explainer-scene-button ${index === sceneIndex ? "active" : ""}`} type="button" role="tab" aria-selected={index === sceneIndex} onClick={() => { setSceneIndex(index); setTourPlaying(false); }}><span>{String(index + 1).padStart(2, "0")}</span>{scene.kicker.split(" · ")[1]}</button>)}</div><div className="explainer-tour-actions"><button className="button button-quiet" type="button" onClick={() => setTourPlaying((current) => !current)}>{tourPlaying ? "Pause tour" : "Play tour"}</button><button className="button button-outline" type="button" onClick={() => { setSceneIndex((current) => (current + 1) % explainerScenes.length); setTourPlaying(false); }}>Next scene →</button></div></div></div></section>
        <div className="info-modal-note"><strong>How to read the dashboard</strong><p>Higher ranks indicate stronger comparative evidence. “Needs More Data” means the signal is still in play but cannot be judged yet; “Inconclusive” means the current evidence did not support further advancement. A Prime Candidate is a strong reason for follow-up—not scientific confirmation.</p></div>
      </div>
    </section>
  </div>;
}

export default function HomePage() {
  const [activeTab, setActiveTab] = useState<RankingView>("overall");
  const [identifiedOpen, setIdentifiedOpen] = useState(false);
  const [identifiedRefreshKey, setIdentifiedRefreshKey] = useState(0);
  const [howItWorksOpen, setHowItWorksOpen] = useState(false);
  const [opsAccessOpen, setOpsAccessOpen] = useState(false);
  const [opsPassword, setOpsPassword] = useState("");
  const [opsPasswordError, setOpsPasswordError] = useState(false);
  const [opsAccessLoading, setOpsAccessLoading] = useState(false);
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
  const [clockMs, setClockMs] = useState(() => Date.now());
  const dashboardRequestRef = useRef(0);
  const rankingCacheRef = useRef(new Map<string, RankingResponse>());
  const rankingCacheAtRef = useRef(new Map<string, number>());
  const rankingInflightRef = useRef(new Map<string, Promise<RankingResponse>>());
  const stagePrefetchInFlightRef = useRef(false);

  const closeCandidate = useCallback(() => {
    setSelectedId(null);
    setCandidate(null);
    setCurve(null);
    setHistory(null);
  }, []);

  const updateReplay = useCallback((view: RankingView, patch: Partial<ReplayState>) => {
    setReplayStates((current) => ({ ...current, [view]: { ...current[view], ...patch } }));
  }, []);

  const fetchRanking = useCallback((query: string) => {
    const inFlight = rankingInflightRef.current.get(query);
    if (inFlight) return inFlight;
    const request = fetchJson<RankingResponse>(`/api/rankings?${query}`)
      .finally(() => rankingInflightRef.current.delete(query));
    rankingInflightRef.current.set(query, request);
    return request;
  }, []);

  const loadDashboard = useCallback(async () => {
    const requestId = dashboardRequestRef.current + 1;
    dashboardRequestRef.current = requestId;
    const controls = rankingControls[activeTab];
    const query = new URLSearchParams({ view: activeTab, limit: "100", page: String(rankingPage) });
    if (activeTab === "overall" && controls.stage) query.set("stage_filter", controls.stage);
    if (activeTab !== "overall" && controls.status) query.set("status_filter", controls.status);
    if (controls.sort !== "rank") query.set("sort", controls.sort);
    const cacheKey = query.toString();
    const cachedRanking = rankingCacheRef.current.get(cacheKey);
    setLoading(!cachedRanking);
    if (cachedRanking) setRanking(cachedRanking);
    setError(null);
    try {
      const nextRanking = await fetchRanking(query.toString());
      if (requestId !== dashboardRequestRef.current) return;
      if (nextRanking.read_state === "publication_busy" && !nextRanking.served_from_cache) return;
      rankingCacheRef.current.set(cacheKey, nextRanking);
      rankingCacheAtRef.current.set(cacheKey, Date.now());
      setRanking(nextRanking);
    } catch (cause) {
      if (requestId !== dashboardRequestRef.current) return;
      setError(cause instanceof Error ? cause.message : "The observatory could not load published data.");
    } finally {
      if (requestId === dashboardRequestRef.current) setLoading(false);
    }
  }, [activeTab, fetchRanking, rankingControls, rankingPage]);

  const loadStatus = useCallback(async () => {
    try {
      const nextStatus = await fetchJson<DashboardStatus>("/api/status");
      setStatus(nextStatus);
    } catch (cause) {
      setError((current) => current ?? (cause instanceof Error ? cause.message : "The observatory could not load published status."));
    }
  }, []);

  const prefetchStageRankings = useCallback(async () => {
    if (stagePrefetchInFlightRef.current) return;
    stagePrefetchInFlightRef.current = true;
    try {
      for (const view of ["stage1", "stage2", "stage3"] as const) {
        const query = new URLSearchParams({ view, limit: "100", page: "1" }).toString();
        const cachedAt = rankingCacheAtRef.current.get(query) ?? 0;
        if (Date.now() - cachedAt < 30_000) continue;
        try {
          const nextRanking = await fetchRanking(query);
          rankingCacheRef.current.set(query, nextRanking);
          rankingCacheAtRef.current.set(query, Date.now());
        } catch {
          // A background warm-up must never replace a working overall view or
          // surface an error while the user is browsing the dashboard.
        }
      }
    } finally {
      stagePrefetchInFlightRef.current = false;
    }
  }, [fetchRanking]);

  useEffect(() => { void loadDashboard(); }, [loadDashboard]);

  useEffect(() => { void loadStatus(); }, [loadStatus]);

  useEffect(() => {
    if (activeTab === "overall" && ranking && !replayStates.overall.active) {
      void prefetchStageRankings();
    }
  }, [activeTab, prefetchStageRankings, ranking, replayStates.overall.active]);

  useEffect(() => {
    const clock = window.setInterval(() => setClockMs(Date.now()), 1000);
    return () => window.clearInterval(clock);
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") {
        void loadDashboard();
        void loadStatus();
      }
    }, 15000);
    return () => window.clearInterval(timer);
  }, [loadDashboard, loadStatus]);

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

  const liveRanking = ranking?.view === activeTab ? ranking : null;
  const displayRanking = activeReplay.active ? activeReplay.snapshot : liveRanking;
  const rows = displayRanking?.rows ?? [];
  const removals = activeReplay.active ? displayRanking?.removed ?? [] : [];
  const dataState = status?.data_version ? "Published" : "Awaiting data";
  const pipelineState = status?.data_version ? "Published" : status?.worker?.state === "idle" ? "Standby" : "Starting";
  const copy = stageCopy(activeTab);
  const totalCandidates = displayRanking?.total_ranked_count ?? rows.length;
  const processedCount = activeReplay.active ? displayRanking?.processed_target_count ?? 0 : status?.processed_target_count ?? ranking?.processed_target_count ?? 0;
  const targetCount = status?.total_target_count ?? ranking?.total_target_count ?? 126000;
  const totalStars = displayRanking?.total_star_count ?? new Set(rows.map((row) => row.target_id)).size;
  const filterActive = activeTab === "overall" ? activeControls.stage !== null : activeControls.status !== null;
  const panelTitle = loading && !liveRanking && !activeReplay.active ? "Loading published state" : totalCandidates ? `Total candidates ${totalCandidates.toLocaleString()} · Total stars ${totalStars.toLocaleString()}` : filterActive ? "No candidates match this filter" : "No candidates published yet";
  const remainingCount = Math.max(0, targetCount - processedCount);
  const activeCandidateCount = status?.active_candidate_count ?? rows.length;
  const footerState = status?.last_ingest_at ? `Last published: ${formatDate(status.last_ingest_at)}` : "Awaiting first publication";
  const nextUpdateAt = status?.worker.next_update_at ?? (status?.last_ingest_at ? new Date(new Date(status.last_ingest_at).getTime() + 60 * 1000).toISOString() : null);
  const nextUpdateLabel = countdownLabel(nextUpdateAt, clockMs);
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
    <header className="topbar"><div className="brand-lockup"><div className="brand-mark" aria-hidden="true">✦</div><div className="brand-title"><h1>Explore Potential Space Discoveries</h1><button className="about-trigger" type="button" aria-label="Learn how TransitScope works" aria-haspopup="dialog" aria-expanded={howItWorksOpen} onClick={() => setHowItWorksOpen(true)}><span aria-hidden="true">→</span></button></div></div><div className="topbar-actions"><span className="data-state"><span className={`status-dot ${status?.data_version ? "" : "muted"}`} /> {dataState}</span><button className="button button-quiet" type="button" onClick={() => void handleRefresh()} disabled={refreshing || loading}>↻ <span>{refreshing ? "Checking" : "Refresh"}</span></button><button className="button button-quiet tool-button" type="button" aria-label="Open ML Ops dashboard" title="ML Ops dashboard" onClick={() => { setOpsPassword(""); setOpsPasswordError(false); setOpsAccessOpen(true); }}>🔧</button></div></header>
    <nav className="tab-row" aria-label="Assessment rankings">{tabs.map((tab) => <button className={`tab-button ${!identifiedOpen && activeTab === tab.id ? "active" : ""}`} key={tab.id} onClick={() => { setIdentifiedOpen(false); setPipelineStatusOpen(false); setOpenColumnControl(null); setRankingPage(1); setActiveTab(tab.id); }} type="button">{tab.label}</button>)}<button className={`tab-button ${identifiedOpen ? "active" : ""}`} type="button" onClick={() => { closeCandidate(); exitReplay(activeTab); setPipelineStatusOpen(false); setOpenColumnControl(null); setIdentifiedOpen(true); }}>Identified</button></nav>
    {!identifiedOpen ? <>
    <section className="stage-heading"><div><div className="stage-kicker"><span className="stage-number">{copy.number}</span> {copy.kicker}</div><h3>{copy.title}</h3><p>{copy.description}</p></div>{activeReplay.active ? <div className="replay-dock-wrap"><div className="replay-dock" aria-label={`${copy.title} replay controls`} title={activeReplay.loading ? "Loading saved replay snapshot" : `Saved replay · ${activeReplay.snapshot?.occurred_at ? formatDate(activeReplay.snapshot.occurred_at) : "Beginning of assessment"}`}><button aria-label={activeReplay.playing ? "Pause replay" : "Play replay"} className="button button-quiet replay-play" type="button" onClick={() => updateReplay(activeTab, { playing: !activeReplay.playing })} disabled={activeReplay.maxSequence === 0}>{activeReplay.playing ? "Ⅱ" : "▶"}</button><label className="replay-speed"> <span>Speed</span><select aria-label="Replay speed" value={activeReplay.speed} onChange={(event) => updateReplay(activeTab, { speed: Number(event.target.value) })}><option value={0.5}>0.5×</option><option value={1}>1×</option><option value={2}>2×</option><option value={4}>4×</option></select></label><input aria-label="Replay position" className="replay-range" type="range" min={0} max={activeReplay.maxSequence} value={activeReplay.cursor} onChange={(event) => void loadReplaySnapshot(activeTab, Number(event.target.value))} /><button className="button button-replay-active replay-exit" type="button" onClick={() => exitReplay(activeTab)}>Exit replay</button></div>{activeReplay.error && <p className="replay-error">{activeReplay.error}</p>}</div> : <div className="stage-actions"><button className="button button-outline" type="button" onClick={() => void startReplay(activeTab)}>Replay</button>{activeTab === "overall" && <button className="button button-outline" type="button" onClick={() => setPipelineStatusOpen((open) => !open)} aria-expanded={pipelineStatusOpen}>{pipelineStatusOpen ? "Hide pipeline status" : "Pipeline status"}</button>}</div>}</section>
    {activeTab === "overall" && pipelineStatusOpen && !activeReplay.active && <section className="metrics-grid" aria-label="Processing summary"><article className="metric-card"><span className="metric-label">Target universe</span><strong>{remainingCount.toLocaleString()} / {targetCount.toLocaleString()}</strong><span className="metric-detail">stars left in target universe exploration</span></article><article className="metric-card"><span className="metric-label">Processed</span><strong>{processedCount.toLocaleString()}</strong><span className="metric-detail">targets with published evaluations</span></article><article className="metric-card"><span className="metric-label">Active candidates</span><strong>{activeCandidateCount.toLocaleString()}</strong><span className="metric-detail">currently visible in this assessment</span></article><article className="metric-card metric-card-status"><span className="metric-label">Pipeline status</span><strong><span className={`status-dot ${status?.data_version ? "" : "muted"}`} /> {pipelineState}</strong><span className="metric-detail">{footerState}</span><span className="metric-detail">Next update: {nextUpdateLabel} · {nextUpdateAt ? formatDate(nextUpdateAt) : "Calculating…"}</span></article></section>}
    <section className="ranking-panel" aria-labelledby="ranking-title"><div className="panel-header"><div><p className="eyebrow">Top candidates · {activeReplay.active ? "saved replay" : "live ranking"}</p><h3 id="ranking-title">{panelTitle}</h3></div><span className="panel-meta">Top {displayCap.toLocaleString()} displayed</span></div>{error ? <div className="error-state" role="alert"><span className="status-dot muted" /><h4>Published state unavailable</h4><p>{error}</p><button className="button button-outline" type="button" onClick={() => void loadDashboard()}>Try again</button></div> : activeReplay.loading && !activeReplay.snapshot ? <div className="empty-state" role="status"><div className="loading-ring" aria-hidden="true" /><h4>Reading saved replay state.</h4><p>The replay is reconstructing this view from persisted ranking snapshots. No model is run during replay.</p></div> : loading && !liveRanking && !activeReplay.active ? <div className="empty-state" role="status"><div className="loading-ring" aria-hidden="true" /><h4>Reading the latest published state.</h4><p>The API is checking the worker’s durable publication boundary. No model is run during a dashboard load.</p></div> : displayRanking ? <div className="table-wrap"><div className="ranking-table ranking-table-header"><span>Rank</span><span>Candidate</span><RankingColumnControl label={statusControlLabel} active={statusControlActive} open={openColumnControl === "status"} ariaLabel={activeTab === "overall" ? "Filter overall ranking by stage" : "Filter ranking by status"} options={statusControlOptions} onToggle={() => setOpenColumnControl((current) => current === "status" ? null : "status")} onSelect={(value) => activeTab === "overall" ? updateRankingControls({ stage: value as AssessmentStage }) : updateRankingControls({ status: value as CandidateStatus })} onClear={() => activeTab === "overall" ? updateRankingControls({ stage: null }) : updateRankingControls({ status: null })} /><span>Current activity</span><span>Movement</span><RankingColumnControl label={assessmentSortLabel} active={activeControls.sort !== "rank"} open={openColumnControl === "assessed"} ariaLabel="Sort ranking by assessment time" options={[{ value: "last_assessed_desc", label: "Most recently assessed" }, { value: "last_assessed_asc", label: "Oldest assessed first" }]} onToggle={() => setOpenColumnControl((current) => current === "assessed" ? null : "assessed")} onSelect={(value) => updateRankingControls({ sort: value as AssessmentSort })} onClear={() => updateRankingControls({ sort: "rank" })} /></div><div className="ranking-rows">{removals.map((removal) => <div className="ranking-table ranking-table-row replay-removal-row" key={`removed-${displayRanking?.sequence_number ?? 0}-${removal.candidate_id}`} role="status" aria-label={`${candidateDisplayLabel(removal.target_id, removal.candidate_id, undefined, removal.signal_index)} removed as already known`}><span className="rank-value">{removal.rank ?? "—"}</span><span className="candidate-cell"><strong>{candidateDisplayLabel(removal.target_id, removal.candidate_id, undefined, removal.signal_index)}</strong></span><span><span className="stage-status-stage">{stageName(removal.stage)}</span>{" "}<span className="status-pill status-known-match">Already Known</span><small className="score-label">{removal.known_match_id ?? "Catalog match"}</small></span><span className="activity-cell"><span className="activity-pulse removal-pulse" />{removal.activity}</span><span className="movement movement-removed">→ Removed</span><span className="last-update">{formatDate(removal.occurred_at)}</span></div>)}{rows.map((row) => { const resolutionLabel = signalResolutionLabel(row.signal_resolution); const rowStatus = displayStatus(row.status, row.stage); return <button className="ranking-table ranking-table-row" key={row.candidate_id} type="button" onClick={() => void openCandidate(row.candidate_id)} aria-label={`Open details for ${candidateDisplayLabel(row.target_id, row.candidate_id, undefined, row.signal_index)}`}><span className="rank-value">{row.rank}</span><span className="candidate-cell"><strong>{candidateDisplayLabel(row.target_id, row.candidate_id, undefined, row.signal_index)}</strong>{resolutionLabel && <small className="signal-resolution-note">{resolutionLabel}</small>}</span><span><span className="stage-status-stage">{stageName(row.stage)}</span>{" "}<span className={`status-pill status-${statusClass(rowStatus)}`}>{rowStatus}</span></span><span className="activity-cell"><span className="activity-pulse" />{row.activity}</span><span className={`movement movement-${row.movement}`}>{movementLabel(row)}</span><span className="last-update">{formatDate(row.last_evaluated_at)}</span></button>; })}{!rows.length && !removals.length && <div className="filtered-empty-state" role="status"><span className="status-dot muted" aria-hidden="true" /><strong>No candidates match this selection.</strong><span>Try another status or clear the filter to return to the full ranking.</span></div>}</div>{pageCount > 1 && <nav className="ranking-pages" aria-label="Ranking pages"><span>Showing page {displayRanking?.page ?? rankingPage} of {pageCount}</span><div>{Array.from({ length: pageCount }, (_, index) => index + 1).map((page) => <button className={`page-button ${(displayRanking?.page ?? rankingPage) === page ? "active" : ""}`} key={page} type="button" onClick={() => selectRankingPage(page)}>{page}</button>)}</div></nav>}</div> : <div className="empty-state" role="status"><div className="empty-orbit" aria-hidden="true"><span /></div><h4>{activeReplay.active && activeReplay.cursor === 0 ? "The replay begins before the first candidate." : "The observatory is waiting for its first observation."}</h4><p>{activeReplay.active ? "Move the replay forward to watch candidates enter, change rank, advance, or leave the active pool." : "The ranking will populate when a MAST/TESS evaluation is published. Empty is a real state here—not a hidden placeholder result."}</p><div className="empty-state-meta"><span className="status-dot muted" /> No current activity</div></div>}</section>
    </> : <IdentifiedPanel onOpenCandidate={(candidateId) => void openCandidate(candidateId)} refreshKey={identifiedRefreshKey} />}
    <footer className="footer-note"><span className="footer-pulse" aria-hidden="true" /><span>Data source: MAST / TESS · {footerState} · Evidence is provisional</span></footer>
    {modal}
    {howItWorksOpen && <HowItWorksModal onClose={() => setHowItWorksOpen(false)} />}
    {opsAccessOpen && <div className="modal-backdrop" role="presentation" onClick={() => setOpsAccessOpen(false)}><section className="candidate-modal access-modal" role="dialog" aria-modal="true" aria-labelledby="ops-access-title" onClick={(event) => event.stopPropagation()}><header className="modal-header"><div><p className="eyebrow accent">Private monitoring view</p><h3 id="ops-access-title">Open ML Ops</h3><p className="modal-subtitle">Enter the monitor password to open worker controls and pipeline telemetry.</p></div><button className="modal-close" type="button" aria-label="Close ML Ops access dialog" onClick={() => setOpsAccessOpen(false)}>×</button></header><form className="access-form" onSubmit={async (event) => { event.preventDefault(); setOpsAccessLoading(true); setOpsPasswordError(false); try { const response = await fetch("/ops/session", { method: "POST", headers: { "content-type": "application/json", accept: "application/json" }, body: JSON.stringify({ password: opsPassword }) }); if (!response.ok) { setOpsPasswordError(true); return; } window.location.assign("/ops"); } catch { setOpsPasswordError(true); } finally { setOpsAccessLoading(false); } }}><label className="access-label" htmlFor="ops-password">Password</label><input className="access-input" id="ops-password" type="password" autoFocus value={opsPassword} onChange={(event) => { setOpsPassword(event.target.value); setOpsPasswordError(false); }} /><p className={`access-error ${opsPasswordError ? "" : "access-error-hidden"}`} role={opsPasswordError ? "alert" : undefined}>Incorrect password.</p><div className="access-actions"><button className="button button-outline" type="button" onClick={() => setOpsAccessOpen(false)}>Cancel</button><button className="button button-quiet" type="submit" disabled={opsAccessLoading}>{opsAccessLoading ? "Checking…" : "Open dashboard"}</button></div></form></section></div>}
  </main>;
}
