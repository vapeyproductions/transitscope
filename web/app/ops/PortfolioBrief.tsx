type PortfolioBriefProps = {
  publicView?: boolean;
};

const architectureSteps = [
  { label: "MAST / TESS", detail: "Provider adapters", tone: "cyan" },
  { label: "Feature layer", detail: "Light curves + signals", tone: "blue" },
  { label: "Stage workers", detail: "Bounded CPU jobs", tone: "purple" },
  { label: "Evidence store", detail: "DuckDB + snapshots", tone: "gold" },
  { label: "Replay + UI", detail: "Read-only observability", tone: "green" },
];

const modelCards = [
  {
    stage: "Stage 1 · detection",
    title: "Find transit-like signals",
    body: "An explainable Box Least Squares search scores periodic dimming candidates and records period, depth, duration, coverage, and data sufficiency.",
    outputs: "Evidence score · period · depth · sufficiency",
  },
  {
    stage: "Stage 2 · repeatability",
    title: "Test whether the pattern repeats",
    body: "Historical windows are compared against the Stage 1 signal so a candidate must survive longer-baseline coverage before receiving a deeper test.",
    outputs: "Recurrence · coverage · consistency · repeatability",
  },
  {
    stage: "Stage 3 · holdout",
    title: "Predict a chronological holdout",
    body: "Earlier windows train a phase-feature model and a transparent baseline; the final chronological window measures whether the signal generalizes.",
    outputs: "Precision · recall · F1 · event recovery · model lift",
  },
];

const lineageRows = [
  ["Input provenance", "MAST/TESS SPOC products plus NASA Exoplanet Archive cross-reference data"],
  ["Persisted state", "Target manifests, evaluations, events, model runs, ranking snapshots, and checkpoints"],
  ["Replay contract", "The UI reads saved snapshots and events; replay never reruns a model or fetches data"],
  ["Integrity rule", "Unavailable or uncomputed metrics stay visibly unavailable instead of becoming estimates"],
];

export default function PortfolioBrief({ publicView = false }: PortfolioBriefProps) {
  return <section className="portfolio-brief" id="portfolio-overview" aria-labelledby="portfolio-brief-title">
    <div className="portfolio-brief-header">
      <div>
        <p className="eyebrow accent">Engineering case study</p>
        <h2 id="portfolio-brief-title">From telescope pixels to a reproducible evidence pipeline</h2>
        <p className="portfolio-brief-lede">TransitScope is a production-shaped ML system for ranking transit-like stellar signals. It treats model output as evidence, keeps the waterfall observable, and makes every published state replayable.</p>
      </div>
      <div className="portfolio-brief-actions">
        <a className="button button-quiet" href="#portfolio-architecture">View architecture ↓</a>
        <span className="portfolio-public-badge"><span className="status-dot" />{publicView ? "Public read-only telemetry" : "Operator telemetry"}</span>
      </div>
    </div>

    <div className="portfolio-stat-strip" aria-label="TransitScope system characteristics">
      <div><strong>126,000</strong><span>target universe</span></div>
      <div><strong>3</strong><span>evidence stages</span></div>
      <div><strong>CPU-bounded</strong><span>parallel worker jobs</span></div>
      <div><strong>Snapshot-first</strong><span>deterministic replay</span></div>
    </div>

    <div className="portfolio-architecture" id="portfolio-architecture">
      <div className="portfolio-section-heading"><div><p className="eyebrow">System architecture</p><h3>One observable path from source data to a ranked decision</h3></div><span>Single-writer state · bounded work · persisted evidence</span></div>
      <div className="architecture-flow" role="img" aria-label="MAST and TESS data flows through feature extraction, staged workers, a persisted evidence store, and a replayable public interface">
        {architectureSteps.map((step, index) => <div className="architecture-flow-item" key={step.label}>
          <article className={`architecture-node ${step.tone}`}><span className="architecture-node-index">0{index + 1}</span><strong>{step.label}</strong><small>{step.detail}</small></article>
          {index < architectureSteps.length - 1 && <span className="architecture-arrow" aria-hidden="true">→</span>}
        </div>)}
      </div>
      <p className="architecture-caption">The worker is rate-limited and checkpointed so the dashboard never starts a job just because somebody opened a page. Provider latency, retries, queue pressure, and data freshness remain visible in the monitoring panels below.</p>
    </div>

    <div className="portfolio-detail-grid">
      <article className="portfolio-card" id="portfolio-model-cards">
        <div className="portfolio-section-heading"><div><p className="eyebrow">Model cards</p><h3>Each stage answers a different question</h3></div><a href="#top100-status-title">See live evidence ↗</a></div>
        <div className="portfolio-model-stack">{modelCards.map((card) => <article className="portfolio-model-card" key={card.stage}><span>{card.stage}</span><h4>{card.title}</h4><p>{card.body}</p><small>{card.outputs}</small></article>)}</div>
        <div className="portfolio-integrity-note"><strong>Scientific boundary</strong><span>A Prime Candidate is a high-priority investigation result, not scientific confirmation. Metrics are only meaningful when their data sufficiency and holdout status are recorded.</span></div>
      </article>

      <article className="portfolio-card" id="portfolio-lineage">
        <div className="portfolio-section-heading"><div><p className="eyebrow">Data lineage + reproducibility</p><h3>Every claim has a source and a state</h3></div><a href="#job-call-distribution-title">See dispatch ↗</a></div>
        <div className="portfolio-lineage-list">{lineageRows.map(([label, value]) => <div className="portfolio-lineage-row" key={label}><strong>{label}</strong><span>{value}</span></div>)}</div>
        <div className="portfolio-proof-links" aria-label="Live portfolio proof points">
          <a href="#cycle-activity-title"><span>01</span><strong>Queue motion</strong><small>Saved cycles and handoffs</small></a>
          <a href="#top100-status-title"><span>02</span><strong>Model behavior</strong><small>Status balance and dwell</small></a>
          <a href="#job-call-distribution-title"><span>03</span><strong>System health</strong><small>Dispatch and throughput</small></a>
        </div>
      </article>
    </div>
  </section>;
}
