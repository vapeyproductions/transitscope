const architectureSteps = [
  { label: "MAST / TESS", tone: "cyan" },
  { label: "Features", tone: "blue" },
  { label: "Stages 1–3", tone: "purple" },
  { label: "DuckDB + snapshots", tone: "gold" },
  { label: "Ops tracking", tone: "green" },
];

export default function PortfolioBrief() {
  return <section className="ops-architecture-strip" id="portfolio-architecture" aria-labelledby="ops-architecture-title">
    <div className="ops-architecture-heading">
      <p className="eyebrow accent" id="ops-architecture-title">Pipeline architecture</p>
      <span>Source → processing → tracking</span>
    </div>
    <div className="ops-architecture-flow" role="img" aria-label="MAST and TESS data flows through feature extraction, three assessment stages, persisted snapshots, and operations tracking">
      {architectureSteps.map((step, index) => <div className="ops-architecture-flow-item" key={step.label}>
        <span className={`ops-architecture-node ${step.tone}`}>{step.label}</span>
        {index < architectureSteps.length - 1 && <span className="ops-architecture-arrow" aria-hidden="true">→</span>}
      </div>)}
    </div>
  </section>;
}
