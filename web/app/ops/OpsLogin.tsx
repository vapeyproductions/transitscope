"use client";

import { FormEvent, useState } from "react";

export default function OpsLogin() {
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const response = await fetch("/ops/session", {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify({ password }),
      });
      if (!response.ok) {
        setError("Incorrect password.");
        return;
      }
      window.location.reload();
    } catch {
      setError("The monitoring access service is unavailable.");
    } finally {
      setSubmitting(false);
    }
  }

  return <main className="observatory-shell ops-page-shell ops-login-shell">
    <section className="candidate-modal access-modal ops-login-card" role="dialog" aria-modal="true" aria-labelledby="ops-login-title">
      <header className="modal-header">
        <div>
          <p className="eyebrow accent">Private operations view</p>
          <h3 id="ops-login-title">Open ML Ops</h3>
          <p className="modal-subtitle">Enter the monitor password to view worker controls and pipeline telemetry.</p>
        </div>
      </header>
      <form className="access-form" onSubmit={submit}>
        <label className="access-label" htmlFor="ops-page-password">Password</label>
        <input className="access-input" id="ops-page-password" type="password" autoFocus value={password} onChange={(event) => setPassword(event.target.value)} />
        <p className={`access-error ${error ? "" : "access-error-hidden"}`} role={error ? "alert" : undefined}>{error ?? "Incorrect password."}</p>
        <div className="access-actions">
          <a className="button button-outline" href="/">← Observatory</a>
          <button className="button button-quiet" type="submit" disabled={submitting}>{submitting ? "Checking…" : "Open dashboard"}</button>
        </div>
      </form>
    </section>
  </main>;
}
