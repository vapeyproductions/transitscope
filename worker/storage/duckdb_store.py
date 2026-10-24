import json
import fcntl
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import duckdb


SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json_value(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


class _LockedWriteConnection:
    """DuckDB connection proxy that holds the worker's exclusive sidecar lock."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, lock_file: Any) -> None:
        self._connection = connection
        self._lock_file = lock_file
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def __enter__(self) -> "_LockedWriteConnection":
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            self._connection.__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._connection.close()
        finally:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()


class DuckDBStore:
    """Single-writer store for durable pipeline state and replay history."""

    def __init__(self, database: Path) -> None:
        self.database = Path(database)

    def connect(self) -> Any:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.database.with_suffix(self.database.suffix + ".lock")
        lock_file = lock_path.open("a+")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            connection = duckdb.connect(str(self.database))
        except Exception:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
            raise
        return _LockedWriteConnection(connection, lock_file)

    def initialize(self) -> None:
        schema = SCHEMA_PATH.read_text()
        with self.connect() as connection:
            connection.execute(schema)
            # The worker database is a durable artifact and may predate the
            # queue-capacity policy. Keep the migration idempotent so a live
            # deployment can adopt the new state without losing replay data.
            connection.execute(
                "ALTER TABLE candidates ADD COLUMN IF NOT EXISTS processing_slot_occupied BOOLEAN DEFAULT true"
            )
            connection.execute(
                "ALTER TABLE candidates ADD COLUMN IF NOT EXISTS signal_id VARCHAR"
            )
            connection.execute(
                "ALTER TABLE candidates ADD COLUMN IF NOT EXISTS signal_group_id VARCHAR"
            )
            connection.execute(
                "ALTER TABLE candidates ADD COLUMN IF NOT EXISTS signal_resolution VARCHAR DEFAULT 'independent'"
            )
            connection.execute(
                "ALTER TABLE candidates ADD COLUMN IF NOT EXISTS alias_of_candidate_id VARCHAR"
            )
            connection.execute(
                "ALTER TABLE candidates ADD COLUMN IF NOT EXISTS promotion_eligible BOOLEAN DEFAULT true"
            )
            connection.execute(
                """
                UPDATE candidates
                SET processing_slot_occupied = CASE
                    WHEN active = false OR status IN ('Inconclusive', 'Known Match', 'Duplicate Signal') THEN false
                    WHEN signal_resolution IN ('pending_alias_review', 'pending_independence_review', 'duplicate_signal') THEN false
                    ELSE true
                END
                """
            )
            connection.execute(
                """
                UPDATE candidates
                SET signal_group_id = COALESCE(signal_group_id, candidate_id),
                    signal_resolution = COALESCE(signal_resolution, 'independent'),
                    promotion_eligible = CASE
                        WHEN active = false OR status IN ('Inconclusive', 'Known Match', 'Duplicate Signal') THEN false
                        WHEN signal_resolution IN ('pending_alias_review', 'pending_independence_review', 'duplicate_signal') THEN false
                        ELSE COALESCE(promotion_eligible, true)
                    END
                WHERE signal_group_id IS NULL
                   OR signal_resolution IS NULL
                   OR promotion_eligible IS NULL
                   OR status IN ('Inconclusive', 'Known Match', 'Duplicate Signal')
                   OR signal_resolution IN ('pending_alias_review', 'pending_independence_review', 'duplicate_signal')
                """
            )

    def record_pipeline_cycle(self, cycle: Mapping[str, object]) -> None:
        """Persist one completed live-worker cycle for the ML Ops timeline."""

        columns = (
            "cycle_id",
            "started_at",
            "finished_at",
            "status",
            "duration_seconds",
            "payload_json",
        )
        with self.connect() as connection:
            connection.execute(
                f"INSERT INTO pipeline_cycles ({', '.join(columns)}) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (cycle_id) DO UPDATE SET finished_at = excluded.finished_at, status = excluded.status, duration_seconds = excluded.duration_seconds, payload_json = excluded.payload_json",
                [
                    cycle.get("cycle_id"),
                    cycle.get("started_at"),
                    cycle.get("finished_at"),
                    cycle.get("status"),
                    cycle.get("duration_seconds", 0.0),
                    _json_value(cycle.get("payload", {})),
                ],
            )

    def publish_batch(
        self,
        *,
        version: int,
        targets: Sequence[Mapping[str, object]] = (),
        ingest_runs: Sequence[Mapping[str, object]] = (),
        candidates: Sequence[Mapping[str, object]] = (),
        signals: Sequence[Mapping[str, object]] = (),
        evaluations: Sequence[Mapping[str, object]] = (),
        events: Sequence[Mapping[str, object]] = (),
        snapshots: Sequence[Mapping[str, object]] = (),
        candidate_updates: Sequence[Mapping[str, object]] = (),
        coverage_watermarks: Sequence[Mapping[str, object]] = (),
        batch_run: Mapping[str, object] | None = None,
        checkpoint: Mapping[str, object] | None = None,
        note: str | None = None,
    ) -> None:
        """Write one consistent batch, publishing only after every row succeeds."""

        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(
                    """
                    INSERT INTO publish_versions (version, status, created_at, note)
                    VALUES (?, 'building', ?, ?)
                    ON CONFLICT (version) DO NOTHING
                    """,
                    [version, _now(), note],
                )

                self._insert_many(
                    connection,
                    "target_manifest",
                    (
                        "target_id",
                        "provider_target_id",
                        "ra_deg",
                        "dec_deg",
                        "sector",
                        "cadence",
                        "source_product_family",
                        "selection_bucket",
                        "manifest_status",
                    ),
                    targets,
                )
                self._insert_many(
                    connection,
                    "ingest_runs",
                    (
                        "run_id",
                        "provider",
                        "query_config_json",
                        "started_at",
                        "finished_at",
                        "status",
                        "targets_seen",
                        "products_downloaded",
                        "products_failed",
                        "error_json",
                    ),
                    ingest_runs,
                    json_columns={"query_config_json", "error_json"},
                )
                self._insert_many(
                    connection,
                    "candidates",
                    (
                        "candidate_id",
                        "signal_id",
                        "target_id",
                        "current_stage",
                        "status",
                        "overall_score",
                        "stage_score",
                        "rank_overall",
                        "rank_stage",
                        "activity",
                        "rank_delta",
                        "movement_kind",
                        "last_evaluated_at",
                        "known_match_id",
                        "active",
                        "signal_group_id",
                        "signal_resolution",
                        "alias_of_candidate_id",
                        "promotion_eligible",
                    ),
                    candidates,
                )
                self._upsert_signals(connection, signals)
                self._insert_many(
                    connection,
                    "evaluations",
                    (
                        "evaluation_id",
                        "candidate_id",
                        "stage",
                        "model_version",
                        "data_window_start",
                        "data_window_end",
                        "input_product_ids",
                        "feature_json",
                        "metric_json",
                        "classification",
                        "data_sufficiency",
                    ),
                    evaluations,
                    json_columns={"input_product_ids", "feature_json", "metric_json"},
                )
                self._insert_many(
                    connection,
                    "pipeline_events",
                    (
                        "event_id",
                        "sequence_number",
                        "candidate_id",
                        "stage",
                        "event_type",
                        "payload_json",
                        "occurred_at",
                        "model_run_id",
                    ),
                    events,
                    json_columns={"payload_json"},
                )
                self._insert_many(
                    connection,
                    "ranking_snapshots",
                    (
                        "snapshot_id",
                        "view",
                        "sequence_number",
                        "occurred_at",
                        "candidate_order_json",
                        "active_candidate_count",
                        "processed_target_count",
                    ),
                    snapshots,
                    json_columns={"candidate_order_json"},
                )

                self._upsert_coverage_watermarks(connection, coverage_watermarks)

                self._insert_batch_run(connection, batch_run)
                self._update_candidates(connection, candidate_updates)
                self._upsert_checkpoint(connection, checkpoint)

                connection.execute(
                    """
                    UPDATE publish_versions
                    SET status = 'published', published_at = ?
                    WHERE version = ?
                    """,
                    [_now(), version],
                )
                connection.execute("COMMIT")
            except Exception:
                # DuckDB may already have ended the transaction while
                # propagating a constraint/connection error. Do not replace
                # the useful original exception with a rollback failure.
                try:
                    connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    @staticmethod
    def _upsert_signals(
        connection: duckdb.DuckDBPyConnection,
        signals: Sequence[Mapping[str, object]],
    ) -> None:
        for signal in signals:
            connection.execute(
                """
                INSERT INTO candidate_signals (
                    signal_id, candidate_id, target_id, signal_index, signal_key,
                    period_days, transit_time, duration_hours, depth, snr,
                    extraction_model_version, first_seen_at, last_seen_at, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (signal_id) DO UPDATE SET
                    candidate_id = excluded.candidate_id,
                    target_id = excluded.target_id,
                    signal_index = excluded.signal_index,
                    signal_key = excluded.signal_key,
                    period_days = excluded.period_days,
                    transit_time = excluded.transit_time,
                    duration_hours = excluded.duration_hours,
                    depth = excluded.depth,
                    snr = excluded.snr,
                    extraction_model_version = excluded.extraction_model_version,
                    last_seen_at = excluded.last_seen_at,
                    active = excluded.active
                """,
                [
                    signal.get("signal_id"),
                    signal.get("candidate_id"),
                    signal.get("target_id"),
                    signal.get("signal_index"),
                    signal.get("signal_key"),
                    signal.get("period_days"),
                    signal.get("transit_time"),
                    signal.get("duration_hours"),
                    signal.get("depth"),
                    signal.get("snr"),
                    signal.get("extraction_model_version"),
                    signal.get("first_seen_at", _now()),
                    signal.get("last_seen_at", _now()),
                    signal.get("active", True),
                ],
            )

    @staticmethod
    def _insert_batch_run(
        connection: duckdb.DuckDBPyConnection,
        batch_run: Mapping[str, object] | None,
    ) -> None:
        if not batch_run:
            return
        columns = (
            "batch_id",
            "stage",
            "selection_policy",
            "requested_count",
            "selected_count",
            "evaluated_count",
            "failed_count",
            "max_workers",
            "status",
            "started_at",
            "finished_at",
            "error_json",
        )
        connection.execute(
            f"INSERT INTO batch_runs ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)}) ON CONFLICT DO NOTHING",
            [
                _json_value(batch_run[column]) if column == "error_json" else batch_run.get(column)
                for column in columns
            ],
        )

    @staticmethod
    def _update_candidates(
        connection: duckdb.DuckDBPyConnection,
        updates: Sequence[Mapping[str, object]],
    ) -> None:
        for update in updates:
            candidate_id = update.get("candidate_id")
            if not candidate_id:
                continue
            connection.execute(
                """
                UPDATE candidates
                SET current_stage = COALESCE(?, current_stage), signal_id = COALESCE(?, signal_id), rank_overall = ?, rank_stage = ?, rank_delta = ?, movement_kind = ?,
                    active = ?, activity = ?, status = ?, overall_score = ?, stage_score = ?,
                    last_evaluated_at = ?, known_match_id = ?,
                    processing_slot_occupied = COALESCE(?, processing_slot_occupied),
                    signal_group_id = COALESCE(?, signal_group_id),
                    signal_resolution = COALESCE(?, signal_resolution),
                    alias_of_candidate_id = ?,
                    promotion_eligible = COALESCE(?, promotion_eligible)
                WHERE candidate_id = ?
                """,
                [
                    update.get("current_stage"),
                    update.get("signal_id"),
                    update.get("rank_overall"),
                    update.get("rank_stage"),
                    update.get("rank_delta", 0),
                    update.get("movement_kind", "unchanged"),
                    update.get("active", True),
                    update.get("activity", "No current activity"),
                    update.get("status"),
                    update.get("overall_score"),
                    update.get("stage_score"),
                    update.get("last_evaluated_at"),
                    update.get("known_match_id"),
                    update.get("processing_slot_occupied"),
                    update.get("signal_group_id"),
                    update.get("signal_resolution"),
                    update.get("alias_of_candidate_id"),
                    update.get("promotion_eligible"),
                    candidate_id,
                ],
            )

    @staticmethod
    def _upsert_coverage_watermarks(
        connection: duckdb.DuckDBPyConnection,
        watermarks: Sequence[Mapping[str, object]],
    ) -> None:
        for watermark in watermarks:
            connection.execute(
                """
                INSERT INTO coverage_watermarks (
                    candidate_id, stage, coverage_signature, coverage_start,
                    coverage_end, product_count, window_count, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (candidate_id, stage) DO UPDATE SET
                    coverage_signature = excluded.coverage_signature,
                    coverage_start = excluded.coverage_start,
                    coverage_end = excluded.coverage_end,
                    product_count = excluded.product_count,
                    window_count = excluded.window_count,
                    updated_at = excluded.updated_at
                """,
                [
                    watermark.get("candidate_id"),
                    watermark.get("stage"),
                    watermark.get("coverage_signature"),
                    watermark.get("coverage_start"),
                    watermark.get("coverage_end"),
                    watermark.get("product_count", 0),
                    watermark.get("window_count", 0),
                    watermark.get("updated_at", _now()),
                ],
            )

    @staticmethod
    def _upsert_checkpoint(
        connection: duckdb.DuckDBPyConnection,
        checkpoint: Mapping[str, object] | None,
    ) -> None:
        if not checkpoint:
            return
        connection.execute(
            """
            INSERT INTO processing_checkpoints (
                stage, manifest_path, cursor, universe_count, processed_count,
                last_batch_id, selection_policy, adaptive_sampling_enabled, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stage) DO UPDATE SET
                manifest_path = excluded.manifest_path,
                cursor = excluded.cursor,
                universe_count = excluded.universe_count,
                processed_count = excluded.processed_count,
                last_batch_id = excluded.last_batch_id,
                selection_policy = excluded.selection_policy,
                adaptive_sampling_enabled = excluded.adaptive_sampling_enabled,
                updated_at = excluded.updated_at
            """,
            [
                checkpoint.get("stage"),
                checkpoint.get("manifest_path"),
                checkpoint.get("cursor"),
                checkpoint.get("universe_count"),
                checkpoint.get("processed_count"),
                checkpoint.get("last_batch_id"),
                checkpoint.get("selection_policy"),
                checkpoint.get("adaptive_sampling_enabled", False),
                checkpoint.get("updated_at"),
            ],
        )

    @staticmethod
    def _insert_many(
        connection: duckdb.DuckDBPyConnection,
        table: str,
        columns: Sequence[str],
        rows: Iterable[Mapping[str, object]],
        *,
        json_columns: set[str] | None = None,
    ) -> None:
        rows = list(rows)
        if not rows:
            return

        placeholders = ", ".join("?" for _ in columns)
        column_sql = ", ".join(columns)
        sql = (
            f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders}) "
            "ON CONFLICT DO NOTHING"
        )
        values = []
        for row in rows:
            values.append(
                [
                    _json_value(row.get(column)) if json_columns and column in json_columns else row.get(column)
                    for column in columns
                ]
            )
        connection.executemany(sql, values)

    def latest_published_version(self) -> int | None:
        self.initialize()
        with self.connect() as connection:
            result = connection.execute(
                """
                SELECT version
                FROM publish_versions
                WHERE status = 'published'
                ORDER BY version DESC
                LIMIT 1
                """
            ).fetchone()
        return int(result[0]) if result else None

