import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from batching import (
    BatchConfig,
    classify_signal_relationships,
    generate_target_universe,
    publish_stage1_batch,
    run_batch,
    select_next_targets,
    signal_alias_comparison,
)
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


class BatchingTests(unittest.TestCase):
    def test_phase_shifted_siblings_are_held_without_being_deleted(self) -> None:
        first = {
            "target": {"target_id": "TIC 10935379"},
            "result": {
                "candidate_id": "candidate-signal-1",
                "target_id": "TIC 10935379",
                "status": "Promising",
                "evidence_score": 83.0,
                "features": {
                    "period_days": 2.08147,
                    "transit_time": 1411.50,
                    "duration_hours": 4.0,
                    "depth": 0.014,
                },
            },
        }
        second = {
            "target": {"target_id": "TIC 10935379"},
            "result": {
                "candidate_id": "candidate-signal-2",
                "target_id": "TIC 10935379",
                "status": "Promising",
                "evidence_score": 82.0,
                "features": {
                    "period_days": 2.08147,
                    "transit_time": 1411.17,
                    "duration_hours": 4.0,
                    "depth": 0.0135,
                },
            },
        }

        comparison = signal_alias_comparison(first["result"], second["result"])
        self.assertIsNotNone(comparison)
        grouped, alias_events = classify_signal_relationships([first, second])
        by_id = {item["result"]["candidate_id"]: item["result"] for item in grouped}
        primary = by_id["candidate-signal-1"]
        sibling = by_id["candidate-signal-2"]
        self.assertEqual(primary["signal_resolution"], "primary")
        self.assertTrue(primary["promotion_eligible"])
        self.assertEqual(sibling["signal_resolution"], "pending_alias_review")
        self.assertFalse(sibling["promotion_eligible"])
        self.assertFalse(sibling["processing_slot_occupied"])
        self.assertEqual(sibling["alias_of_candidate_id"], primary["candidate_id"])
        self.assertEqual(len(alias_events), 1)

    def test_universe_generation_is_deterministic_and_stratified(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "targets.jsonl"
            count = generate_target_universe(
                path,
                universe_size=6,
                seed_rows=[{"target_id": "TIC 1", "provider_target_id": "1"}],
            )
            rows = [json.loads(line) for line in path.read_text().splitlines()]

            self.assertEqual(count, 6)
            self.assertEqual(len(rows), 6)
            self.assertEqual(rows[0]["target_id"], "TIC 1")
            self.assertEqual(rows[0]["selection_bucket"], "stratum-00")
            self.assertEqual(rows[1]["selection_bucket"], "stratum-01")
            self.assertEqual(rows[-1]["manifest_status"], "queued")

    def test_selection_advances_cursor_and_skips_completed_targets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "targets.jsonl"
            generate_target_universe(path, universe_size=4)
            selected, cursor = select_next_targets(
                path,
                cursor=0,
                count=2,
                processed_ids={"queued-target-000000000"},
            )

            self.assertEqual(cursor, 3)
            self.assertEqual(
                [row["target_id"] for row in selected],
                ["queued-target-000000001", "queued-target-000000002"],
            )

    def test_batch_failure_is_checkpointed_without_losing_the_batch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = DataPaths(root)
            paths.ensure()
            manifest = paths.manifests / "targets.jsonl"
            generate_target_universe(manifest, universe_size=3)

            result = run_batch(
                data_dir=root,
                manifest_path=manifest,
                config=BatchConfig(batch_size=2, max_workers=2),
            )

            self.assertEqual(result["status"], "published")
            self.assertEqual(result["failed_count"], 2)
            self.assertEqual(result["cursor"], 2)
            with DuckDBStore(paths.database).connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT cursor FROM processing_checkpoints WHERE stage = 'stage1'"
                    ).fetchone()[0],
                    2,
                )
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM batch_runs").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM pipeline_events").fetchone()[0], 2)

            resumed = run_batch(
                data_dir=root,
                manifest_path=manifest,
                config=BatchConfig(batch_size=2, max_workers=1),
            )
            self.assertEqual(resumed["cursor"], 3)
            with DuckDBStore(paths.database).connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM pipeline_events").fetchone()[0], 3)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM batch_runs").fetchone()[0], 2)

    def test_adaptive_sampling_is_explicitly_gated(self) -> None:
        with self.assertRaisesRegex(ValueError, "adaptive sampling"):
            BatchConfig(adaptive_sampling=True)

    def test_inconclusive_candidate_stays_rankable_but_releases_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = DataPaths(root)
            paths.ensure()
            manifest = paths.manifests / "targets.jsonl"
            target = {
                "target_id": "target-inconclusive",
                "provider_target_id": "target-inconclusive",
                "selection_bucket": "stratum-00",
                "manifest_status": "queued",
            }
            manifest.write_text(json.dumps(target) + "\n")
            evaluated_at = datetime.now(timezone.utc)
            store = DuckDBStore(paths.database)
            publish_stage1_batch(
                store,
                config=BatchConfig(batch_size=1, top_n=500, target_pool_size=2000),
                batch_id="stage1-test-inconclusive",
                manifest_path=manifest,
                universe_count=1,
                cursor=1,
                processed_count=1,
                selected=[target],
                results=[
                    {
                        "target": target,
                        "result": {
                            "candidate_id": "candidate-inconclusive",
                            "target_id": target["target_id"],
                            "status": "Inconclusive",
                            "evidence_score": 41.0,
                            "activity": "No current activity",
                            "evaluated_at": evaluated_at,
                            "model_version": "test-stage1",
                            "product_id": "product-1",
                            "features": {"period_days": 3.0},
                            "normalized_path": None,
                            "window_start": 0.0,
                            "window_end": 27.0,
                            "header_metadata": {},
                            "data_sufficiency": "sufficient",
                        }
                    }
                ],
                failures=[],
            )
            with store.connect() as connection:
                row = connection.execute(
                    "SELECT active, processing_slot_occupied FROM candidates WHERE candidate_id = ?",
                    ["candidate-inconclusive"],
                ).fetchone()
            self.assertEqual(row, (True, False))

    def test_retiring_a_candidate_opens_a_replacement_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = DataPaths(root)
            paths.ensure()
            manifest = paths.manifests / "targets.jsonl"
            rows = [
                {
                    "target_id": "target-1",
                    "provider_target_id": "target-1",
                    "selection_bucket": "stratum-00",
                    "manifest_status": "queued",
                },
                {
                    "target_id": "queued-target-000000001",
                    "provider_target_id": "queued-target-000000001",
                    "selection_bucket": "stratum-01",
                    "manifest_status": "queued",
                },
                {
                    "target_id": "queued-target-000000002",
                    "provider_target_id": "queued-target-000000002",
                    "selection_bucket": "stratum-02",
                    "manifest_status": "queued",
                },
            ]
            manifest.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
            occurred_at = datetime.now(timezone.utc)
            store = DuckDBStore(paths.database)
            store.publish_batch(
                version=1,
                targets=[
                    {
                        "target_id": "target-1",
                        "provider_target_id": "target-1",
                        "ra_deg": None,
                        "dec_deg": None,
                        "sector": None,
                        "cadence": None,
                        "source_product_family": None,
                        "selection_bucket": "stratum-00",
                        "manifest_status": "processed",
                    }
                ],
                candidates=[
                    {
                        "candidate_id": "stage1-candidate-1",
                        "target_id": "target-1",
                        "current_stage": "stage1",
                        "status": "Promising",
                        "overall_score": 80.0,
                        "stage_score": 80.0,
                        "rank_overall": 1,
                        "rank_stage": 1,
                        "activity": "No current activity",
                        "rank_delta": 0,
                        "movement_kind": "new",
                        "last_evaluated_at": occurred_at,
                        "known_match_id": None,
                        "active": True,
                    }
                ],
                snapshots=[
                    {
                        "snapshot_id": "snapshot-1",
                        "view": "stage1",
                        "sequence_number": 1,
                        "occurred_at": occurred_at,
                        "candidate_order_json": ["stage1-candidate-1"],
                        "active_candidate_count": 1,
                        "processed_target_count": 1,
                    }
                ],
            )

            result = run_batch(
                data_dir=root,
                manifest_path=manifest,
                config=BatchConfig(batch_size=1, max_workers=1),
                retire_candidate_ids=["stage1-candidate-1"],
            )

            self.assertEqual(result["replacement_slots"], 1)
            with store.connect() as connection:
                self.assertFalse(
                    connection.execute(
                        "SELECT active FROM candidates WHERE candidate_id = 'stage1-candidate-1'"
                    ).fetchone()[0]
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM pipeline_events WHERE event_type = 'stage1_released'"
                    ).fetchone()[0],
                    1,
                )


if __name__ == "__main__":
    unittest.main()
