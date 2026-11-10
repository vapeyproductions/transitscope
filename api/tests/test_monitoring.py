import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from app.store import ReadOnlyStore


class MonitoringStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database = Path(self.temp_dir.name) / "transitscope.duckdb"
        connection = duckdb.connect(str(database))
        connection.execute("CREATE TABLE publish_versions (version BIGINT, status VARCHAR)")
        connection.execute(
            """
            CREATE TABLE target_manifest (
                target_id VARCHAR, provider_target_id VARCHAR, ra_deg DOUBLE,
                dec_deg DOUBLE, selection_bucket VARCHAR, manifest_status VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE candidates (
                candidate_id VARCHAR, target_id VARCHAR, current_stage VARCHAR,
                status VARCHAR, overall_score DOUBLE, stage_score DOUBLE,
                active BOOLEAN, processing_slot_occupied BOOLEAN,
                last_evaluated_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE evaluations (
                stage VARCHAR, model_version VARCHAR, data_sufficiency VARCHAR,
                classification VARCHAR, feature_json VARCHAR, metric_json VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE processing_checkpoints (
                stage VARCHAR, cursor BIGINT, universe_count BIGINT,
                processed_count BIGINT, selection_policy VARCHAR,
                adaptive_sampling_enabled BOOLEAN, updated_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE ingest_runs (
                provider VARCHAR, status VARCHAR, products_downloaded INTEGER,
                products_failed INTEGER, finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE coverage_watermarks (
                candidate_id VARCHAR, product_count INTEGER, window_count INTEGER,
                updated_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE batch_runs (
                batch_id VARCHAR, stage VARCHAR, selection_policy VARCHAR,
                requested_count INTEGER, selected_count INTEGER, evaluated_count INTEGER,
                failed_count INTEGER, status VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE pipeline_events (
                sequence_number BIGINT, stage VARCHAR, event_type VARCHAR,
                occurred_at TIMESTAMP, model_run_id VARCHAR
            )
            """
        )
        now = datetime.now(timezone.utc)
        connection.execute("INSERT INTO publish_versions VALUES (7, 'published')")
        connection.executemany(
            "INSERT INTO target_manifest VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("TIC 1", "1", 10.0, -20.0, "cell-a", "processed"),
                ("TIC 2", "2", 11.0, -20.0, "cell-a", "queued"),
                ("TIC 3", "3", 110.0, 20.0, "cell-b", "processed"),
            ],
        )
        connection.executemany(
            "INSERT INTO candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("c1", "TIC 1", "stage1", "Promising", 80.0, 80.0, True, True, now),
                ("c2", "TIC 3", "stage2", "Needs More Data", 55.0, 55.0, True, True, now),
            ],
        )
        connection.execute(
            "INSERT INTO evaluations VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                "stage3",
                "stage3-holdout-v1",
                "sufficient",
                "Promising",
                json.dumps({"holdout_metrics": {"f1": 0.8}, "model_vs_baseline_f1_delta": 0.2}),
                json.dumps({"evidence_score": 0.7}),
                now,
            ],
        )
        connection.execute(
            "INSERT INTO processing_checkpoints VALUES (?, ?, ?, ?, ?, ?, ?)",
            ["stage1", 2, 126000, 2, "adaptive", True, now],
        )
        connection.execute("INSERT INTO ingest_runs VALUES (?, ?, ?, ?, ?)", ["MAST", "success", 3, 0, now])
        connection.execute("INSERT INTO coverage_watermarks VALUES (?, ?, ?, ?)", ["c2", 3, 2, now])
        connection.close()
        self.store = ReadOnlyStore(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_monitoring_is_read_only_and_reports_pause_gate_and_sampling(self) -> None:
        previous = os.environ.get("RUN_LIVE_PIPELINE")
        os.environ["RUN_LIVE_PIPELINE"] = "false"
        try:
            response = self.store.monitoring({"state": "paused_by_config"})
        finally:
            if previous is None:
                os.environ.pop("RUN_LIVE_PIPELINE", None)
            else:
                os.environ["RUN_LIVE_PIPELINE"] = previous

        self.assertTrue(response["control"]["pipeline_paused"])
        self.assertEqual(response["data_version"], 7)
        self.assertEqual(response["universe"]["processed_count"], 2)
        self.assertEqual(response["stages"]["stage1"]["counts"]["Promising"], 1)
        self.assertEqual(response["sampling"]["cursor"], 2)
        self.assertEqual(response["sampling"]["buckets"][0]["bucket"], "cell-a")
        self.assertEqual(response["models"][0]["mean_holdout_f1"], 0.8)

    def test_status_keeps_published_count_with_worker_ahead(self) -> None:
        previous = os.environ.get("TARGET_UNIVERSE_SIZE")
        os.environ["TARGET_UNIVERSE_SIZE"] = "100000"
        try:
            response = self.store.status(
                {
                    "state": "idle",
                    "last_stage1_published_count": 20,
                }
            )
        finally:
            if previous is None:
                os.environ.pop("TARGET_UNIVERSE_SIZE", None)
            else:
                os.environ["TARGET_UNIVERSE_SIZE"] = previous

        self.assertEqual(response["data_version"], 7)
        self.assertEqual(response["processed_target_count"], 2)
        self.assertEqual(response["total_target_count"], 100000)
        self.assertNotEqual(response.get("read_state"), "worker_progress_ahead")
        self.assertEqual(response["active_candidate_count"], 2)
        self.assertEqual(response["active_star_count"], 2)
        self.assertEqual(response["stage_summary"]["stage1"]["retained_candidates"], 1)
        self.assertEqual(response["stage_summary"]["stage1"]["status_counts"]["Promising"], 1)

    def test_status_does_not_wait_for_duckdb_during_active_cycle(self) -> None:
        response = self.store.status(
            {
                "state": "processing_stage2",
                "last_stage1_published_count": 20,
            }
        )

        self.assertEqual(response["processed_target_count"], 20)
        self.assertEqual(response["read_state"], "publication_busy")
        self.assertTrue(response["served_from_cache"])

    def test_fast_status_keeps_worker_snapshot_separate_from_public_counts(self) -> None:
        store = ReadOnlyStore(Path(self.temp_dir.name) / "missing-data-dir")
        response = store.fast_status(
            {
                "state": "processing_stage2",
                "last_stage1_published_count": 42,
            }
        )

        self.assertIsNone(response["processed_target_count"])
        self.assertEqual(response["worker_processed_target_count"], 42)
        self.assertIsNone(response["publication_lag"]["processed_targets"])
        self.assertEqual(response["read_state"], "worker_snapshot")
        self.assertFalse(response["served_from_cache"])


if __name__ == "__main__":
    unittest.main()
