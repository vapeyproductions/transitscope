import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


class StorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.paths = DataPaths(Path(self.temp_dir.name))
        self.paths.ensure()
        self.store = DuckDBStore(self.paths.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_paths_create_expected_directories(self) -> None:
        self.assertTrue(self.paths.raw.is_dir())
        self.assertTrue(self.paths.normalized.is_dir())
        self.assertTrue(self.paths.manifests.is_dir())

    def test_publish_is_idempotent_and_visible_after_commit(self) -> None:
        occurred_at = datetime.now(timezone.utc)
        batch = {
            "version": 1,
            "targets": [
                {
                    "target_id": "tic-1",
                    "provider_target_id": "tic-1",
                    "ra_deg": 1.0,
                    "dec_deg": 2.0,
                    "sector": 1,
                    "cadence": "short",
                    "source_product_family": "probe",
                    "selection_bucket": "probe",
                    "manifest_status": "processed",
                }
            ],
            "candidates": [
                {
                    "candidate_id": "candidate-1",
                    "target_id": "tic-1",
                    "current_stage": "stage1",
                    "status": "Needs More Data",
                    "overall_score": 0.1,
                    "stage_score": 0.1,
                    "rank_overall": 1,
                    "rank_stage": 1,
                    "activity": "No current activity",
                    "rank_delta": 0,
                    "movement_kind": "unchanged",
                    "last_evaluated_at": occurred_at,
                    "known_match_id": None,
                    "active": True,
                }
            ],
            "events": [
                {
                    "event_id": "event-1",
                    "sequence_number": 1,
                    "candidate_id": "candidate-1",
                    "stage": "stage1",
                    "event_type": "discovered",
                    "payload_json": {"score": 0.1},
                    "occurred_at": occurred_at,
                    "model_run_id": "run-1",
                }
            ],
            "snapshots": [
                {
                    "snapshot_id": "snapshot-1",
                    "view": "stage1",
                    "sequence_number": 1,
                    "occurred_at": occurred_at,
                    "candidate_order_json": ["candidate-1"],
                    "active_candidate_count": 1,
                    "processed_target_count": 1,
                }
            ],
        }

        self.store.publish_batch(**batch)
        self.store.publish_batch(**batch)

        self.assertEqual(self.store.latest_published_version(), 1)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM pipeline_events").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM ranking_snapshots").fetchone()[0], 1)
            payload = connection.execute("SELECT payload_json FROM pipeline_events").fetchone()[0]
            self.assertEqual(json.loads(payload), {"score": 0.1})

    def test_failed_batch_does_not_publish_partial_state(self) -> None:
        with self.assertRaises(KeyError):
            self.store.publish_batch(
                version=2,
                events=[{"event_id": "missing-required-fields"}],
            )

        self.assertIsNone(self.store.latest_published_version())
        with self.store.connect() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM pipeline_events").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM publish_versions").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()

