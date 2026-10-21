import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from app.store import ReadOnlyStore


class ReplayStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database = Path(self.temp_dir.name) / "transitscope.duckdb"
        connection = duckdb.connect(str(database))
        connection.execute("CREATE TABLE publish_versions (version BIGINT, status VARCHAR)")
        connection.execute("CREATE TABLE candidates (candidate_id VARCHAR, target_id VARCHAR, current_stage VARCHAR, status VARCHAR, overall_score DOUBLE, stage_score DOUBLE, rank_overall INTEGER, rank_stage INTEGER, activity VARCHAR, rank_delta INTEGER, movement_kind VARCHAR, last_evaluated_at TIMESTAMP, known_match_id VARCHAR, active BOOLEAN)")
        connection.execute("CREATE TABLE pipeline_events (event_id VARCHAR, sequence_number BIGINT, candidate_id VARCHAR, stage VARCHAR, event_type VARCHAR, payload_json VARCHAR, occurred_at TIMESTAMP, model_run_id VARCHAR)")
        connection.execute("CREATE TABLE ranking_snapshots (snapshot_id VARCHAR, view VARCHAR, sequence_number BIGINT, occurred_at TIMESTAMP, candidate_order_json VARCHAR, active_candidate_count INTEGER, processed_target_count INTEGER)")
        connection.execute("INSERT INTO publish_versions VALUES (3, 'published')")
        connection.execute("INSERT INTO candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ["c1", "TIC 1", "stage1", "Known Match", 0.9, 0.9, None, None, "Known match", 0, "falling", datetime.now(timezone.utc), "KIC 1", False])
        connection.execute("INSERT INTO candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ["c2", "TIC 2", "stage1", "Promising", 0.8, 0.8, 1, 1, "No current activity", 1, "rising", datetime.now(timezone.utc), None, True])
        events = [
            ("e1", 1, "c1", "stage1", "stage1_evaluated", json.dumps({"classification": "Promising", "evidence_score": 0.9}), datetime(2026, 1, 1), "run-1"),
            ("e2", 1, "c2", "stage1", "stage1_evaluated", json.dumps({"classification": "Needs More Data", "evidence_score": 0.4}), datetime(2026, 1, 1), "run-1"),
            ("e3", 2, "c2", "stage1", "stage1_evaluated", json.dumps({"classification": "Promising", "evidence_score": 0.8}), datetime(2026, 1, 2), "run-2"),
            ("e4", 3, "c1", "stage1", "known_match", json.dumps({"catalog_id": "KIC 1"}), datetime(2026, 1, 3), "catalog-1"),
        ]
        connection.executemany("INSERT INTO pipeline_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)", events)
        snapshots = [
            ("s1", "stage1", 1, datetime(2026, 1, 1), json.dumps(["c1", "c2"]), 2, 2),
            ("s2", "stage1", 2, datetime(2026, 1, 2), json.dumps(["c2", "c1"]), 2, 2),
            ("s3", "stage1", 3, datetime(2026, 1, 3), json.dumps(["c2"]), 1, 2),
        ]
        connection.executemany("INSERT INTO ranking_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)", snapshots)
        connection.close()
        self.store = ReadOnlyStore(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_replay_is_deterministic_and_preserves_known_match_removal(self) -> None:
        meta = self.store.replay_meta("stage1")
        self.assertEqual(meta["first_sequence"], 1)
        self.assertEqual(meta["last_sequence"], 3)
        self.assertEqual(meta["available_sequences"], [1, 2, 3])

        first = self.store.replay_snapshot("stage1", 1, 100)
        second = self.store.replay_snapshot("stage1", 1, 100)
        self.assertEqual(first, second)
        self.assertEqual([row["candidate_id"] for row in first["rows"]], ["c1", "c2"])
        self.assertEqual(first["rows"][0]["status"], "Promising")
        third = self.store.replay_snapshot("stage1", 3, 100)
        self.assertEqual(third["rows"][0]["candidate_id"], "c2")
        self.assertEqual(third["removed"][0]["candidate_id"], "c1")
        self.assertEqual(third["removed"][0]["rank"], 2)
        self.assertEqual(third["removed"][0]["removal_reason"], "Already Known")

        events = self.store.replay_events("stage1", 1, 3)["events"]
        self.assertEqual([event["event_type"] for event in events], ["stage1_evaluated", "stage1_evaluated", "stage1_evaluated", "known_match"])
        connection = duckdb.connect(str(self.store.database), read_only=True)
        self.assertEqual(connection.execute("SELECT COUNT(*) FROM ranking_snapshots").fetchone()[0], 3)
        connection.close()


if __name__ == "__main__":
    unittest.main()
