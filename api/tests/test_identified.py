import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from app.store import ReadOnlyStore


class IdentifiedStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database = Path(self.temp_dir.name) / "transitscope.duckdb"
        connection = duckdb.connect(str(database))
        connection.execute("CREATE TABLE publish_versions (version BIGINT, status VARCHAR)")
        connection.execute(
            """
            CREATE TABLE candidates (
                candidate_id VARCHAR, target_id VARCHAR, current_stage VARCHAR,
                status VARCHAR, overall_score DOUBLE, stage_score DOUBLE,
                rank_overall INTEGER, rank_stage INTEGER, activity VARCHAR,
                rank_delta INTEGER, movement_kind VARCHAR, last_evaluated_at TIMESTAMP,
                known_match_id VARCHAR, active BOOLEAN
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE evaluations (
                evaluation_id VARCHAR, candidate_id VARCHAR, stage VARCHAR,
                model_version VARCHAR, data_window_start TIMESTAMP,
                data_window_end TIMESTAMP, input_product_ids VARCHAR,
                feature_json VARCHAR, metric_json VARCHAR, classification VARCHAR,
                data_sufficiency VARCHAR, created_at TIMESTAMP
            )
            """
        )
        connection.execute("INSERT INTO publish_versions VALUES (18, 'published')")
        now = datetime.now(timezone.utc)
        connection.execute(
            "INSERT INTO candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "candidate-known",
                "TIC 123",
                "stage2",
                "Known Match",
                68.5,
                55.2,
                None,
                None,
                "Mapped to known catalog object",
                -1,
                "falling",
                now,
                "42.01",
                False,
            ],
        )
        evaluations = [
            (
                "eval-stage1",
                "candidate-known",
                "stage1",
                "stage1-bls-v1",
                json.dumps([]),
                json.dumps({"coverage_days": 24.0}),
                json.dumps({"evidence_score": 78.4}),
                "Promising",
                "sufficient",
                now,
            ),
            (
                "eval-stage2",
                "candidate-known",
                "stage2",
                "stage2-repeatability-v1",
                json.dumps([]),
                json.dumps({"coverage_days": 351.0, "observed_event_count": 12}),
                json.dumps({"evidence_score": 55.2, "overall_score": 68.5}),
                "Inconclusive",
                "sufficient",
                now,
            ),
            (
                "eval-catalog",
                "candidate-known",
                "catalog",
                "exoplanet-archive-match-v1",
                json.dumps([]),
                json.dumps(
                    {
                        "current_stage": "stage2",
                        "match_kind": "target-id+period",
                        "coordinate_separation_deg": 0.0003,
                        "period_delta_fraction": 0.001,
                        "catalog_record": {
                            "object_name": "42.01",
                            "record_id": "42.01",
                            "table": "toi",
                            "disposition": "PC",
                            "period_days": 4.2,
                        },
                    }
                ),
                json.dumps({"catalog_id": "42.01", "match_score": 100.0}),
                "Known Match",
                "sufficient",
                now,
            ),
        ]
        connection.executemany(
            """
            INSERT INTO evaluations (
                evaluation_id, candidate_id, stage, model_version,
                input_product_ids, feature_json, metric_json, classification,
                data_sufficiency, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            evaluations,
        )
        connection.close()
        self.store = ReadOnlyStore(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_identified_projects_catalog_match_and_stage_performance(self) -> None:
        response = self.store.identified(100)

        self.assertEqual(response["data_version"], 18)
        self.assertEqual(response["count"], 1)
        row = response["rows"][0]
        self.assertEqual(row["target_id"], "TIC 123")
        self.assertEqual(row["identified_stage"], "stage2")
        self.assertEqual(row["identified_as"]["object_name"], "42.01")
        self.assertEqual(row["identified_as"]["match_kind"], "target-id+period")
        self.assertEqual(row["identified_as"]["match_score"], 100.0)
        self.assertEqual(row["performance"]["stage1"]["evidence_score"], 78.4)
        self.assertEqual(row["performance"]["stage2"]["coverage_days"], 351.0)
        self.assertEqual(row["performance"]["stage2"]["status"], "Inconclusive")


if __name__ == "__main__":
    unittest.main()
