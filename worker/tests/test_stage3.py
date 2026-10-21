import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from pipeline.stage3 import Stage3Processor, publish_stage3_result
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


def synthetic_series(days: float = 90.0) -> tuple[np.ndarray, np.ndarray]:
    time = np.arange(0.0, days, 0.02)
    phase = ((time - 2.0 + 5.0) % 10.0) - 5.0
    flux = np.ones_like(time) + 0.00015 * np.sin(time * 1.7)
    flux[np.abs(phase) < 0.08] -= 0.01
    return time, flux


class Stage3Tests(unittest.TestCase):
    def test_chronological_holdout_compares_model_and_baseline(self) -> None:
        time, flux = synthetic_series()
        result = Stage3Processor().evaluate_series(
            time,
            flux,
            candidate_id="stage2-candidate",
            target_id="TIC 1",
            stage2_features={
                "stage1_period_days": 10.0,
                "stage1_transit_time": 2.0,
                "stage1_duration_hours": 4.0,
            },
        )

        self.assertEqual(result["status"], "Promising")
        self.assertEqual(len(result["features"]["training_windows"]), 2)
        self.assertIsNotNone(result["features"]["holdout_window"]["start"])
        self.assertIn("precision", result["features"]["holdout_metrics"])
        self.assertIn("f1", result["features"]["baseline_metrics"])
        self.assertEqual(result["model_version"], "stage3-logreg-phase-v1")
        self.assertGreaterEqual(result["features"]["holdout_metrics"]["event_recovery_rate"], 0.5)

    def test_short_or_non_periodic_history_stays_needs_more_data(self) -> None:
        time = np.arange(0.0, 10.0, 0.02)
        flux = np.ones_like(time)
        result = Stage3Processor().evaluate_series(
            time,
            flux,
            candidate_id="stage2-candidate",
            target_id="TIC 1",
            stage2_features={"stage1_period_days": 10.0, "stage1_transit_time": 2.0},
        )

        self.assertEqual(result["status"], "Needs More Data")
        self.assertEqual(result["data_sufficiency"], "insufficient")

    def test_stage3_publish_persists_metrics_and_replay_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = DataPaths(Path(temp_dir))
            paths.ensure()
            store = DuckDBStore(paths.database)
            occurred_at = datetime.now(timezone.utc)
            store.publish_batch(
                version=1,
                targets=[
                    {
                        "target_id": "TIC 1",
                        "provider_target_id": "1",
                        "ra_deg": 1.0,
                        "dec_deg": 2.0,
                        "sector": 1,
                        "cadence": "SPOC",
                        "source_product_family": "TESS SPOC light curve",
                        "selection_bucket": "stratum-00",
                        "manifest_status": "processed",
                    }
                ],
                candidates=[
                    {
                        "candidate_id": "stage2-candidate",
                        "target_id": "TIC 1",
                        "current_stage": "stage2",
                        "status": "Promising",
                        "overall_score": 82.0,
                        "stage_score": 84.0,
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
                evaluations=[
                    {
                        "evaluation_id": "stage2-evaluation",
                        "candidate_id": "stage2-candidate",
                        "stage": "stage2",
                        "model_version": "stage2-repeatability-v1",
                        "data_window_start": None,
                        "data_window_end": None,
                        "input_product_ids": ["fixture.fits"],
                        "feature_json": {
                            "stage1_period_days": 10.0,
                            "stage1_transit_time": 2.0,
                            "stage1_duration_hours": 4.0,
                        },
                        "metric_json": {"stage1_score": 80.0},
                        "classification": "Promising",
                        "data_sufficiency": "sufficient",
                    }
                ],
            )
            time, flux = synthetic_series()
            result = Stage3Processor().evaluate_series(
                time,
                flux,
                candidate_id="stage2-candidate",
                target_id="TIC 1",
                stage2_features={
                    "stage1_period_days": 10.0,
                    "stage1_transit_time": 2.0,
                    "stage1_duration_hours": 4.0,
                },
                normalized_dir=paths.normalized,
            )
            version = publish_stage3_result(
                store,
                result,
                stage1_score=80.0,
                stage2_score=84.0,
                input_product_ids=["fixture.fits"],
            )

            self.assertEqual(version, 2)
            with store.connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_stage,status FROM candidates WHERE candidate_id = 'stage2-candidate'"
                    ).fetchone(),
                    ("stage3", "Promising"),
                )
                evaluation_row = connection.execute(
                    "SELECT metric_json FROM evaluations WHERE stage = 'stage3'"
                ).fetchone()
                metrics = json.loads(evaluation_row[0])
                self.assertEqual(metrics["baseline_model_version"], "stage3-phase-window-v1")
                self.assertIn("holdout_metrics", metrics)
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM ranking_snapshots WHERE sequence_number = 2"
                    ).fetchone()[0],
                    2,
                )
                event_count = connection.execute(
                    "SELECT COUNT(*) FROM pipeline_events WHERE event_type = 'stage3_evaluated'"
                ).fetchone()[0]
                self.assertEqual(event_count, 1)
                # Reading replay state does not invoke a model or create another event.
                connection.execute("SELECT candidate_order_json FROM ranking_snapshots WHERE view = 'stage3'").fetchall()
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM pipeline_events WHERE event_type = 'stage3_evaluated'"
                    ).fetchone()[0],
                    event_count,
                )


if __name__ == "__main__":
    unittest.main()
