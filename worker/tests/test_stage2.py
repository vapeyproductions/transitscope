import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from pipeline.stage2 import Stage2Processor, publish_stage2_result
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


def synthetic_series(days: float) -> tuple[np.ndarray, np.ndarray]:
    time = np.arange(0.0, days, 0.02)
    phase = ((time - 2.0 + 5.0) % 10.0) - 5.0
    flux = np.ones_like(time)
    flux[np.abs(phase) < 0.08] -= 0.01
    flux += 0.0005 * np.sin(time * 3.0)
    return time, flux


class Stage2Tests(unittest.TestCase):
    def test_short_history_stays_needs_more_data(self) -> None:
        time, flux = synthetic_series(30.0)
        result = Stage2Processor().evaluate_series(
            time,
            flux,
            candidate_id="stage1-candidate",
            target_id="TIC 1",
            stage1_features={"period_days": 10.0, "transit_time": 2.0, "duration_hours": 4.0},
        )

        self.assertEqual(result["status"], "Needs More Data")
        self.assertEqual(result["data_sufficiency"], "insufficient")
        self.assertLess(result["features"]["coverage_days"], 180.0)

    def test_long_repeatable_history_can_be_promising(self) -> None:
        time, flux = synthetic_series(365.0)
        result = Stage2Processor().evaluate_series(
            time,
            flux,
            candidate_id="stage1-candidate",
            target_id="TIC 1",
            stage1_features={"period_days": 10.0, "transit_time": 2.0, "duration_hours": 4.0},
        )

        self.assertEqual(result["status"], "Promising")
        self.assertGreaterEqual(result["features"]["observed_event_count"], 2)
        self.assertGreaterEqual(result["evidence_score"], 60.0)

    def test_short_recent_window_backfills_older_archive_history(self) -> None:
        old_time, old_flux = synthetic_series(365.0)
        recent_time, recent_flux = synthetic_series(30.0)
        recent_time = recent_time + 1200.0
        result = Stage2Processor().evaluate_series(
            np.concatenate((old_time, recent_time)),
            np.concatenate((old_flux, recent_flux)),
            candidate_id="stage1-candidate",
            target_id="TIC 1",
            stage1_features={"period_days": 10.0, "transit_time": 2.0, "duration_hours": 4.0},
        )

        self.assertTrue(result["features"]["backfilled_beyond_lookback"])
        self.assertGreater(result["features"]["coverage_days"], 180.0)
        self.assertEqual(result["status"], "Promising")

    def test_stage2_publish_moves_candidate_and_writes_both_rankings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = DataPaths(root)
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
                        "candidate_id": "stage1-candidate",
                        "target_id": "TIC 1",
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
                evaluations=[
                    {
                        "evaluation_id": "stage1-evaluation",
                        "candidate_id": "stage1-candidate",
                        "stage": "stage1",
                        "model_version": "stage1-bls-v1",
                        "data_window_start": None,
                        "data_window_end": None,
                        "input_product_ids": ["fixture.fits"],
                        "feature_json": {
                            "period_days": 10.0,
                            "transit_time": 2.0,
                            "duration_hours": 4.0,
                        },
                        "metric_json": {"evidence_score": 80.0},
                        "classification": "Promising",
                        "data_sufficiency": "sufficient",
                    }
                ],
                snapshots=[
                    {
                        "snapshot_id": "stage1-snapshot",
                        "view": "stage1",
                        "sequence_number": 1,
                        "occurred_at": occurred_at,
                        "candidate_order_json": ["stage1-candidate"],
                        "active_candidate_count": 1,
                        "processed_target_count": 1,
                    }
                ],
            )
            time, flux = synthetic_series(365.0)
            result = Stage2Processor().evaluate_series(
                time,
                flux,
                candidate_id="stage1-candidate",
                target_id="TIC 1",
                stage1_features={"period_days": 10.0, "transit_time": 2.0, "duration_hours": 4.0},
                normalized_path=paths.normalized / "stage2-candidate.npz",
            )
            version = publish_stage2_result(
                store,
                result,
                stage1_score=80.0,
                input_product_ids=["fixture.fits"],
            )

            self.assertEqual(version, 2)
            with store.connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT current_stage,status FROM candidates WHERE candidate_id = 'stage1-candidate'"
                    ).fetchone(),
                    ("stage2", "Promising"),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM ranking_snapshots WHERE sequence_number = 2"
                    ).fetchone()[0],
                    3,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT stage FROM evaluations WHERE candidate_id = 'stage1-candidate' ORDER BY created_at DESC LIMIT 1"
                    ).fetchone()[0],
                    "stage2",
                )
                self.assertEqual(
                    json.loads(
                        connection.execute(
                            "SELECT payload_json FROM pipeline_events WHERE event_type = 'stage2_evaluated'"
                        ).fetchone()[0]
                    )["model_version"],
                    "stage2-repeatability-v2-backfill",
                )


if __name__ == "__main__":
    unittest.main()
