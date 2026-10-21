import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ingestion.exoplanet_archive import (
    CatalogRecord,
    ExoplanetArchiveAdapter,
    ExoplanetArchiveConfig,
)
from pipeline.lifecycle import promotable_candidates, publish_known_matches
from storage.duckdb_store import DuckDBStore
from storage.paths import DataPaths


class FakeArchiveAdapter(ExoplanetArchiveAdapter):
    def _query_table(self, table: str):  # type: ignore[override]
        if table == "pscomppars":
            return [
                CatalogRecord(
                    record_id="Kepler-1 b",
                    table=table,
                    object_name="Kepler-1 b",
                    target_id="TIC 123",
                    ra_deg=10.0,
                    dec_deg=-2.0,
                    period_days=10.0,
                    disposition="1",
                    raw={"pl_name": "Kepler-1 b"},
                )
            ], "fixture-pscomppars"
        return [], f"fixture-{table}"


class CatalogTests(unittest.TestCase):
    def test_archive_projection_is_cached_and_matching_is_conservative(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = FakeArchiveAdapter(
                ExoplanetArchiveConfig(data_dir=Path(temp_dir), tables=("pscomppars", "toi"))
            )
            payload = adapter.fetch(force=True)
            self.assertEqual(payload["status"], "success")
            self.assertTrue(Path(payload["cache_path"]).exists())
            self.assertEqual(len(payload["records"]), 1)

            records = [adapter._record_from_cache(row) for row in payload["records"]]
            match = adapter.match_candidate(
                target_id="TIC 123",
                ra_deg=50.0,
                dec_deg=50.0,
                period_days=10.01,
                records=records,
            )
            self.assertIsNotNone(match)
            self.assertEqual(match["match_kind"], "target-id+period")

            different_period = adapter.match_candidate(
                target_id="TIC 123",
                ra_deg=10.0,
                dec_deg=-2.0,
                period_days=4.0,
                records=records,
            )
            self.assertIsNone(different_period)

    def test_known_match_tombstones_candidate_and_reorders_all_views(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = DataPaths(root)
            paths.ensure()
            store = DuckDBStore(paths.database)
            occurred_at = datetime.now(timezone.utc)
            candidates = []
            for candidate_id, target_id, score, rank in (
                ("candidate-known", "TIC 123", 90.0, 1),
                ("candidate-next", "TIC 456", 70.0, 2),
            ):
                candidates.append(
                    {
                        "candidate_id": candidate_id,
                        "target_id": target_id,
                        "current_stage": "stage1",
                        "status": "Promising",
                        "overall_score": score,
                        "stage_score": score,
                        "rank_overall": rank,
                        "rank_stage": rank,
                        "activity": "No current activity",
                        "rank_delta": 0,
                        "movement_kind": "new",
                        "last_evaluated_at": occurred_at,
                        "known_match_id": None,
                        "active": True,
                    }
                )
            store.publish_batch(
                version=1,
                targets=[
                    {
                        "target_id": "TIC 123",
                        "provider_target_id": "123",
                        "ra_deg": 10.0,
                        "dec_deg": -2.0,
                        "sector": 1,
                        "cadence": "SPOC",
                        "source_product_family": "TESS SPOC light curve",
                        "selection_bucket": "stratum-00",
                        "manifest_status": "processed",
                    },
                    {
                        "target_id": "TIC 456",
                        "provider_target_id": "456",
                        "ra_deg": 20.0,
                        "dec_deg": -3.0,
                        "sector": 1,
                        "cadence": "SPOC",
                        "source_product_family": "TESS SPOC light curve",
                        "selection_bucket": "stratum-01",
                        "manifest_status": "processed",
                    },
                ],
                candidates=candidates,
                evaluations=[
                    {
                        "evaluation_id": f"stage1-{candidate['candidate_id']}",
                        "candidate_id": candidate["candidate_id"],
                        "stage": "stage1",
                        "model_version": "stage1-bls-v1",
                        "data_window_start": None,
                        "data_window_end": None,
                        "input_product_ids": ["fixture.fits"],
                        "feature_json": {"period_days": 10.0},
                        "metric_json": {"evidence_score": candidate["stage_score"]},
                        "classification": candidate["status"],
                        "data_sufficiency": "sufficient",
                    }
                    for candidate in candidates
                ],
                snapshots=[
                    {
                        "snapshot_id": f"{view}-snapshot-1",
                        "view": view,
                        "sequence_number": 1,
                        "occurred_at": occurred_at,
                        "candidate_order_json": ["candidate-known", "candidate-next"],
                        "active_candidate_count": 2,
                        "processed_target_count": 2,
                    }
                    for view in ("overall", "stage1", "stage2", "stage3")
                ],
            )
            version = publish_known_matches(
                store,
                [
                    {
                        "candidate_id": "candidate-known",
                        "catalog_id": "Kepler-1 b",
                        "object_name": "Kepler-1 b",
                        "match_kind": "target-id+period",
                        "match_score": 100.0,
                        "coordinate_separation_deg": 0.0,
                        "period_delta_fraction": 0.0,
                        "tolerances": {"coordinate_tolerance_deg": 0.02},
                        "record": {"record_id": "Kepler-1 b"},
                    }
                ],
                catalog_cache_path="catalog.json",
            )

            self.assertEqual(version, 2)
            with store.connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT status,active,known_match_id,rank_overall FROM candidates WHERE candidate_id = 'candidate-known'"
                    ).fetchone(),
                    ("Known Match", False, "Kepler-1 b", None),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT rank_overall,rank_stage FROM candidates WHERE candidate_id = 'candidate-next'"
                    ).fetchone(),
                    (1, 1),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM ranking_snapshots WHERE sequence_number = 2"
                    ).fetchone()[0],
                    4,
                )
                event = connection.execute(
                    "SELECT event_type,payload_json FROM pipeline_events WHERE event_type = 'known_match'"
                ).fetchone()
                self.assertEqual(event[0], "known_match")
                self.assertEqual(json.loads(event[1])["catalog_id"], "Kepler-1 b")

    def test_promotion_uses_status_priority_and_top_x(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = DataPaths(Path(temp_dir))
            paths.ensure()
            store = DuckDBStore(paths.database)
            now = datetime.now(timezone.utc)
            store.publish_batch(
                version=1,
                candidates=[
                    {
                        "candidate_id": "promising",
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
                        "last_evaluated_at": now,
                        "known_match_id": None,
                        "active": True,
                    },
                    {
                        "candidate_id": "needs-data",
                        "target_id": "TIC 2",
                        "current_stage": "stage1",
                        "status": "Needs More Data",
                        "overall_score": 79.0,
                        "stage_score": 79.0,
                        "rank_overall": 2,
                        "rank_stage": 2,
                        "activity": "Awaiting more data",
                        "rank_delta": 0,
                        "movement_kind": "new",
                        "last_evaluated_at": now,
                        "known_match_id": None,
                        "active": True,
                    },
                    {
                        "candidate_id": "inconclusive",
                        "target_id": "TIC 3",
                        "current_stage": "stage1",
                        "status": "Inconclusive",
                        "overall_score": 99.0,
                        "stage_score": 99.0,
                        "rank_overall": 3,
                        "rank_stage": 3,
                        "activity": "No current activity",
                        "rank_delta": 0,
                        "movement_kind": "new",
                        "last_evaluated_at": now,
                        "known_match_id": None,
                        "active": True,
                    },
                ],
            )
            selected = promotable_candidates(store, stage="stage1", capacity=1)
            self.assertEqual([candidate["candidate_id"] for candidate in selected], ["promising"])
            selected = promotable_candidates(store, stage="stage1", capacity=2)
            self.assertEqual(
                [candidate["candidate_id"] for candidate in selected],
                ["promising", "needs-data"],
            )
            selected = promotable_candidates(store, stage="stage1", capacity=3)
            self.assertEqual(
                [candidate["candidate_id"] for candidate in selected],
                ["promising", "needs-data", "inconclusive"],
            )


if __name__ == "__main__":
    unittest.main()
