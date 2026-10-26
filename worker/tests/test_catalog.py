import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ingestion.exoplanet_archive import (
    CATALOG_CACHE_SCHEMA_VERSION,
    CatalogRecord,
    ExoplanetArchiveAdapter,
    ExoplanetArchiveConfig,
)
from pipeline.lifecycle import (
    candidate_match_inputs,
    overall_sort_key,
    promotable_candidates,
    publish_known_matches,
    stage3_replacement_debt,
)
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
    def test_catalog_inputs_are_limited_to_prime_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = DataPaths(Path(temp_dir))
            paths.ensure()
            store = DuckDBStore(paths.database)
            now = datetime.now(timezone.utc)
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
                        "selection_bucket": "fixture",
                        "manifest_status": "processed",
                    }
                ],
                candidates=[
                    {
                        "candidate_id": "primary",
                        "target_id": "TIC 123",
                        "current_stage": "stage3",
                        "status": "Promising",
                        "overall_score": 90.0,
                        "stage_score": 90.0,
                        "rank_overall": 1,
                        "rank_stage": 1,
                        "activity": "No current activity",
                        "rank_delta": 0,
                        "movement_kind": "new",
                        "last_evaluated_at": now,
                        "known_match_id": None,
                        "active": True,
                        "signal_resolution": "independent",
                    },
                    {
                        "candidate_id": "sibling",
                        "target_id": "TIC 123",
                        "current_stage": "stage1",
                        "status": "Promising",
                        "overall_score": 89.0,
                        "stage_score": 89.0,
                        "rank_overall": 2,
                        "rank_stage": 2,
                        "activity": "Queued for independence assessment",
                        "rank_delta": 0,
                        "movement_kind": "new",
                        "last_evaluated_at": now,
                        "known_match_id": None,
                        "active": True,
                        "signal_resolution": "pending_alias_review",
                    },
                ],
            )

            self.assertEqual([item["candidate_id"] for item in candidate_match_inputs(store)], ["primary"])
            self.assertEqual(
                {item["candidate_id"] for item in candidate_match_inputs(store, prime_only=False)},
                {"primary", "sibling"},
            )

    def test_live_projection_includes_tic_id_and_failed_cache_is_not_reused(self) -> None:
        self.assertIn("tic_id", ExoplanetArchiveAdapter.TABLE_COLUMNS["pscomppars"])
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = ExoplanetArchiveAdapter(ExoplanetArchiveConfig(data_dir=Path(temp_dir)))
            cache_path = adapter.cache_path
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(
                json.dumps(
                    {
                        "schema_version": CATALOG_CACHE_SCHEMA_VERSION,
                        "status": "failed",
                        "records": [],
                    }
                )
            )
            self.assertFalse(adapter._cache_is_fresh(cache_path))

    def test_overall_ranking_allows_exceptional_stage1_to_beat_weak_stage3(self) -> None:
        candidates = [
            {
                "candidate_id": "stage3-prime",
                "current_stage": "stage3",
                "status": "Promising",
                "overall_score": 48.0,
                "stage_score": 44.0,
            },
            {
                "candidate_id": "stage1-promising",
                "current_stage": "stage1",
                "status": "Promising",
                "overall_score": 98.0,
                "stage_score": 98.0,
            },
        ]

        ordered = sorted(candidates, key=overall_sort_key)

        self.assertEqual(
            [candidate["candidate_id"] for candidate in ordered],
            ["stage1-promising", "stage3-prime"],
        )

    def test_overall_ranking_prime_candidate_beats_merely_strong_stage1(self) -> None:
        candidates = [
            {
                "candidate_id": "stage1-promising",
                "current_stage": "stage1",
                "status": "Promising",
                "overall_score": 98.0,
                "stage_score": 98.0,
            },
            {
                "candidate_id": "stage3-prime",
                "current_stage": "stage3",
                "status": "Promising",
                "overall_score": 84.0,
                "stage_score": 90.0,
            },
        ]

        ordered = sorted(candidates, key=overall_sort_key)

        self.assertEqual(
            [candidate["candidate_id"] for candidate in ordered],
            ["stage3-prime", "stage1-promising"],
        )

    def test_archive_projection_is_cached_and_matching_is_conservative(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = FakeArchiveAdapter(
                ExoplanetArchiveConfig(data_dir=Path(temp_dir), tables=("pscomppars", "toi"))
            )
            payload = adapter.fetch(force=True)
            self.assertEqual(payload["status"], "success")
            self.assertTrue(Path(payload["cache_path"]).exists())
            self.assertEqual(len(payload["records"]), 1)
            self.assertEqual(payload["record_count"], 1)
            self.assertTrue(payload["complete_projection"])

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

    def test_stage3_replacement_debt_is_durable_and_stage_local(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = DataPaths(Path(temp_dir))
            paths.ensure()
            store = DuckDBStore(paths.database)
            now = datetime.now(timezone.utc)
            store.publish_batch(
                version=1,
                events=[
                    {
                        "event_id": "stage3-inconclusive",
                        "sequence_number": 1,
                        "candidate_id": "stage3-inconclusive",
                        "stage": "stage3",
                        "event_type": "stage3_evaluated",
                        "payload_json": {"classification": "Inconclusive"},
                        "occurred_at": now,
                        "model_run_id": "fixture",
                    },
                    {
                        "event_id": "stage3-promising",
                        "sequence_number": 2,
                        "candidate_id": "stage3-promising",
                        "stage": "stage3",
                        "event_type": "stage3_evaluated",
                        "payload_json": {"classification": "Promising"},
                        "occurred_at": now,
                        "model_run_id": "fixture",
                    },
                    {
                        "event_id": "stage3-promising-dispatched",
                        "sequence_number": 3,
                        "candidate_id": "stage3-promising",
                        "stage": "stage3",
                        "event_type": "stage3_replacement_called",
                        "payload_json": {"source_candidate_id": "stage3-promising"},
                        "occurred_at": now,
                        "model_run_id": "fixture",
                    },
                    {
                        "event_id": "stage1-known",
                        "sequence_number": 4,
                        "candidate_id": "stage1-known",
                        "stage": "stage1",
                        "event_type": "known_match",
                        "payload_json": {"active": False},
                        "occurred_at": now,
                        "model_run_id": "catalog",
                    },
                    {
                        "event_id": "stage3-known",
                        "sequence_number": 5,
                        "candidate_id": "stage3-known",
                        "stage": "stage3",
                        "event_type": "known_match",
                        "payload_json": {"active": False},
                        "occurred_at": now,
                        "model_run_id": "catalog",
                    },
                ],
            )

            debt = stage3_replacement_debt(store)

            self.assertEqual(
                [item["candidate_id"] for item in debt],
                ["stage3-inconclusive", "stage3-known"],
            )

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
            selected = promotable_candidates(store, stage="stage1", threshold=1)
            self.assertEqual([candidate["candidate_id"] for candidate in selected], ["promising"])
            selected = promotable_candidates(store, stage="stage1", threshold=2)
            self.assertEqual([candidate["candidate_id"] for candidate in selected], ["promising"])
            selected = promotable_candidates(store, stage="stage1", threshold=3)
            self.assertEqual([candidate["candidate_id"] for candidate in selected], ["promising"])


if __name__ == "__main__":
    unittest.main()
