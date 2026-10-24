import unittest
from pathlib import Path

import numpy as np

from pipeline.stage1 import LightCurve, Stage1Processor, resolve_artifact_path


class Stage1Tests(unittest.TestCase):
    def test_candidate_ids_distinguish_multiple_signals_from_one_target(self) -> None:
        self.assertEqual(
            Stage1Processor._candidate_id("TIC 123456789", 1),
            "stage1-tic-123456789-signal-01",
        )
        self.assertEqual(
            Stage1Processor._candidate_id("TIC 123456789", 2),
            "stage1-tic-123456789-signal-02",
        )
        first = {"period_days": 3.2, "transit_time": 1.0, "duration_hours": 2.0}
        second = {"period_days": 11.8, "transit_time": 2.0, "duration_hours": 4.0}
        self.assertNotEqual(
            Stage1Processor._candidate_id("TIC 123456789", 1, first),
            Stage1Processor._candidate_id("TIC 123456789", 2, second),
        )
        self.assertEqual(
            Stage1Processor._candidate_id("TIC 123456789", 1, first),
            Stage1Processor._candidate_id("TIC 123456789", 2, first),
        )

    def test_remove_signal_flattens_periodic_transit_windows(self) -> None:
        time = np.linspace(0, 10, 10001)
        flux = np.ones_like(time)
        phase = ((time - 0.5 + 0.5 * 2.0) % 2.0) - 0.5 * 2.0
        transit = np.abs(phase) <= 0.05
        flux[transit] = 0.98
        residual = Stage1Processor._remove_signal(
            time,
            flux,
            {"period_days": 2.0, "duration_hours": 2.4, "transit_time": 0.5},
        )
        self.assertGreater(float(np.nanmedian(residual[transit])), 0.995)

    def test_isolate_signal_removes_siblings_but_keeps_selected_signal(self) -> None:
        time = np.linspace(0, 20, 20001)
        flux = np.ones_like(time)
        for period, transit_time in ((2.0, 0.5), (5.0, 1.0)):
            phase = ((time - transit_time + 0.5 * period) % period) - 0.5 * period
            flux[np.abs(phase) <= 0.05] = 0.98
        isolated = Stage1Processor.isolate_signal(
            time,
            flux,
            {
                "signal_index": 1,
                "signal_family": [
                    {"signal_index": 1, "period_days": 2.0, "transit_time": 0.5, "duration_hours": 2.4},
                    {"signal_index": 2, "period_days": 5.0, "transit_time": 1.0, "duration_hours": 2.4},
                ],
            },
        )
        sibling_phase = ((time - 1.0 + 2.5) % 5.0) - 2.5
        sibling = np.abs(sibling_phase) <= 0.05
        selected_phase = ((time - 0.5 + 1.0) % 2.0) - 1.0
        selected = np.abs(selected_phase) <= 0.05
        self.assertGreater(float(np.nanmedian(isolated[sibling])), 0.995)
        self.assertLess(float(np.nanmedian(isolated[selected])), 0.99)

    def test_resolve_artifact_path_supports_probe_manifest_paths(self) -> None:
        self.assertEqual(
            resolve_artifact_path(
                "data/raw/example.fits", data_dir=Path("/app/data")
            ),
            Path("/app/data/raw/example.fits"),
        )

    def test_cleaning_rejects_short_coverage_as_needs_more_data_input(self) -> None:
        processor = Stage1Processor()
        time = np.linspace(0, 2, 600)
        flux = np.ones(600)
        quality = np.zeros(600, dtype=np.int64)
        cleaned = processor.clean(LightCurve(time, flux, quality, "PDCSAP_FLUX", {}))
        self.assertFalse(cleaned["sufficient"])
        self.assertEqual(cleaned["features"]["usable_samples"], 600)


if __name__ == "__main__":
    unittest.main()
