import unittest

import numpy as np
from pipeline.independence import assess_signal_independence


class SignalIndependenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.time = np.arange(0.0, 40.0, 0.02)
        self.primary = {"period_days": 2.0, "transit_time": 0.5, "duration_hours": 2.0}

    @staticmethod
    def _inject(time: np.ndarray, features: dict[str, float], depth: float) -> np.ndarray:
        period = features["period_days"]
        phase = ((time - features["transit_time"] + period / 2) % period) - period / 2
        flux = np.ones_like(time)
        flux[np.abs(phase) <= features["duration_hours"] / 24 / 2] -= depth
        return flux

    def test_distinct_residual_signal_passes_chronological_holdout(self) -> None:
        sibling = {"period_days": 2.02, "transit_time": 1.0, "duration_hours": 2.0}
        flux = self._inject(self.time, self.primary, 0.01)
        flux += self._inject(self.time, sibling, 0.008) - 1.0

        result = assess_signal_independence(
            self.time,
            flux,
            sibling_features=sibling,
            primary_features=self.primary,
        )

        self.assertEqual(result.status, "Promising")
        self.assertTrue(result.metrics["primary_removed"])
        self.assertGreaterEqual(result.metrics["first_half_event_count"], 1)
        self.assertGreaterEqual(result.metrics["second_half_event_count"], 1)

    def test_same_transit_train_fails_after_primary_removal(self) -> None:
        flux = self._inject(self.time, self.primary, 0.01)

        result = assess_signal_independence(
            self.time,
            flux,
            sibling_features=self.primary,
            primary_features=self.primary,
        )

        self.assertEqual(result.status, "Duplicate Signal")
        self.assertEqual(result.metrics["full_depth_snr"], 0.0)

    def test_short_observation_is_not_declared_duplicate(self) -> None:
        sibling = {"period_days": 2.02, "transit_time": 1.0, "duration_hours": 2.0}
        result = assess_signal_independence(
            self.time[:100],
            self._inject(self.time[:100], sibling, 0.008),
            sibling_features=sibling,
            primary_features=self.primary,
        )

        self.assertEqual(result.status, "Needs More Data")
        self.assertEqual(result.data_sufficiency, "insufficient")


if __name__ == "__main__":
    unittest.main()
