import unittest
from pathlib import Path

import numpy as np

from pipeline.stage1 import LightCurve, Stage1Processor, resolve_artifact_path


class Stage1Tests(unittest.TestCase):
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
