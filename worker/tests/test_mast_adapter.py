import unittest
from pathlib import Path

from ingestion.mast_adapter import MastProbeConfig, MastProductAdapter


class MastAdapterTests(unittest.TestCase):
    def test_config_normalizes_tic_prefix(self) -> None:
        config = MastProbeConfig.from_mapping(
            {"data_dir": "data", "target_name": "TIC 123"}
        )
        self.assertEqual(config.target_name, "123")
        self.assertEqual(config.sector, 1)

    def test_product_selection_prefers_public_light_curve(self) -> None:
        products = [
            {
                "description": "Target pixel files",
                "productSubGroupDescription": "TP",
                "dataRights": "PUBLIC",
                "productFilename": "target_tp.fits",
            },
            {
                "description": "Light curves",
                "productSubGroupDescription": "LC",
                "dataRights": "PUBLIC",
                "productFilename": "target_lc.fits",
            },
        ]
        selected = MastProductAdapter.select_light_curve_product(products)
        self.assertEqual(selected["productFilename"], "target_lc.fits")

    def test_product_selection_rejects_missing_public_light_curve(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "no public FITS light-curve"):
            MastProductAdapter.select_light_curve_product(
                [
                    {
                        "description": "Target pixel files",
                        "productSubGroupDescription": "TP",
                        "dataRights": "PUBLIC",
                        "productFilename": "target_tp.fits",
                    }
                ]
            )


if __name__ == "__main__":
    unittest.main()
