import unittest

from pipeline.live import _stage2_needs_data_queue


class LiveQueueTests(unittest.TestCase):
    def test_stage2_needs_data_queue_is_ranked_and_thresholded(self) -> None:
        candidates = [
            {
                "candidate_id": "outside-window",
                "current_stage": "stage2",
                "status": "Needs More Data",
                "active": True,
                "promotion_eligible": True,
                "rank_stage": 101,
            },
            {
                "candidate_id": "rank-3",
                "current_stage": "stage2",
                "status": "Needs More Data",
                "active": True,
                "promotion_eligible": True,
                "rank_stage": 3,
            },
            {
                "candidate_id": "rank-1",
                "current_stage": "stage2",
                "status": "Needs More Data",
                "active": True,
                "promotion_eligible": True,
                "rank_stage": 1,
            },
            {
                "candidate_id": "promising",
                "current_stage": "stage2",
                "status": "Promising",
                "active": True,
                "promotion_eligible": True,
                "rank_stage": 2,
            },
            {
                "candidate_id": "inconclusive",
                "current_stage": "stage2",
                "status": "Needs More Data",
                "active": False,
                "promotion_eligible": True,
                "rank_stage": 4,
            },
        ]

        queue = _stage2_needs_data_queue(candidates, threshold=100)

        self.assertEqual(
            [candidate["candidate_id"] for candidate in queue],
            ["rank-1", "rank-3"],
        )


if __name__ == "__main__":
    unittest.main()
