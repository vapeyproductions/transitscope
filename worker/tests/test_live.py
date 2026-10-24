import unittest

from main import queue_cycle_events
from pipeline.live import _stage2_needs_data_queue


class LiveQueueTests(unittest.TestCase):
    def test_continuous_stage1_intake_is_distinguished_from_waterfall_work(self) -> None:
        events = queue_cycle_events(
            {
                "continuous_stage1_intake": {
                    "intake_reason": "continuous_stage1_intake",
                    "candidate_ids": ["candidate-new"],
                },
                "stage1_requests": [
                    {
                        "intake_reason": "released_stage1_work",
                        "candidate_ids": ["candidate-replacement"],
                    },
                    {
                        "intake_reason": "stage1_needs_more_data_reassessment",
                        "refresh_count": 1,
                        "candidate_ids": ["candidate-refresh"],
                    },
                ],
            },
            None,
        )

        self.assertEqual(
            [(event["candidate_id"], event["kind"], event["reason"]) for event in events],
            [
                ("candidate-replacement", "intake", "released_stage1_work"),
                ("candidate-refresh", "reassessment", "stage1_needs_more_data_reassessment"),
                ("candidate-new", "intake", "continuous_stage1_intake"),
            ],
        )

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
