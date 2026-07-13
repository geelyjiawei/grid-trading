import json
import sys
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[1]
REPLAY_DIR = REPOSITORY / "contracts" / "replay"
if str(REPLAY_DIR) not in sys.path:
    sys.path.insert(0, str(REPLAY_DIR))

from python_grid_plan_replay import replay_fixture  # noqa: E402


class GridPlanReplayTests(unittest.TestCase):
    def test_python_grid_plans_match_the_cross_language_golden_replay(self):
        fixture = REPLAY_DIR / "grid-plan-v1.json"
        expected = json.loads(
            (REPLAY_DIR / "grid-plan-v1.expected.json").read_text(encoding="utf-8")
        )

        self.assertEqual(replay_fixture(fixture), expected)


if __name__ == "__main__":
    unittest.main()
