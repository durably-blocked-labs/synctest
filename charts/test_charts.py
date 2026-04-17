import importlib.util
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("MPLBACKEND", "Agg")
os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")


def load_charts_module():
    module_path = Path(__file__).with_name("charts.py")
    spec = importlib.util.spec_from_file_location("synctest_charts", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class SearchSpaceFigureTest(unittest.TestCase):
    def test_search_space_plot_handles_large_theoretical_bounds(self):
        charts = load_charts_module()
        data_dir = Path(__file__).resolve().parents[1] / "bugs" / "ra-priority-inversion" / "benchmarking" / "data"

        summaries = charts.load_all_summaries(data_dir)
        traces = charts.load_all_traces(data_dir)
        rep_summaries, rep_traces, _, _ = charts.representative_views(summaries, traces, {})

        with tempfile.TemporaryDirectory() as tmpdir:
            charts.fig_search_space_vs_explored(rep_summaries, rep_traces, tmpdir)
            output = Path(tmpdir) / "search_space_vs_explored.png"
            self.assertTrue(output.exists())


class SeedRunsFigureTest(unittest.TestCase):
    def test_seed_runs_rows_use_first_bug_or_observed_run_count(self):
        charts = load_charts_module()
        summaries = {
            "pct-d2": {
                2: [
                    {"policy": "pct-d2", "attempt": 2, "run_num": 1, "user_failed": False},
                    {"policy": "pct-d2", "attempt": 2, "run_num": 2, "user_failed": True},
                ],
                1: [
                    {"policy": "pct-d2", "attempt": 1, "run_num": 1, "user_failed": False},
                    {"policy": "pct-d2", "attempt": 1, "run_num": 2, "user_failed": False},
                    {"policy": "pct-d2", "attempt": 1, "run_num": 3, "user_failed": False},
                ],
            },
            "chess-gl": {
                1: [
                    {"policy": "chess-gl", "attempt": 1, "run_num": 1, "user_failed": False},
                    {"policy": "chess-gl", "attempt": 1, "run_num": 2, "user_failed": True},
                ],
            },
        }

        rows = charts.seed_runs_to_bug_rows(summaries, policies=("pct-d2", "chess-gl"))

        self.assertEqual(
            rows,
            [
                {"policy": "pct-d2", "attempt": 1, "runs": 3, "found": False},
                {"policy": "pct-d2", "attempt": 2, "runs": 2, "found": True},
                {"policy": "chess-gl", "attempt": 1, "runs": 2, "found": True},
            ],
        )

    def test_seed_runs_plot_writes_temp_output(self):
        charts = load_charts_module()
        summaries = {
            "random": {
                1: [{"policy": "random", "attempt": 1, "run_num": 11, "user_failed": True}],
                2: [{"policy": "random", "attempt": 2, "run_num": 17, "user_failed": True}],
            },
            "pct-d2": {
                1: [{"policy": "pct-d2", "attempt": 1, "run_num": 5, "user_failed": True}],
                2: [{"policy": "pct-d2", "attempt": 2, "run_num": 7, "user_failed": True}],
            },
            "pct-d3": {
                1: [{"policy": "pct-d3", "attempt": 1, "run_num": 3, "user_failed": True}],
                2: [{"policy": "pct-d3", "attempt": 2, "run_num": 4, "user_failed": True}],
            },
            "chess-gl": {
                1: [{"policy": "chess-gl", "attempt": 1, "run_num": 9, "user_failed": True}],
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            charts.fig_seed_runs_to_bug(summaries, tmpdir, title_subject="quorum repair")
            output = Path(tmpdir) / "seed_runs_to_bug.png"
            self.assertTrue(output.exists())


if __name__ == "__main__":
    unittest.main()
