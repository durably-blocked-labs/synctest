import importlib.util
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("MPLBACKEND", "Agg")


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


if __name__ == "__main__":
    unittest.main()
