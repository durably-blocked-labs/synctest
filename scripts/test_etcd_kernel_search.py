import importlib.util
from pathlib import Path
import struct
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("etcd-kernel-search.py")
SPEC = importlib.util.spec_from_file_location("etcd_kernel_search", SCRIPT)
SEARCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SEARCH)


class KernelOracleTests(unittest.TestCase):
    def test_cycle_requires_both_blocked_functions_and_timeout(self):
        dump = (
            "panic: test timed out after 2s\n"
            "goroutine 10 [sync.Mutex.Lock, synctest bubble 1]:\n"
            "etcd6873.(*watchBroadcasts).coalesce(0x1)\n"
            "goroutine 11 [chan receive (durable), synctest bubble 1]:\n"
            "etcd6873.(*watchBroadcasts).stop(0x1)\n"
        )
        self.assertTrue(SEARCH.cycle_6873(dump))
        self.assertFalse(SEARCH.cycle_6873(dump.replace("panic: test timed out", "panic: unrelated")))
        self.assertFalse(SEARCH.cycle_6873(dump.replace(".coalesce(", ".update(")))
        self.assertFalse(SEARCH.cycle_6873(dump.replace("[chan receive", "[runnable")))
        self.assertFalse(SEARCH.cycle_6873(dump.replace(".stop(0x1)", ".stop(0x2)")))

    def test_saved_choices_reject_invalid_count_and_option(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "choices.bin"
            data = bytearray(8 + 1024 * 72)
            data[:4] = b"BKC1"
            struct.pack_into("<I", data, 4, 1)
            struct.pack_into("<IIIII", data, 8, 1, 3, 1, 3, 4)
            path.write_bytes(data)
            self.assertEqual(SEARCH.choices_from(path), [{"bgids": [1, 3, 4], "chosen": 1}])
            struct.pack_into("<I", data, 8, 3)
            path.write_bytes(data)
            with self.assertRaises(ValueError):
                SEARCH.choices_from(path)
            struct.pack_into("<I", data, 4, 1025)
            path.write_bytes(data)
            with self.assertRaises(ValueError):
                SEARCH.choices_from(path)

    def test_branch_changes_one_decision_after_inherited_prefix(self):
        choices = [{"bgids": [1, 3], "chosen": 0},
                   {"bgids": [3, 4], "chosen": 1}]
        self.assertEqual(list(SEARCH.branch(choices, 1)),
                         [[{"bgids": [1, 3], "chosen": 0},
                           {"bgids": [3, 4], "chosen": 0}]])


if __name__ == "__main__":
    unittest.main()
