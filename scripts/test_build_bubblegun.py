import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


SCRIPT = Path(__file__).with_name("build-bubblegun.py")


class BubbleGunBuildTests(unittest.TestCase):
    def test_term_stops_build_process_group(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_go = root / "go"
            heartbeat = root / "heartbeat"
            child = root / "child.py"
            child.write_text(
                "import os,time\n"
                "p=os.environ['BUBBLEGUN_TEST_HEARTBEAT']\n"
                "while True:\n"
                "    with open(p, 'w') as stream: stream.write(str(time.monotonic()))\n"
                "    time.sleep(.02)\n"
            )
            fake_go.write_text(
                f"#!{sys.executable}\n"
                "import os,subprocess,sys,time\n"
                "subprocess.Popen([sys.executable, os.environ['BUBBLEGUN_TEST_CHILD']])\n"
                "while True: time.sleep(1)\n"
            )
            fake_go.chmod(0o755)
            output = root / "out"
            env = dict(os.environ, BUBBLEGUN_TEST_HEARTBEAT=str(heartbeat),
                       BUBBLEGUN_TEST_CHILD=str(child))
            parent = subprocess.Popen(
                [sys.executable, str(SCRIPT), "--go", str(fake_go), "--output", str(output), "--timeout", "20"],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            try:
                deadline = time.monotonic() + 3
                while not heartbeat.exists() and time.monotonic() < deadline:
                    if parent.poll() is not None:
                        self.fail("fake build exited before heartbeat")
                    time.sleep(.01)
                self.assertTrue(heartbeat.exists(), "fake build descendant never started")
                parent.send_signal(signal.SIGTERM)
                self.assertEqual(parent.wait(timeout=3), 143)
                before = heartbeat.read_text()
                time.sleep(.1)
                self.assertEqual(heartbeat.read_text(), before)
                self.assertEqual(json.loads((output / "status.json").read_text())["status"], "interrupted")
            finally:
                if parent.poll() is None:
                    parent.kill()
                    parent.wait()


if __name__ == "__main__":
    unittest.main()
