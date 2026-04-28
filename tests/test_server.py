import subprocess
import sys
import time
import unittest
import urllib.request
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"


class ServerSmokeTests(unittest.TestCase):
    def test_server_starts_and_serves_root_and_status(self):
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8012"],
            cwd=BACKEND_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.time() + 15
            while time.time() < deadline:
                try:
                    with urllib.request.urlopen("http://127.0.0.1:8012/", timeout=2) as response:
                        self.assertEqual(response.status, 200)
                    with urllib.request.urlopen("http://127.0.0.1:8012/api/grid/status", timeout=2) as response:
                        self.assertEqual(response.status, 200)
                    break
                except Exception:
                    time.sleep(0.5)
            else:
                self.fail("Server did not start successfully within timeout")
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    unittest.main()
