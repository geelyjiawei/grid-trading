import socket
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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]

        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=BACKEND_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.time() + 15
            while time.time() < deadline:
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/",
                        timeout=2,
                    ) as response:
                        self.assertEqual(response.status, 200)
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/api/grid/status",
                        timeout=2,
                    ) as response:
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
