#!/usr/bin/env python3
"""Issue #285: healthy real cluster, then the factory's simultaneous SIGINT.

Build tigerstore + factory and generate certs first. Set TIGERSTORE,
STORAGE_TEST_FACTORY, STORAGE_TEST_CERTS. No Kubernetes module is needed.
"""
import os
from pathlib import Path
import selectors
import signal
import ssl
import subprocess
import tempfile
import time
import unittest
import urllib.request


class FactoryStop(unittest.TestCase):
    def test_simultaneous_sigint(self):
        certs = Path(os.environ["STORAGE_TEST_CERTS"])
        tls = ssl.create_default_context(cafile=str(certs / "ca.pem"))
        tls.load_cert_chain(certs / "client.pem", certs / "client-key.pem")
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=tls)
        )
        # Repetition covers elections choosing different leaders; not a campaign.
        for attempt in range(3):
            with self.subTest(attempt=attempt), tempfile.TemporaryDirectory() as work:
                env = dict(os.environ, STORAGE_TEST_DATA_DIR=work)
                with tempfile.TemporaryFile(mode="w+") as log:
                    factory = subprocess.Popen(
                        [env["STORAGE_TEST_FACTORY"]], env=env,
                        stdout=subprocess.PIPE, stderr=log, text=True,
                    )
                    try:
                        with selectors.DefaultSelector() as ready:
                            ready.register(factory.stdout, selectors.EVENT_READ)
                            self.assertTrue(ready.select(10), "factory did not print endpoints")
                        endpoints = factory.stdout.readline().strip().split(",")
                        self.assertEqual(len(endpoints), 3)
                        for endpoint in endpoints:
                            deadline = time.monotonic() + 10
                            while True:
                                try:
                                    with opener.open(endpoint + "/health", timeout=1) as r:
                                        self.assertIn(b'"health":"true"', r.read())
                                    break
                                except (OSError, urllib.error.URLError):
                                    if time.monotonic() >= deadline:
                                        raise
                                    time.sleep(0.05)
                        start = time.monotonic()
                        factory.send_signal(signal.SIGINT)
                        code = factory.wait(timeout=10)  # adapter's existing outer bound
                        elapsed = time.monotonic() - start
                        print(f"attempt {attempt}: factory exit={code}, stop={elapsed:.3f}s", flush=True)
                        self.assertEqual(code, 0, "factory killed a normally stopping node")
                    finally:
                        if factory.poll() is None:
                            factory.kill()
                        factory.wait()
                        factory.stdout.close()
                        log.seek(0)
                        print(log.read(), flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
