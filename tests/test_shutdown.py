"""Checks that SIGTERM stops the bridge promptly and cleanly, as a container
runtime sends it: as PID 1 a process without a handler ignores it, and the
runtime waits out its grace period before SIGKILL.

Run with: python -m unittest tests.test_shutdown
"""
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

# Well inside a container runtime's default 10 s stop grace.
_EXIT_WITHIN_SECONDS = 3.0


class SigtermTest(unittest.TestCase):

  def _free_port(self):
    with socket.socket() as s:
      s.bind(('127.0.0.1', 0))
      return s.getsockname()[1]

  def _wait_until_listening(self, process, port):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
      self.assertIsNone(process.poll(), 'bridge exited before it listened')
      try:
        socket.create_connection(('127.0.0.1', port), timeout=0.5).close()
        return
      except OSError:
        time.sleep(0.1)
    self.fail('bridge never listened')

  def _run_and_signal(self, sig):
    http_port = self._free_port()
    # A broker that accepts and says nothing: the client only has to connect.
    with socket.socket() as broker, tempfile.TemporaryDirectory() as tmp:
      broker.bind(('127.0.0.1', 0))
      broker.listen()
      config = os.path.join(tmp, 'config_test.json')
      with open(config, 'w') as f:
        json.dump(
            {
                'name': 'test',
                'app': 'test',
                'model': 'test',
                'sw_version': 'test',
                'dsn': 'test',
                'temp_type': 'C',
                'mac_address': 'test-device',
                'ip_address': '127.0.0.1',
                'lanip_key': 'test-key',
                'lanip_key_id': 1,
            }, f)
      process = subprocess.Popen([
          sys.executable, '-m', 'aircon', '--log_level', 'WARNING', 'run', '--port',
          str(http_port), '--mqtt_host', '127.0.0.1', '--mqtt_port',
          str(broker.getsockname()[1]), '--local_ip', '127.0.0.1', '--config', config, '--type',
          'ac'
      ],
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.PIPE)
      try:
        self._wait_until_listening(process, http_port)
        started = time.monotonic()
        process.send_signal(sig)
        code = process.wait(timeout=_EXIT_WITHIN_SECONDS)
        elapsed = time.monotonic() - started
        stderr = process.stderr.read().decode()
      finally:
        if process.poll() is None:
          process.kill()
          process.wait()
        process.stderr.close()
      self.assertEqual(code, 0, stderr)
      self.assertLess(elapsed, _EXIT_WITHIN_SECONDS)
      self.assertNotIn('Traceback', stderr)

  def test_sigterm_exits_cleanly(self):
    self._run_and_signal(signal.SIGTERM)

  def test_sigint_exits_cleanly(self):
    self._run_and_signal(signal.SIGINT)


if __name__ == '__main__':
  unittest.main()
