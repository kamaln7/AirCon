"""Checks that a broker that is down or restarting never stops the bridge: the
connection is retried with backoff, and every connection (re)publishes what a
fresh start does.

Run with: python -m unittest tests.test_mqtt_reconnect
"""
import asyncio
from dataclasses import dataclass
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from aircon.__main__ import mqtt_loop
from aircon.mqtt_client import MqttClient

# paho sleeps in slices of at most a second, so a stop waits at most that long.
_STOP_WITHIN_SECONDS = 2.5


def _free_port():
  with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    return s.getsockname()[1]


@dataclass
class Properties:
  t_power: str = 'on'
  t_temp: int = None


class FakeDevice:
  mac_address = 'test-device'

  def __init__(self, available):
    self.available = available

  def get_all_properties(self):
    return Properties()


class FakeBroker:
  """Just enough MQTT 3.1.1 to accept a client and record what it sends."""

  def __init__(self, port):
    self.port = port
    self.events = []
    self._server = None
    self._writers = []

  async def start(self):
    self._server = await asyncio.start_server(self._serve, '127.0.0.1', self.port)

  async def stop(self):
    self._server.close()
    self.drop_clients()
    await self._server.wait_closed()

  def drop_clients(self):
    for writer in self._writers:
      writer.close()
    self._writers = []

  async def _serve(self, reader, writer):
    self._writers.append(writer)
    try:
      while True:
        first = (await reader.readexactly(1))[0]
        length, shift = 0, 0
        while True:
          byte = (await reader.readexactly(1))[0]
          length |= (byte & 0x7f) << shift
          shift += 7
          if not byte & 0x80:
            break
        body = await reader.readexactly(length)
        kind = first >> 4
        if kind == 1:  # CONNECT
          self.events.append(('connect',))
          writer.write(bytes([0x20, 0x02, 0x00, 0x00]))  # CONNACK, accepted
        elif kind == 3:  # PUBLISH, QoS 0
          topic_length = int.from_bytes(body[:2], 'big')
          self.events.append(('publish', body[2:2 + topic_length].decode(),
                              body[2 + topic_length:].decode(), bool(first & 1)))
        elif kind == 8:  # SUBSCRIBE
          topics, i = [], 2
          while i < len(body):
            topic_length = int.from_bytes(body[i:i + 2], 'big')
            topics.append(body[i + 2:i + 2 + topic_length].decode())
            i += 2 + topic_length + 1
          self.events.append(('subscribe', topics))
          writer.write(bytes([0x90, 2 + len(topics)]) + body[:2] + bytes(len(topics)))
        elif kind == 12:  # PINGREQ
          writer.write(bytes([0xd0, 0x00]))
        await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError):
      pass

  def publishes(self):
    return {(e[1], e[2], e[3]) for e in self.events if e[0] == 'publish'}


class ReconnectTest(unittest.IsolatedAsyncioTestCase):

  async def asyncSetUp(self):
    patch = mock.patch.multiple(MqttClient,
                                _RECONNECT_MIN_DELAY_SECONDS=0.05,
                                _RECONNECT_MAX_DELAY_SECONDS=0.2)
    patch.start()
    self.addCleanup(patch.stop)
    self.port = _free_port()
    topics = {
        'pub': 'hisense_ac/{}/{}/status',
        'sub': 'hisense_ac/{}/{}/command',
        'lwt': 'hisense_ac/LWT',
    }
    self.client = MqttClient('test', topics, [FakeDevice(available=True)])
    self.client.discovery_configs = {'homeassistant/climate/test-device/hvac/config': '{"a": 1}'}
    self.broker = FakeBroker(self.port)

  async def _until(self, condition, what):
    deadline = time.monotonic() + 10
    while not condition():
      self.assertLess(time.monotonic(), deadline, f'timed out waiting for {what}')
      await asyncio.sleep(0.02)

  def _published_on_connect(self):
    return {
        ('hisense_ac/LWT', 'online', True),
        ('homeassistant/climate/test-device/hvac/config', '{"a": 1}', True),
        ('hisense_ac/test-device/available/status', 'online', True),
        ('hisense_ac/test-device/t_power/status', 'on', True),
    }

  async def test_retries_until_the_broker_is_up_and_publishes_everything(self):
    self.client.connect_to_broker('127.0.0.1', self.port)
    with self.assertLogs(level='WARNING') as logs:
      task = asyncio.create_task(mqtt_loop(self.client))
      # Nothing listens: the loop is still alive, and says so on every attempt.
      await self._until(lambda: len(logs.records) >= 3, 'repeated retry warnings')
      self.assertFalse(task.done())
    self.assertIn('Cannot connect to the MQTT broker 127.0.0.1:%d' % self.port,
                  logs.records[0].getMessage())
    self.assertIn('ConnectionRefusedError', logs.records[0].getMessage())

    await self.broker.start()
    try:
      await self._until(lambda: self._published_on_connect() <= self.broker.publishes(),
                        'the state to be published')
      self.assertIn(('subscribe', ['hisense_ac/test-device/t_power/command',
                                   'hisense_ac/test-device/t_temp/command']),
                    self.broker.events)
      # No value yet: nothing to publish for it.
      self.assertNotIn('hisense_ac/test-device/t_temp/status',
                       {p[0] for p in self.broker.publishes()})

      # The broker restarts and forgets everything: the next connection says it all again.
      self.broker.events.clear()
      self.broker.drop_clients()
      await self._until(lambda: self._published_on_connect() <= self.broker.publishes(),
                        'the state to be published again after a dropped connection')
      self.assertEqual(self.broker.events[0], ('connect',))
    finally:
      task.cancel()
      with self.assertRaises(asyncio.CancelledError):
        await task
      await self.broker.stop()

  async def test_stopping_while_the_broker_is_down_returns_promptly(self):
    self.client.connect_to_broker('127.0.0.1', self.port)
    task = asyncio.create_task(mqtt_loop(self.client))
    await asyncio.sleep(0.3)
    started = time.monotonic()
    task.cancel()
    with self.assertRaises(asyncio.CancelledError):
      await task
    self.assertLess(time.monotonic() - started, _STOP_WITHIN_SECONDS)


class SigtermWhileRetryingTest(unittest.TestCase):

  def test_sigterm_during_retries_exits_promptly(self):
    http_port = _free_port()
    # Nothing listens here: the bridge is left retrying, with the real backoff.
    broker_port = _free_port()
    with tempfile.TemporaryDirectory() as tmp:
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
          str(broker_port), '--local_ip', '127.0.0.1', '--config', config, '--type', 'ac'
      ],
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.PIPE,
                                 # Logs go to stderr, one JSON record each.
                                 env={**os.environ, 'PLATFORM': 'docker'})
      try:
        deadline = time.monotonic() + 20
        while True:
          self.assertIsNone(process.poll(), 'bridge exited while the broker was down')
          try:
            socket.create_connection(('127.0.0.1', http_port), timeout=0.5).close()
            break
          except OSError:
            self.assertLess(time.monotonic(), deadline, 'bridge never listened')
            time.sleep(0.1)
        # Long enough for several attempts, so the stop lands in a backoff wait.
        time.sleep(2.5)
        self.assertIsNone(process.poll(), 'bridge exited while the broker was down')
        started = time.monotonic()
        process.send_signal(signal.SIGTERM)
        code = process.wait(timeout=10)
        elapsed = time.monotonic() - started
        stderr = process.stderr.read().decode()
      finally:
        if process.poll() is None:
          process.kill()
          process.wait()
        process.stderr.close()
      self.assertEqual(code, 0, stderr)
      self.assertLess(elapsed, _STOP_WITHIN_SECONDS)
      self.assertIn('Cannot connect to the MQTT broker', stderr)
      self.assertNotIn('Traceback', stderr)


if __name__ == '__main__':
  unittest.main()
