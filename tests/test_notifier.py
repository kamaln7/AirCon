"""Checks that one unreachable AC cannot hold up the others.

Run with: python -m unittest tests.test_notifier
"""
import asyncio
import time
import unittest

from aircon.aircon import AcDevice
from aircon.notifier import Notifier


class FakeResponse:

  def __init__(self, status=202):
    self.status = status

  async def text(self):
    return ''

  async def __aenter__(self):
    return self

  async def __aexit__(self, *exc):
    return False


class FakeSession:
  """Answers 202 for every IP except the ones in `dead`, which hang then fail."""

  def __init__(self, dead=(), dead_delay=0.0):
    self._dead = set(dead)
    self._dead_delay = dead_delay
    self.calls = []

  def request(self, method, url, **kwargs):
    ip = url.split('//', 1)[1].split('/', 1)[0]
    notify = kwargs['json']['local_reg']['notify']
    self.calls.append((time.monotonic(), method, ip, notify))
    return self._respond(ip)

  def _respond(self, ip):
    session = self

    class _Ctx:

      async def __aenter__(self):
        if ip in session._dead:
          await asyncio.sleep(session._dead_delay)
          raise ConnectionRefusedError(f'{ip} is off at the wall')
        return FakeResponse()

      async def __aexit__(self, *exc):
        return False

    return _Ctx()


class FakeQueue:

  def __init__(self):
    self.size = 0

  def qsize(self):
    return self.size


class FakeDevice:

  def __init__(self, ip):
    self.ip_address = ip
    self.available = False
    self.last_callback = time.time()
    self.commands_queue = FakeQueue()

  def __hash__(self):
    return hash(self.ip_address)


async def _run_briefly(notifier, session, seconds):
  task = asyncio.ensure_future(notifier.start(session))
  await asyncio.sleep(seconds)
  await notifier.stop()
  task.cancel()
  try:
    await task
  except asyncio.CancelledError:
    pass


class NotifierTest(unittest.IsolatedAsyncioTestCase):

  def _notifier(self, devices):
    notifier = Notifier(8888, '10.0.0.1')
    for device in devices:
      notifier.register_device(device)
    return notifier

  async def test_command_to_healthy_unit_is_not_held_behind_a_dead_one(self):
    """The reported bug: a command took 17.5s because two units were off.

    The dead unit hangs for the whole test. A command queued for the healthy
    unit must still reach it, with notify=1, more or less immediately.
    """
    dead, live = FakeDevice('192.168.20.20'), FakeDevice('192.168.20.28')
    notifier = self._notifier([dead, live])
    session = FakeSession(dead=['192.168.20.20'], dead_delay=30.0)

    task = asyncio.ensure_future(notifier.start(session))
    await asyncio.sleep(0.05)
    live.commands_queue.size = 1  # Home Assistant sends a command.
    notifier.notify(live)
    await asyncio.sleep(0.05)
    await notifier.stop()
    task.cancel()
    try:
      await task
    except asyncio.CancelledError:
      pass

    notified = [c for c in session.calls if c[2] == live.ip_address and c[3] == 1]
    self.assertTrue(notified, 'the healthy unit was never told it has a command')
    # The dead unit is still hanging in its own loop; nobody waited for it.
    self.assertEqual(1, len([c for c in session.calls if c[2] == dead.ip_address]))

  async def test_offline_device_backs_off_instead_of_hammering(self):
    dead = FakeDevice('192.168.20.20')
    notifier = self._notifier([dead])
    session = FakeSession(dead=['192.168.20.20'])

    await _run_briefly(notifier, session, 0.2)

    # One immediate attempt, then it waits _KEEP_ALIVE_INTERVAL before the next.
    self.assertEqual(1, len(session.calls))
    self.assertEqual(Notifier._KEEP_ALIVE_INTERVAL, notifier._configurations[dead].interval)
    self.assertTrue(notifier._configurations[dead].offline_logged)

  async def test_backoff_grows_to_the_cap_and_resets_on_success(self):
    device = FakeDevice('192.168.20.20')
    notifier = self._notifier([device])
    config = notifier._configurations[device]
    for _ in range(10):
      notifier._mark_offline(config, ConnectionRefusedError())
    self.assertEqual(Notifier._OFFLINE_RETRY_MAX, config.interval)
    self.assertFalse(device.available)

    config.last_attempt = 0.0
    await notifier._perform_request(FakeSession(), config)
    self.assertEqual(Notifier._KEEP_ALIVE_INTERVAL, config.interval)
    self.assertFalse(config.offline_logged)

  async def test_silent_device_is_unavailable_even_though_it_answers_us(self):
    """Registration succeeds but the AC never calls back: HA must not show it live."""
    device = FakeDevice('192.168.20.28')
    device.available = True
    device.last_callback = time.time() - Notifier._CALLBACK_TIMEOUT - 1
    notifier = self._notifier([device])

    await notifier._perform_request(FakeSession(), notifier._configurations[device])

    self.assertFalse(device.available)

  async def test_queueing_a_command_wakes_that_device(self):
    """Guards the aircon.py plumbing: a device must hand itself to notify()."""
    notifier = Notifier(8888, '10.0.0.1')
    device = AcDevice(
        {
            'name': 'Test AC',
            'app': 'test',
            'model': 'test',
            'sw_version': 'test',
            'mac_address': 'test-device',
            'ip_address': '192.168.20.28',
            'temp_type': 'C',
            'lanip_key': 'test-key',
            'lanip_key_id': 1,
        }, notifier.notify)
    notifier.register_device(device)

    device.queue_command('t_temp', '25')
    await asyncio.sleep(0)

    self.assertTrue(notifier._configurations[device].event.is_set())

  async def test_notify_wakes_only_the_named_device(self):
    one, two = FakeDevice('192.168.20.28'), FakeDevice('192.168.20.23')
    notifier = self._notifier([one, two])
    notifier.notify(one)
    await asyncio.sleep(0)
    self.assertTrue(notifier._configurations[one].event.is_set())
    self.assertFalse(notifier._configurations[two].event.is_set())


if __name__ == '__main__':
  unittest.main()
