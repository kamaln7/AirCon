import aiohttp
import asyncio
from dataclasses import dataclass
from http import HTTPStatus
import json
import logging
import socket
import time

from .aircon import Device


@dataclass
class _NotifyConfiguration:
  device: Device
  headers: dict
  event: asyncio.Event
  last_attempt: float = 0.0
  # Seconds to wait before the next registration. Grows while the AC is
  # unreachable, resets to _KEEP_ALIVE_INTERVAL once it answers again.
  interval: float = 0.0
  offline_logged: bool = False


class Notifier:
  _KEEP_ALIVE_INTERVAL = 10.0
  _TIME_TO_HANDLE_REQUESTS = 100e-3
  # An AC switched off at the wall is a normal steady state here, not an error.
  # Back off to one probe a minute rather than retrying it to death.
  _OFFLINE_RETRY_MAX = 60.0
  # The AC opens its own connection back to us; if it stops doing that we can no
  # longer see or command it, however happily it keeps ACKing our registration.
  _CALLBACK_TIMEOUT = 60.0

  def __init__(self, port: int, local_ip: str):
    # Keyed by device so notify() can wake exactly one keep-alive loop.
    self._configurations = {}
    self._running = False
    self._loop = asyncio.get_running_loop()

    local_ip = local_ip or self._get_local_ip()
    self._local_reg = {'ip': local_ip, 'notify': 0, 'port': port, 'uri': '/local_lan'}

  def _get_local_ip(self):
    sock = None
    try:
      sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
      sock.connect(('10.255.255.255', 1))
      return sock.getsockname()[0]
    finally:
      if sock:
        sock.close()

  def register_device(self, device: Device):
    if device in self._configurations:
      return
    headers = {
        'Accept': 'application/json',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Host': device.ip_address,
        'Accept-Encoding': 'gzip'
    }
    self._configurations[device] = _NotifyConfiguration(device, headers, asyncio.Event())

  def notify(self, device: Device):
    """Wake up just this device's keep-alive loop. Safe from any thread."""
    config = self._configurations.get(device)
    if config:
      self._loop.call_soon_threadsafe(config.event.set)

  async def start(self, session: aiohttp.ClientSession):
    self._running = True
    await asyncio.gather(
        *(self._keep_alive(session, config) for config in self._configurations.values()))

  async def stop(self):
    self._running = False
    for config in self._configurations.values():
      config.event.set()

  async def _keep_alive(self, session: aiohttp.ClientSession, config: _NotifyConfiguration):
    """One loop per device, so a unit that is off at the wall only delays itself."""
    while self._running:
      config.event.clear()
      try:
        queue_size = await self._perform_request(session, config)
      except Exception:
        logging.exception('[KeepAlive] %s: unexpected error', config.device.ip_address)
        queue_size = 0
      if queue_size > 1:
        # The AC only takes one command per callback; keep prodding it to drain.
        await asyncio.sleep(self._TIME_TO_HANDLE_REQUESTS)
        continue
      try:
        await asyncio.wait_for(config.event.wait(),
                               timeout=config.interval or self._KEEP_ALIVE_INTERVAL)
      except asyncio.TimeoutError:
        pass

  async def _perform_request(self, session: aiohttp.ClientSession,
                             config: _NotifyConfiguration) -> int:
    now = time.time()
    device = config.device
    queue_size = device.commands_queue.qsize()
    if queue_size == 0 and now - config.last_attempt < config.interval:
      return 0
    config.last_attempt = now

    method = 'PUT' if device.available else 'POST'
    payload = {'local_reg': {**self._local_reg, 'notify': int(queue_size > 0)}}
    url = f'http://{device.ip_address}/local_reg.json'
    logging.debug(f'[KeepAlive] Sending {method} {url} {json.dumps(payload)}')
    try:
      async with session.request(method, url, json=payload, headers=config.headers) as resp:
        if resp.status != HTTPStatus.ACCEPTED.value:
          raise ConnectionError(f'{resp.status}, {await resp.text()}')
    except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as e:
      self._mark_offline(config, e)
      return 0

    config.interval = self._KEEP_ALIVE_INTERVAL
    if config.offline_logged:
      logging.info('[KeepAlive] %s is reachable again.', device.ip_address)
      config.offline_logged = False
    silence = now - device.last_callback
    if silence > self._CALLBACK_TIMEOUT:
      # It accepts our registration but never calls back, so whatever Home
      # Assistant is showing is stale. Say so, and re-register (POST) next time.
      if device.available:
        logging.error('[KeepAlive] %s accepts registrations but has not called back in %.0fs.',
                      device.ip_address, silence)
      device.available = False
    return queue_size

  def _mark_offline(self, config: _NotifyConfiguration, error: Exception):
    device = config.device
    device.available = False
    config.interval = min(max(config.interval * 2, self._KEEP_ALIVE_INTERVAL),
                          self._OFFLINE_RETRY_MAX)
    message = '[KeepAlive] Failed to reach %s, retrying in %.0fs: %r'
    args = (device.ip_address, config.interval, error)
    if config.offline_logged:
      logging.debug(message, *args)  # Powered off for months is normal; do not spam.
    else:
      logging.error(message, *args)
      config.offline_logged = True
