import asyncio
from dataclasses import fields
import enum
import logging
import paho.mqtt.client as mqtt
import sys

from .aircon import Device
from .properties import AcWorkMode, FglOperationMode, Power


class MqttClient(mqtt.Client):

  _SETTINGS_DEBOUNCE_SECONDS = 0.2
  _POWER_ON_DELAY_SECONDS = 2.0
  _MODE_FAN_RETRY_SECONDS = 2.0
  # The AC never reports back what it did with a setting, so Home Assistant
  # would otherwise show our optimistic guess until the next status sweep.
  # Read the properties we just wrote back, long enough after that the unit
  # has acted on them.
  _VERIFY_SETTINGS_SECONDS = 5.0
  _BATCHABLE_SETTINGS = {
      't_backlight',
      't_fan_leftright',
      't_fan_power',
      't_fan_speed',
      't_power',
      't_sleep',
      't_temp',
      't_work_mode',
  }

  # The broker retry delay doubles from the minimum to the maximum, forever.
  _RECONNECT_MIN_DELAY_SECONDS = 1
  _RECONNECT_MAX_DELAY_SECONDS = 30

  def __init__(self, client_id: str, mqtt_topics: dict, devices: [Device]):
    super().__init__(client_id=client_id, clean_session=True)
    self._mqtt_topics = mqtt_topics
    self._devices = devices
    self._event_loop = asyncio.get_running_loop()
    self._pending_settings = {}
    self._settings_handles = {}
    self._verify_handles = {}
    self._broker = None
    # Retained Home Assistant discovery configs (topic -> JSON payload),
    # published on every connection: the broker may have lost them.
    self.discovery_configs = {}

    self.reconnect_delay_set(min_delay=self._RECONNECT_MIN_DELAY_SECONDS,
                             max_delay=self._RECONNECT_MAX_DELAY_SECONDS)
    self.on_connect = self.mqtt_on_connect
    self.on_connect_fail = self.mqtt_on_connect_fail
    self.on_disconnect = self.mqtt_on_disconnect
    self.on_message = self.mqtt_on_message

  def connect_to_broker(self, host: str, port: int) -> None:
    """Connect once the network loop runs, retrying with backoff until it works.

    Returns at once, so the broker being down or restarting at start never
    stops the program: the loop thread started by loop_start() makes the
    attempts (paho's loop_start retries the first connection, and reconnects
    after a lost one, the same way) and on_connect publishes the state.
    """
    self._broker = f'{host}:{port}'
    self.connect_async(host, port)

  def mqtt_on_connect_fail(self, client: mqtt.Client, userdata):
    # Called by paho from its except block, so the cause is the active exception.
    logging.warning('Cannot connect to the MQTT broker %s: %r; retrying, backing off up to %s s',
                    self._broker,
                    sys.exc_info()[1],
                    self._RECONNECT_MAX_DELAY_SECONDS)

  def mqtt_on_disconnect(self, client: mqtt.Client, userdata, rc):
    if rc != 0:
      logging.warning('Lost the connection to the MQTT broker %s (rc=%s); reconnecting',
                      self._broker, rc)

  def mqtt_on_connect(self, client: mqtt.Client, userdata, flags, rc):
    if rc != 0:
      logging.warning('The MQTT broker %s refused the connection (rc=%s); retrying', self._broker,
                      rc)
      return
    logging.info('Connected to the MQTT broker %s', self._broker)

    # A session is clean, so the broker has forgotten the subscriptions, and a
    # restarted broker may have lost the retained messages too: send everything
    # a fresh start sends.
    client.publish(self._mqtt_topics['lwt'], payload='online', retain=True)
    for topic, payload in self.discovery_configs.items():
      client.publish(topic, payload=payload, retain=True)

    for device in self._devices:
      client.subscribe([(self._mqtt_topics['sub'].format(device.mac_address, data_field.name), 0)
                        for data_field in fields(device.get_all_properties())])
    # Subscribe to subscription updates.
    client.subscribe('$SYS/broker/log/M/subscribe/#')

    # Publish current status of all properties for available devices.
    for device in self._devices:
      self.mqtt_publish_update(device.mac_address,
                               'available',
                               'online' if device.available else 'offline',
                               retain=True)
      if device.available:
        properties = device.get_all_properties()
        for data_field in fields(properties):
          value = getattr(properties, data_field.name, None)
          if value is not None:
            self.mqtt_publish_update(device.mac_address, data_field.name, value, retain=True)

  def mqtt_on_message(self, client: mqtt.Client, userdata, message: mqtt.MQTTMessage):
    logging.info('MQTT message Topic: {}, Payload {}'.format(message.topic, message.payload))
    if message.topic.startswith('$SYS/broker/log/M/subscribe'):
      return self.mqtt_on_subscribe(message.payload)
    mac_address = message.topic.rsplit('/', 3)[1]
    prop_name = message.topic.rsplit('/', 3)[2]
    payload = message.payload.decode('utf-8')
    if prop_name == 't_work_mode':
      if payload == 'fan_only':
        payload = 'FAN'

    for device in self._devices:
      if device.mac_address != mac_address:
        continue
      chosen_device = device

    try:
      if hasattr(chosen_device, 'queue_settings') and prop_name in self._BATCHABLE_SETTINGS:
        self._event_loop.call_soon_threadsafe(
            self._debounce_setting, chosen_device, prop_name, payload.upper())
      else:
        self._event_loop.call_soon_threadsafe(
            self._queue_command, chosen_device, prop_name, payload.upper())
    except Exception:
      logging.exception('Failed to parse value %s for property %s', payload, prop_name)

  def _debounce_setting(self, device: Device, prop_name: str, value) -> None:
    updates = {prop_name: value}
    if prop_name == 't_work_mode':
      updates = ({'t_power': 'OFF'} if value == 'OFF'
                 else ({'t_power': 'ON', 't_work_mode': value}
                       if device.get_power() == Power.OFF
                       else {'t_work_mode': value}))

    pending = self._pending_settings.setdefault(device.mac_address, {})
    pending.update(updates)
    handle = self._settings_handles.pop(device.mac_address, None)
    if handle:
      handle.cancel()
    self._settings_handles[device.mac_address] = self._event_loop.call_later(
        self._SETTINGS_DEBOUNCE_SECONDS, self._flush_settings, device)

  def _flush_settings(self, device: Device) -> None:
    self._settings_handles.pop(device.mac_address, None)
    settings = self._pending_settings.pop(device.mac_address)
    if (settings.get('t_power') == 'ON' and device.get_power() == Power.OFF and
        len(settings) > 1):
      self._queue_settings(device, {'t_power': 'ON'})
      remaining = {name: value for name, value in settings.items() if name != 't_power'}
      self._event_loop.call_later(
          self._POWER_ON_DELAY_SECONDS, self._queue_settings, device, remaining)
      return
    self._queue_settings(device, settings)

  def _queue_settings(self, device: Device, settings: dict, retry_mode_fan: bool = True) -> None:
    try:
      device.queue_settings(settings)
    except Exception:
      logging.exception('Failed to queue combined settings for %s', device.mac_address)
      return
    self._verify_settings_later(device, settings)
    if (retry_mode_fan and 't_fan_speed' in settings and
        settings.get('t_work_mode') not in (None, device.get_work_mode().name)):
      self._event_loop.call_later(
          self._MODE_FAN_RETRY_SECONDS, self._queue_settings, device, settings, False)

  def _verify_settings_later(self, device: Device, settings: dict) -> None:
    """Ask the AC what these properties actually ended up as.

    It silently clamps or ignores some writes -- the mode/fan retry above
    exists because of one such case -- and it only volunteers state when we
    have something queued for it. Without this the optimistic value stands
    until the next sweep.
    """
    handle = self._verify_handles.pop(device.mac_address, None)
    if handle:
      handle.cancel()
    self._verify_handles[device.mac_address] = self._event_loop.call_later(
        self._VERIFY_SETTINGS_SECONDS, self._verify_settings, device, tuple(settings))

  def _verify_settings(self, device: Device, names: tuple) -> None:
    self._verify_handles.pop(device.mac_address, None)
    try:
      device.queue_status(names)
    except Exception:
      logging.exception('Failed to queue verification read for %s', device.mac_address)

  @staticmethod
  def _queue_command(device: Device, prop_name: str, value) -> None:
    try:
      device.queue_command(prop_name, value)
    except Exception:
      logging.exception('Failed to queue value %s for property %s', value, prop_name)

  def mqtt_on_subscribe(self, payload: bytes):
    # The last segment in the space delimited string is the topic.
    topic = payload.decode('utf-8').rsplit(' ', 1)[-1]
    if topic not in self._mqtt_topics['pub']:
      return
    mac_address = topic.rsplit('/', 3)[1]
    prop_name = topic.rsplit('/', 3)[2]

    for device in self._devices:
      if device.mac_address != mac_address:
        continue
      chosen_device = device

    self.mqtt_publish_update(chosen_device.mac_address,
                             prop_name,
                             chosen_device.get_property(prop_name),
                             retain=True)

  def mqtt_publish_update(self,
                          mac_address: str,
                          property_name: str,
                          value,
                          retain: bool = True) -> None:
    if isinstance(value, enum.Enum):
      payload = 'fan_only' if (value is AcWorkMode.FAN or
                               value is FglOperationMode.FAN) else value.name.lower()
    else:
      payload = str(value)
    self.publish(self._mqtt_topics['pub'].format(mac_address, property_name),
                 payload=payload.encode('utf-8'),
                 retain=retain)
