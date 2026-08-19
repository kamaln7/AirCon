import asyncio
import threading
import unittest

from aircon import control_value
from aircon.__main__ import mqtt_loop
from aircon.aircon import AcDevice
from aircon.mqtt_client import MqttClient
from aircon.properties import AcWorkMode, Dimmer, FanSpeed, Power, SleepMode


class FakeDevice:
  mac_address = 'test-device'

  def __init__(self):
    self.commands = []
    self.settings = []
    self.power = Power.OFF
    self.work_mode = AcWorkMode.COOL

  def get_power(self):
    return self.power

  def get_work_mode(self):
    return self.work_mode

  def queue_command(self, name, value):
    self.commands.append((name, value))

  def queue_settings(self, settings):
    self.settings.append(settings)


class Message:
  def __init__(self, prop_name, payload):
    self.topic = f'hisense_ac/test-device/{prop_name}/command'
    self.payload = payload.encode()


class FakeNetworkLoop:
  def __init__(self):
    self.started = False
    self.stopped = False

  def loop_start(self):
    self.started = True

  def loop_stop(self):
    self.stopped = True


class MqttClientTest(unittest.IsolatedAsyncioTestCase):

  async def asyncSetUp(self):
    self.device = FakeDevice()
    topics = {
        'pub': 'hisense_ac/{}/{}/status',
        'sub': 'hisense_ac/{}/{}/command',
    }
    self.client = MqttClient('test', topics, [self.device])

  async def test_push_callbacks_batch_rapid_settings(self):
    self.device.power = Power.ON
    def publish():
      self.client.mqtt_on_message(None, None, Message('t_work_mode', 'cool'))
      self.client.mqtt_on_message(None, None, Message('t_temp', '71.6'))
      self.client.mqtt_on_message(None, None, Message('t_fan_speed', 'auto'))

    thread = threading.Thread(target=publish)
    thread.start()
    thread.join()

    await asyncio.sleep(0.1)
    self.assertEqual([], self.device.settings)
    await asyncio.sleep(0.2)
    self.assertEqual([{
        't_work_mode': 'COOL',
        't_temp': '71.6',
        't_fan_speed': 'AUTO',
    }], self.device.settings)

  async def test_power_and_mode_fan_changes_are_sequenced(self):
    self.client._POWER_ON_DELAY_SECONDS = 0.05
    self.client._MODE_FAN_RETRY_SECONDS = 0.05
    self.device.work_mode = AcWorkMode.HEAT
    self.client.mqtt_on_message(None, None, Message('t_work_mode', 'cool'))
    self.client.mqtt_on_message(None, None, Message('t_temp', '71.6'))
    self.client.mqtt_on_message(None, None, Message('t_fan_speed', 'low'))

    await asyncio.sleep(0.4)
    remaining = {
        't_work_mode': 'COOL',
        't_temp': '71.6',
        't_fan_speed': 'LOW',
    }
    self.assertEqual([{'t_power': 'ON'}, remaining, remaining], self.device.settings)

  async def test_isolated_setting_stays_brief(self):
    self.client.mqtt_on_message(None, None, Message('t_fan_speed', 'medium'))

    await asyncio.sleep(0.1)
    self.assertEqual([], self.device.settings)
    await asyncio.sleep(0.2)
    self.assertEqual([{'t_fan_speed': 'MEDIUM'}], self.device.settings)

  async def test_work_mode_does_not_redundantly_power_on_running_device(self):
    self.device.power = Power.ON
    self.client.mqtt_on_message(None, None, Message('t_work_mode', 'heat'))
    self.client.mqtt_on_message(None, None, Message('t_temp', '75.2'))

    await asyncio.sleep(0.3)
    self.assertEqual([{
        't_work_mode': 'HEAT',
        't_temp': '75.2',
    }], self.device.settings)

  async def test_network_loop_is_push_driven_and_stops_on_cancel(self):
    network_loop = FakeNetworkLoop()
    task = asyncio.create_task(mqtt_loop(network_loop))
    await asyncio.sleep(0)
    self.assertTrue(network_loop.started)

    task.cancel()
    with self.assertRaises(asyncio.CancelledError):
      await task
    self.assertTrue(network_loop.stopped)

  async def test_non_batchable_command_returns_to_event_loop(self):
    self.client.mqtt_on_message(None, None, Message('t_eco', 'on'))
    await asyncio.sleep(0)
    self.assertEqual([('t_eco', 'ON')], self.device.commands)


class AcDeviceSettingsTest(unittest.TestCase):

  def setUp(self):
    config = {
        'name': 'Test AC',
        'app': 'test',
        'model': 'test',
        'sw_version': 'test',
        'mac_address': 'test-device',
        'ip_address': '127.0.0.1',
        'temp_type': 'F',
        'lanip_key': 'test-key',
        'lanip_key_id': 1,
    }
    self.device = AcDevice(config, lambda: None)
    self.device.update_property('t_temp', 72)

  def pop_command(self, settings):
    self.device.queue_settings(settings)
    command = self.device.commands_queue.get_nowait()
    properties = {
        item['property']['name']: item['property']['value']
        for item in command.command['properties']
    }
    return command, properties

  def test_partial_batch_preserves_fahrenheit_temperature(self):
    self.device.update_property('t_control_value', control_value.set_temp(0, 22))
    _, properties = self.pop_command({'t_fan_speed': 'MEDIUM'})
    control = properties['t_control_value']

    self.assertEqual(22, control_value.get_temp(control))
    self.assertEqual(FanSpeed.MEDIUM, control_value.get_fan_speed(control))

  def test_combined_command_updates_control_and_separate_properties(self):
    self.device.update_property('t_control_value', control_value.set_temp(0, 22))
    command, properties = self.pop_command({
        't_power': 'ON',
        't_work_mode': 'COOL',
        't_temp': '71.6',
        't_sleep': 'ONE',
        't_backlight': 'ON',
    })
    control = properties['t_control_value']

    self.assertEqual(Power.ON, control_value.get_power(control))
    self.assertEqual(AcWorkMode.COOL, control_value.get_work_mode(control))
    self.assertEqual(22, control_value.get_temp(control))
    self.assertEqual(SleepMode.ONE.value, properties['t_sleep'])
    self.assertEqual(Dimmer.ON.value, properties['t_backlight'])

    command.updater()
    self.assertEqual(72, self.device.get_property('t_temp'))
    self.assertEqual(SleepMode.ONE, self.device.get_property('t_sleep'))
    self.assertEqual(Dimmer.ON, self.device.get_property('t_backlight'))

  def test_batch_without_packed_control_uses_individual_commands(self):
    self.device.queue_settings({
        't_temp': '71.6',
        't_fan_speed': 'MEDIUM',
        't_sleep': 'ONE',
        't_backlight': 'ON',
        't_power': 'ON',
        't_work_mode': 'COOL',
    })

    commands = [self.device.commands_queue.get_nowait() for _ in range(6)]
    properties = [{
        item['property']['name']: item['property']['value']
        for item in command.command['properties']
    } for command in commands]
    self.assertEqual([
        {'t_power': Power.ON.value},
        {'t_work_mode': AcWorkMode.COOL.value},
        {'t_temp': 72},
        {'t_fan_speed': FanSpeed.MEDIUM.value},
        {'t_sleep': SleepMode.ONE.value},
        {'t_backlight': Dimmer.ON.value},
    ], properties)

    for command in commands:
      command.updater()
    self.assertEqual(72, self.device.get_property('t_temp'))

  def test_batch_uses_native_setmulti_value(self):
    self.device.update_property('t_setmulti_value', 0)
    self.device.queue_settings({
        't_power': 'ON',
        't_work_mode': 'COOL',
        't_temp': '69.8',
        't_fan_speed': 'HIGH',
        't_fan_power': 'ON',
        't_fan_leftright': 'OFF',
        't_sleep': 'ONE',
    })

    commands = [self.device.commands_queue.get_nowait() for _ in range(3)]
    properties = [{
        item['property']['name']: item['property']['value']
        for item in command.command['properties']
    } for command in commands]
    self.assertEqual([
        {'t_power': Power.ON.value},
        {'t_setmulti_value': 0x014682},
        {'t_sleep': SleepMode.ONE.value},
    ], properties)


if __name__ == '__main__':
  unittest.main()
