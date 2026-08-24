"""The status sweep is the only thing that refreshes AC state, so its size
and contents matter more than they look."""
import unittest

from aircon.aircon import AcDevice
from aircon.properties import AcProperties


def _device():
  return AcDevice(
      {
          'name': 'test',
          'app': 'test',
          'model': 'test',
          'sw_version': 'test',
          'mac_address': 'test-device',
          'ip_address': '127.0.0.1',
          'temp_type': 'C',
          'lanip_key': 'test-key',
          'lanip_key_id': 1,
      }, lambda device: None)


class StatusSweepTest(unittest.TestCase):

  def _queued_names(self, device):
    names = []
    while not device.commands_queue.empty():
      command = device.commands_queue.get_nowait()
      resource = command.command['cmds'][0]['cmd']['resource']
      names.append(resource.split('name=')[1])
    return names

  def test_full_sweep_covers_every_property(self):
    device = _device()
    device.queue_status()
    self.assertEqual(len(self._queued_names(device)), len(AcProperties.__dataclass_fields__))

  def test_hot_sweep_is_only_what_home_assistant_shows(self):
    device = _device()
    device.queue_status(device.hot_properties())
    names = self._queued_names(device)
    self.assertCountEqual(names, device.topics.values())
    # The whole point: room temperature refreshes without dragging the
    # fault flags along.
    self.assertIn('f_temp_in', names)
    self.assertNotIn('f_e_inkeys', names)
    self.assertLess(len(names), len(AcProperties.__dataclass_fields__) / 2)


if __name__ == '__main__':
  unittest.main()
