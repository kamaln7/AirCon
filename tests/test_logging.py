"""Checks the JSON log format a container's collector reads.

Run with: python -m unittest tests.test_logging
"""
import io
import json
import logging
import unittest

from aircon.__main__ import JsonFormatter


class JsonFormatterTest(unittest.TestCase):

  def _format(self, level, msg, *args, exc=False):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.Logger('test')
    logger.addHandler(handler)
    if exc:
      try:
        raise ValueError('boom')
      except ValueError:
        logger.log(level, msg, *args, exc_info=True)
    else:
      logger.log(level, msg, *args)
    output = stream.getvalue()
    self.assertEqual(output.count('\n'), 1)
    return json.loads(output)

  def test_level_is_lowercase_name(self):
    for level, name in [(logging.DEBUG, 'debug'), (logging.INFO, 'info'),
                        (logging.WARNING, 'warning'), (logging.ERROR, 'error'),
                        (logging.CRITICAL, 'critical')]:
      self.assertEqual(self._format(level, 'x')['level'], name)

  def test_message_and_caller(self):
    record = self._format(logging.WARNING, 'AC %s offline', 'kitchen')
    self.assertEqual(record['message'], 'AC kitchen offline')
    self.assertRegex(record['caller'], r'^test_logging\.py:\d+$')
    self.assertRegex(record['time'], r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+\+00:00$')

  def test_traceback_stays_in_message(self):
    record = self._format(logging.ERROR, 'failed', exc=True)
    self.assertTrue(record['message'].startswith('failed\nTraceback'))
    self.assertTrue(record['message'].endswith('ValueError: boom'))


if __name__ == '__main__':
  unittest.main()
