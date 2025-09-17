import unittest
from tap_tester import connections, menagerie, runner

class ParquetSyncFileTest(unittest.TestCase):
    def test_run(self):
        self.assertEqual(1, 2, "test is running and failing")
