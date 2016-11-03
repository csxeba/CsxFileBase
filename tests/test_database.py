import unittest

from CsxFileBase.database import Hashdb


class Test_Database(unittest.TestCase):
    def setUp(self):
        self.db = Hashdb("D:/Data/raw/tiles/", [], [])

    def test_two_approaches_give_the_same_result(self):
        self.db.mp_initialize()
        hashes1 = self.db.hashes
        paths1 = self.db.paths
        self.db.old_initialize()
        hashes2 = self.db.hashes
        paths2 = self.db.paths
        self.db.thread_initialize()
        hashes3 = self.db.hashes
        paths3 = self.db.paths
        self.assertEqual(sorted(hashes1), sorted(hashes2), msg="Hashes are different!")
        self.assertEqual(sorted(hashes2), sorted(hashes3), msg="Hashes are different!")
        self.assertEqual(sorted(paths1), sorted(paths2), msg="Paths are different!")
        self.assertEqual(sorted(paths2), sorted(paths3), msg="Paths are different!")
        self.assertEqual(hashes1, hashes2, msg="Hashes' order is different!")
        self.assertEqual(hashes2, hashes3, msg="Hashes' order is different!")
        self.assertEqual(paths1, paths2, msg="Paths' order is different!")
        self.assertEqual(paths2, paths3, msg="Paths' order is different!")
