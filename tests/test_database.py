import unittest

from CsxFileBase.database import Hashdb


class Test_Database(unittest.TestCase):
    def setUp(self):
        self.db = Hashdb.create_new("D:/Data/raw/tiles/")

    def test_two_approaches_give_the_same_result(self):
        hashes1 = self.db.hashes
        paths1 = self.db.paths
        self.db._old_initialize()
        hashes2 = self.db.hashes
        paths2 = self.db.paths
        self.assertEqual(sorted(hashes1), sorted(hashes2), msg="Hashes are different!")
        self.assertEqual(sorted(paths1), sorted(paths2), msg="Paths are different!")
        self.assertEqual(hashes1, hashes2, msg="Hashes' order is different!")
        self.assertEqual(paths1, paths2, msg="Paths' order is different!")
