import os
import gzip
import pickle

from utilities import hashlite, padto


class Hashdb:

    def __init__(self, root, hashes, paths):
        if not os.path.exists(root):
            raise RuntimeError("No such directory: {}".format(root))
        self.root = root

        self.hashes = hashes
        self.paths = paths

    @classmethod
    def create_new(cls, root):
        hashdb = cls(root, [], [])
        hashdb.initialize()
        return hashdb

    def initialize(self):

        def read_paths():
            pth = []
            print("Initializing database...")
            print("Gathering information...", end="")
            for p, dirs, files in os.walk(self.root):
                pth += [p + "/" + file for file in files]
            print(" Done!")
            return sorted(pth)

        def calc_hashes_vrb():
            hsh = []
            for i, path in enumerate(self.paths):
                hsh.append(hashlite(path))
                print("\rFilling hash database... {:>{w}}/{}"
                      .format(i + 1, N, w=strlen), end=" ")
            return hsh

        self.paths = read_paths()
        N = len(self.paths)
        strlen = len(str(N))
        self.hashes = calc_hashes_vrb()
        print("Done!")
