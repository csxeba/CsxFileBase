import os

from utility import hashlite, EMPTY_LIGHT_HASH


class Hashdb:

    def __init__(self, root, hashes, paths):
        if not os.path.exists(root):
            raise RuntimeError("No such directory: {}".format(root))
        self.root = os.path.abspath(root)
        self.hashes = hashes
        self.paths = paths

    @classmethod
    def create_new(cls, root):
        hashdb = cls(root, [], [])
        hashdb.initialize()
        return hashdb

    def initialize(self):
        print("Initializing hash database on {}".format(self.root))
        self.paths = []
        self.hashes = []
        print("Collectiong information...")
        for node, dirz, files in os.walk(self.root):
            self.paths += [os.path.join(node, f) for f in files]
        N = len(self.paths)
        assert len(set(self.paths)) == N
        print("Found {} file entries".format(N))
        empty_indices = []
        for i, path in enumerate(self.paths):
            print(f"\rFilling hash database {N}/{i+1}", end="")
            lite = hashlite(path)
            if lite == EMPTY_LIGHT_HASH:
                empty_indices.append(i)
                continue
            self.hashes.append(lite)
        print(flush=True)
        for i in empty_indices[::-1]:
            self.paths.pop(i)
        assert len(self.hashes) == len(self.paths)
