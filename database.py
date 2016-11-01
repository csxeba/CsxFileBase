import os
import pickle
import gzip

from .utilities import hashlite, hardcompare, padto


class Hashdb:
    def __init__(self, root, hashes, paths):
        if not os.path.exists(root):
            raise RuntimeError("No such directory: {}".format(root))
        if root[-1] not in ("\\", "/"):
            root += "/"
        self.root = root
        self.dbpath = root + ".csxfilebase/hashes.db"

        self.hashes = hashes
        self.paths = paths

    @classmethod
    def from_existing(cls, root):
        dbhandle = gzip.open(root + ".csxfilebase/hashes.db", "rb")
        hashes, paths = pickle.load(dbhandle)
        return cls(root, hashes, paths, empty=False)

    @classmethod
    def create_new(cls, root):
        hashdb = cls(root, [], [])
        hashdb.initialize()
        return hashdb

    def initialize(self):
        print("Initializing database...")
        print("Gathering information...", end="")
        for path, dirs, files in os.walk(self.root):
            self.paths += [path + "/" + file for file in files]
        N = len(self.paths)
        print(" Done!")
        for i, path in enumerate(self.paths):
            self.hashes.append(hashlite(path))
            print("\rFilling hash database... {}/{}".format(padto(i+1, len(str(N))), N), end=" ")
        print("Done!")

    def check_duplicates(self):

        def duplicate_hashes_exist():
            indiv = list(set(self.hashes))
            if len(indiv) == len(self.hashes):
                return False
            return True

        if not duplicate_hashes_exist():
            print("No duplicates!")
            return

        duplicates = dict()
        N = len(self.hashes) ** 2
        k = 1
        for i, (lefthash, leftpath) in enumerate(zip(self.hashes, self.paths)):
            for j, (righthash, rightpath) in enumerate(zip(self.hashes, self.paths)):
                if leftpath == rightpath:
                    continue
                if lefthash == righthash:
                    if hardcompare(leftpath, rightpath):
                        if leftpath in duplicates:
                            duplicates[leftpath].append(rightpath)
                        else:
                            duplicates[leftpath] = [rightpath]
                print("\rChecking for duplicates... {}/{}".format(padto(k, len(str(N))), N), end="")
                k += 1

        print(" Done!")

        if len(duplicates) == 0:
            print("No duplicates found!")
            return

        dupligroups = []
        for left in sorted(list(duplicates.keys())):
            dupligroups.append("\n".join(sorted([left] + duplicates[left])))
        dupligroups = sorted(list(set(dupligroups)))

        print(len(dupligroups), "duplicate-groups found!")
        dupechain = "DUPLICATES IN {}\n".format(self.root)
        for dgroup in dupligroups:
            dupechain += "-"*50 + "\n"
            dupechain += dgroup + "\n"
        print(dupechain)

        outfl = open(self.root + "duplicates.txt", "w")
        outfl.write(dupechain)
        outfl.close()

        print("Duplicates also logged to", self.root + "duplicates.txt")

    def dump_db(self):
        try:
            os.mkdir(self.root + ".csxfilebase/")
        except FileExistsError:
            pass

        print("Dumping hash database...", end=" ")
        handle = gzip.open(self.dbpath, "wb")
        pickle.dump([self.hashes, self.paths], handle)
        print("Done!")
        print("Database file is @", self.dbpath)
