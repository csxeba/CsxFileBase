import os
import pickle
import gzip
import multiprocessing as mp

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

    def mp_initialize(self):

        print("Initializing database...")
        print("Gathering information...", end="")
        for path, dirs, files in os.walk(self.root):
            self.paths += [path + "/" + file for file in files]
        print(" Done!")

        step=100

        batches = (self.paths[start:start+step] for start in range(0, len(self.paths), step))

        jobs = mp.cpu_count()
        while 1:
            procs = [mp.Process(target=_process_batch) for _ in range(jobs)]
            for proc in procs:
                proc.start()

        pool = mp.Pool(processes=mp.cpu_count())
        results = pool.map(_process_batch, batches)

        rs = []
        for lst in results:
            rs += lst
        del results

        self.paths, self.hashes = zip(*rs)

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

        def extract_duplicates():
            dupe = dict()
            N = len(self.hashes)
            N2 = N ** 2
            steps = (N2 - N) // 2
            k = 1
            for i, (lefthash, leftpath) in enumerate(zip(self.hashes, self.paths)):
                for j in range(i, N):
                    righthash, rightpath = self.hashes[j], self.paths[j]
                    if lefthash == righthash:
                        if hardcompare(leftpath, rightpath):
                            if leftpath in dupe:
                                dupe[leftpath].append(rightpath)
                            else:
                                dupe[leftpath] = [rightpath]
                    print("\rChecking for duplicates... {}/{}"
                          .format(padto(k, len(str(steps))), steps),
                          end="")
                    k += 1
            return dupe

        def construct_output_string():
            dchain = "DUPLICATES IN {}\n".format(self.root)
            for left in duplicates.keys():
                dchain += "-" * 50 + "\n"
                dchain += "\n".join(sorted([left] + duplicates[left]))
                dchain += "\n"
            return dchain

        if not duplicate_hashes_exist():
            print("No duplicates!")
            return

        duplicates = extract_duplicates()
        print(" Done!")

        if len(duplicates) == 0:
            print("No duplicates found!")
            return
        else:
            print(len(duplicates), "duplicate-groups found!")

        dupechain = construct_output_string()

        outfl = open(self.root + "duplicates.txt", "w")
        outfl.write(dupechain)
        outfl.close()

        print("Duplicate-groups dumpled to", self.root + "duplicates.txt")

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


def _process_batch(paths):
    print(".", end="")
    hashes = [hashlite(path) for path in paths]
    return list(zip(paths, hashes))


def process_one(path):
    print("Doing", path)
    return path, hashlite(path)
