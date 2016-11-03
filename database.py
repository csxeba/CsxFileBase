import os
import pickle
import gzip
import multiprocessing as mp

import time

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

        def read_paths():
            pth = []
            print("Initializing database...")
            print("Gathering information...", end="")
            for path, dirs, files in os.walk(self.root):
                pth += [path + "/" + file for file in files]
            print(" Done!")
            return pth

        def define_batches_generator():
            splitted = (self.paths[start:start + step] for start in range(0, N, step))
            return splitted

        self.paths = read_paths()
        N = len(self.paths)
        step = 100
        batches = define_batches_generator()

        results = []
        queue = mp.Queue()
        jobs = mp.cpu_count()
        done = 0

        print("\rFilling hash database... {}/{}".format(padto(done, N), N), end="")
        while len(results) != N:
            procs = [mp.Process(target=_process_batch, args=[next(batches), queue])
                     for _ in range(jobs)]
            res = []
            for proc in procs:
                proc.start()
            while 1:
                processed = queue.get()
                if processed is not None:
                    res += processed
                    done += len(processed)
                    print("\rFilling hash database... {}/{}".format(padto(done, N), N), end="")
                    if len(res) // step == len(procs):
                        results += res
                        break
                time.sleep(1)
            for proc in procs:
                proc.join()

        self.paths, self.hashes = zip(*results)

    def _old_initialize(self):
        print("Initializing database...")
        print("Gathering information...", end="")
        for path, dirs, files in os.walk(self.root):
            self.paths += [path + "/" + file for file in files]
        self.paths.sort()
        N = len(self.paths)
        print(" Done!")
        for i, path in enumerate(self.paths):
            self.hashes.append(hashlite(path))
            print("\rFilling hash database... {}/{}".format(padto(i+1, N), N), end=" ")
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
                for j in range(i+1, N):
                    righthash, rightpath = self.hashes[j], self.paths[j]
                    if lefthash == righthash:
                        if hardcompare(leftpath, rightpath):
                            if leftpath in dupe:
                                dupe[leftpath].append(rightpath)
                            else:
                                dupe[leftpath] = [rightpath]
                    print("\rChecking for duplicates... {}/{}"
                          .format(padto(k, steps), steps),
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

        print("Duplicate-groups dumped to", self.root + "duplicates.txt")

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


def _process_batch(paths, queue):
    hashes = [hashlite(path) for path in paths]
    queue.put(list(zip(paths, hashes)))


def process_one(path):
    return path, hashlite(path)
