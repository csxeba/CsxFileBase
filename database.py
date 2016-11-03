import os
import pickle
import gzip
import multiprocessing as mp
import threading as thr

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
    def create_new(cls, root):
        hashdb = cls(root, [], [])
        hashdb.mp_initialize()
        return hashdb

    def mp_initialize(self):

        def read_paths():
            pth = []
            print("Initializing database...")
            print("Gathering information...", end="")
            for path, dirs, files in os.walk(self.root):
                pth += [path + "/" + file for file in files]
            print(" Done!")
            return sorted(pth)

        def define_batches_generator(N):
            step = 1000
            splitted = (self.paths[start:start + step] for start in range(0, N, step))
            return splitted

        def build_and_start_processes(jobs, batches, shared_list):
            prcs = []
            for job in range(jobs):
                batch = next(batches, ...)
                if batch is ...:
                    break
                prcs.append(mp.Process(target=_process_batch_thr, args=[batch, shared_list]))
                prcs[-1].start()
            return prcs

        def fill_hash_database():
            cpus = mp.cpu_count()
            N = len(self.paths)
            batches = define_batches_generator(N)

            output = []
            print("\rFilling hash database... {}/{} ".format(padto(0, N), N), end="")
            while len(output) != N:
                res = mp.Manager().list()
                procs = build_and_start_processes(cpus, batches, res)
                while len(res) != len(procs):
                    print("\rFilling hash database... {}/{} ".format(padto(len(output), N), N), end="")
                for r, proc in zip(res, procs):
                    proc.join()
                    output += r

            print("\rFilling hash database... {}/{} ".format(padto(len(output), N), N), end="")
            print(" Done!")
            return output

        self.paths = read_paths()
        results = sorted(fill_hash_database(), key=lambda x: x[0])
        self.paths, self.hashes = zip(*results)

    def thread_initialize(self):

        def read_paths():
            pth = []
            print("Initializing database...")
            print("Gathering information...", end="")
            for path, dirs, files in os.walk(self.root):
                pth += [path + "/" + file for file in files]
            print(" Done!")
            return sorted(pth)

        def build_and_start_threads(shared_list):
            thrds = []
            for job in range(step):
                batch = next(batches, ...)
                if batch is ...:
                    break
                thrds.append(thr.Thread(target=_process_batch_thr, args=[batch, shared_list]))
                thrds[-1].start()
            return thrds

        def define_batches_generator():
            splitted = (self.paths[start:start + step] for start in range(0, N, step))
            return splitted

        self.paths = read_paths()
        N = len(self.paths)
        step = 10
        batches = define_batches_generator()

        results = []
        done = 0
        print("\rFilling hash database... {}/{} ".format(padto(done, N), N), end="")
        while len(results) != N:
            res = []
            threads = build_and_start_threads(res)
            while len(res) != len(threads):
                print("\rFilling hash database... {}/{} ".format(padto(done, N), N), end="")
            for r, t in zip(res, threads):
                results += r
                done += len(r)
                t.join()

        print("\rFilling hash database... {}/{} ".format(padto(done, N), N), end="")
        assert len(results) == done, "Done: {} len(results): {}".format(done, len(results))
        print(" Done!")
        results.sort(key=lambda x: x[0])
        self.paths, self.hashes = zip(*results)

    def old_initialize(self):

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
                print("\rFilling hash database... {}/{}".format(padto(i + 1, N), N), end=" ")
            return hsh

        self.paths = read_paths()
        N = len(self.paths)
        self.hashes = calc_hashes_vrb()
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

    def save(self):
        try:
            os.mkdir(self.root + ".csxfilebase/")
        except FileExistsError:
            pass

        print("Dumping hash database...", end=" ")
        handle = gzip.open(self.dbpath, "wb")
        pickle.dump([self.hashes, self.paths], handle)
        print("Done!")
        print("Database file is @", self.dbpath)

    @classmethod
    def load(cls, root):
        dbhandle = gzip.open(root + ".csxfilebase/hashes.db", "rb")
        hashes, paths = pickle.load(dbhandle)
        return cls(root, hashes, paths, empty=False)


def _process_batch(paths, queue):
    hashes = [hashlite(path) for path in paths]
    queue.put(list(zip(paths, hashes)))


def _process_batch_thr(paths, results):
    hashes = [hashlite(path) for path in paths]
    results.append(list(zip(paths, hashes)))


def process_one(path):
    return path, hashlite(path)
