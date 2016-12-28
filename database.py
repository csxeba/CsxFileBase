import os
import pickle
import gzip
import datetime

from .utilities import hashlite, hardcompare, padto


class Hashdb:

    def __init__(self, root, hashes, paths):
        if not os.path.exists(root):
            raise RuntimeError("No such directory: {}".format(root))
        # if root[-1] not in ("\\", "/"):
        #     root += "/"
        self.root = root
        self.dbpath = root + ".csxfilebase/hashes.db"

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
                print("\rFilling hash database... {}/{}".format(padto(i + 1, N), N), end=" ")
            return hsh

        self.paths = read_paths()
        N = len(self.paths)
        self.hashes = calc_hashes_vrb()
        print("Done!")

    def check_duplicates(self, other=None, outfl=None):

        def duplicate_hashes_exist():
            if other is None:
                indiv = list(set(self.hashes))
                if len(indiv) == len(self.hashes):
                    return False
                return True
            else:
                indiv = list(set(self.hashes + other.hashes))
                if len(indiv) == len(self.hashes) + len(other.hashes):
                    return False
                return True

        def extract_duplicates_against_self():
            dupe = {leftpath: [rightpath for righthash, rightpath in zip(self.hashes[i+1:], self.paths[i+1:])
                               if compare_entities(lefthash, leftpath, righthash, rightpath)]
                    for i, (lefthash, leftpath) in enumerate(zip(self.hashes, self.paths))}
            # dupe = dict()
            # mylen = len(self.hashes)
            # steps = ((mylen ** 2) - mylen) // 2
            # for i, (lefthash, leftpath) in enumerate(zip(self.hashes, self.paths)):
            #     for j, (righthash, rightpath) in enumerate(zip(self.hashes[i+1:], self.paths[i+1:])):
            #         print("\rChecking for duplicates... {0:>{w}}/{1}"
            #               .format(i*mylen + j, steps, w=len(str(steps))),
            #               end="")
            #         if compare_entities(lefthash, leftpath, righthash, rightpath):
            #             if lefthash in dupe:
            #                 dupe[leftpath].append(rightpath)
            #             else:
            #                 dupe[leftpath] = [rightpath]
            return {key: val for key, val in dupe.items() if val}

        def extract_duplicates_against_other():
            thislen = len(self.hashes)
            thatlen = len(other.hashes)
            steps = thislen * thatlen
            dupe = dict()
            for i, lefthash, leftpath in zip(range(thislen), self.hashes, self.paths):
                for j, righthash, rightpath in zip(range(thatlen), other.hashes, other.paths):
                    print("\rChecking for duplicates {0:>{w}}/{1}"
                          .format(i * thislen + j, steps, w=len(str(steps))),
                          end="")
                    if compare_entities(lefthash, leftpath, righthash, rightpath):
                        if leftpath in dupe:
                            dupe[leftpath].append(rightpath)
                        else:
                            dupe[leftpath] = [rightpath]
            return dupe

        def compare_entities(lefthash, leftpath, righthash, rightpath):
            if lefthash == righthash:
                return hardcompare(leftpath, rightpath)
            else:
                return False

        def construct_output_string():
            dchain = "DUPLICATES IN {}\n".format(self.root)
            for left in duplicates.keys():
                dchain += "-" * 50 + "\n"
                dchain += "\n".join(sorted([left] + duplicates[left]))
                dchain += "\n"
            return dchain

        def dump_output_to_file():
            if outfl is None:
                flname = self.root
                flname += "duplicates"
                flname += "_" + datetime.datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
                flname += ".txt"
            else:
                flname = outfl

            handle = open(flname, "w")
            handle.write(dupechain)
            handle.close()

            print("Duplicate-groups dumped to", flname)

        if not duplicate_hashes_exist():
            print("No duplicates!")
            return

        if other is None:
            duplicates = extract_duplicates_against_self()
        else:
            duplicates = extract_duplicates_against_other()
        print(" Done!")

        if len(duplicates) == 0:
            print("No duplicates found!")
            return
        else:
            print(len(duplicates), "duplicate-groups found!")

        dupechain = construct_output_string()
        dump_output_to_file()

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
