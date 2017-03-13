import os
import sys
import time
import datetime
from collections import defaultdict

from CsxFileBase.algo.database import Hashdb
from CsxFileBase.utilities import hardcompare


def duplicate_hashes_exist(left, right=None):
    if right is None:
        indiv = list(set(left.hashes))
        if len(indiv) == len(left.hashes):
            return False
        return True
    else:
        indiv = list(set(left.hashes + right.hashes))
        if len(indiv) == len(left.hashes) + len(right.hashes):
            return False
        return True


def silently(left, right=None):
    if not right:
        dupe = {leftpath: [rightpath for righthash, rightpath
                           in zip(left.hashes[i + 1:], left.paths[i + 1:])
                           if compare_entities(lefthash, leftpath, righthash, rightpath)]
                for i, (lefthash, leftpath)
                in enumerate(zip(left.hashes, left.paths))}
    else:
        dupe = {leftpath: [rightpath for righthash, rightpath
                           in zip(right.hashes, right.paths)
                           if compare_entities(lefthash, leftpath, righthash, rightpath)]
                for lefthash, leftpath
                in zip(left.hashes, left.paths)}
    return {key: val for key, val in dupe.items() if val}


def extract_duplicates_against_self(left):
    dupe = defaultdict(list)
    mylen = len(left.hashes)
    steps = ((mylen ** 2) - mylen) // 2
    strlen = len(str(steps))
    for i, (lefthash, leftpath) in enumerate(zip(left.hashes, left.paths)):
        for j, (righthash, rightpath) in enumerate(zip(left.hashes[i + 1:], left.paths[i + 1:])):
            print("\rChecking for duplicates... {:>{w}}/{}"
                  .format(i * mylen + j, steps, w=strlen),
                  end="")
            if compare_entities(lefthash, leftpath, righthash, rightpath):
                dupe[leftpath].append(rightpath)
    return dupe


def extract_duplicates_against_other(left, right):
    thislen = len(left.hashes)
    thatlen = len(right.hashes)
    steps = thislen * thatlen
    strlen = len(str(steps))
    dupe = defaultdict(list)
    for i, lefthash, leftpath in zip(range(thislen), left.hashes, left.paths):
        for j, righthash, rightpath in zip(range(thatlen), right.hashes, right.paths):
            print("\rChecking for duplicates {:>{w}}/{}"
                  .format(i * thislen + j, steps, w=strlen),
                  end="")
            if compare_entities(lefthash, leftpath, righthash, rightpath):
                dupe[leftpath].append(rightpath)
    return dupe


def compare_entities(lefthash, leftpath, righthash, rightpath):
    if lefthash == righthash:
        return hardcompare(leftpath, rightpath)
    else:
        return False


def construct_output_string(left, right=None):
    dchain = "DUPLICATES IN {}\n".format(left.root)
    for left in duplicates.keys():
        dchain += "-" * 50 + "\n"
        dchain += "\n".join(sorted([left] + duplicates[left]))
        dchain += "\n"
    return dchain


def dump_output_to_file(left, right=None, outfl=None):
    if outfl is None:
        flname = left.root
        flname += "duplicates"
        flname += "_" + datetime.datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
        flname += ".txt"
    else:
        flname = outfl

    handle = open(flname, "w")
    handle.write(dupechain)
    handle.close()

    print("Duplicate-groups dumped to", flname)


try:
    hashdb = sys.argv[1]
except IndexError:
    hashdb = os.getcwd()

try:
    otherdb = sys.argv[2]
    print("Extrinsic search: {} vs {}".format(hashdb, otherdb))
except IndexError:
    otherdb = None
    print("Intrinsic search in {}".format(hashdb))

start = time.time()

if not os.path.exists(hashdb):
    raise RuntimeError("No such directory:", hashdb)

hashdb = Hashdb.create_new(hashdb)
if otherdb is not None:
    if not os.path.exists(otherdb):
        raise RuntimeError("No such directory:", otherdb)
    otherdb = Hashdb.create_new(otherdb)
    print("Extrinsic mode!")
else:
    print("Intrinsic mode!")

hashdb.check_duplicates(right=otherdb)
print("Time elapsed: {} s".format(int(time.time() - start)))

if not duplicate_hashes_exist(hashdb, otherdb):
    raise RuntimeError("No duplicates!")

if otherdb is None:
    duplicates = extract_duplicates_against_self(hashdb)
else:
    duplicates = extract_duplicates_against_other(hashdb, otherdb)
print(" Done!")

if len(duplicates) == 0:
    print("No duplicates found!")
else:
    print(len(duplicates), "duplicate-groups found!")

dupechain = construct_output_string(hashdb)
dump_output_to_file(hashdb, otherdb)
