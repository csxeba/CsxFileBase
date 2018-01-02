import os
import sys
import time
import datetime
from collections import defaultdict

from CsxFileBase.algo.database import Hashdb
from CsxFileBase.utilities import hardcompare


def argparse():
    try:
        dbpath = sys.argv[1]
    except IndexError:
        dbpath = os.getcwd()

    try:
        odbpath = sys.argv[2]
        print("Extrinsic search: {} vs {}".format(dbpath, odbpath))
    except IndexError:
        odbpath = None
        print("Intrinsic search in {}".format(dbpath))
    return dbpath, odbpath


def sanity_check(dbpath: Hashdb, odbpath: Hashdb):
    if not os.path.exists(dbpath):
        raise RuntimeError("No such directory:", dbpath)
    if odbpath is not None:
        if not os.path.exists(odbpath):
            raise RuntimeError("No such directory:", odbpath)


def create_dbs(dbpath: Hashdb, odbpath: Hashdb=None):
    db = Hashdb.create_new(dbpath)
    if odbpath is not None:
        odb = Hashdb.create_new(odbpath)
    else:
        odb = None
    return db, odb


def duplicate_hashes_exist(left: Hashdb, right: Hashdb=None):
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


def compare_entities(lefthash, leftpath, righthash, rightpath):
    if lefthash != righthash:
        return False
    return hardcompare(leftpath, rightpath)


def extract_duplicates_against_self(left: Hashdb):
    dupe = defaultdict(list)
    thislen = len(left.hashes)
    steps = ((thislen ** 2) - thislen) // 2
    strlen = len(str(steps))
    for i, (lefthash, leftpath) in enumerate(zip(left.hashes, left.paths)):
        for j, (righthash, rightpath) in enumerate(zip(left.hashes[i + 1:], left.paths[i + 1:])):
            print(f"\rChecking for duplicates... {steps:>{strlen}}/{i*thislen+j}", end="")
            if compare_entities(lefthash, leftpath, righthash, rightpath):
                dupe[leftpath].append(rightpath)
    return dupe


def extract_duplicates_against_other(left: Hashdb, right: Hashdb):
    dupe = defaultdict(list)
    thislen = len(left.hashes)
    thatlen = len(right.hashes)
    steps = thislen * thatlen
    strlen = len(str(steps))
    for i, lefthash, leftpath in zip(range(thislen), left.hashes, left.paths):
        for j, righthash, rightpath in zip(range(thatlen), right.hashes, right.paths):
            print("\rChecking for duplicates {:>{w}}/{}"
                  .format(i * thislen + j, steps, w=strlen),
                  end="")
            if compare_entities(lefthash, leftpath, righthash, rightpath):
                dupe[leftpath].append(rightpath)
    return dupe


def construct_output_string(duplicates, left: Hashdb, right: Hashdb=None):
    dchain = "DUPLICATES IN {}\n".format(left.root)
    for left in duplicates.keys():
        dchain += "-" * 50 + "\n"
        dchain += "\n".join(sorted([left] + duplicates[left]))
        dchain += "\n"
    return dchain


def dump_output_to_file(dupechain, left: Hashdb, right: Hashdb=None, outfl=None):
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


def run_algo(db: Hashdb, odb: Hashdb):

    if not duplicate_hashes_exist(db, odb):
        raise RuntimeError("No duplicates!")

    duplicates = extract_duplicates_against_self(db) if odb is None else extract_duplicates_against_other(db, odb)
    print(" Done!")

    if len(duplicates) == 0:
        print("No duplicates found!")
    else:
        print(len(duplicates), "duplicate-groups found!")

    outchain = construct_output_string(duplicates, db, odb)
    dump_output_to_file(outchain, db, odb)


def main():
    start = time.time()
    hdbpath, odbpath = argparse()
    hashdb, otherdb = create_dbs(hdbpath, odbpath)
    run_algo(hashdb, otherdb)
    seconds = time.time() - start
    minutes = seconds / 60
    m = minutes > 3
    print("Run took {0[0]:>.2f} {0[1]}!".format(
        (minutes, "minutes") if m else (seconds, "seconds")
    ))


if __name__ == '__main__':
    main()
