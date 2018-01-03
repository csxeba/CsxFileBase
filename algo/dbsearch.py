from collections import defaultdict
from datetime import datetime

from structures.database import Hashdb

from utilities import hardcompare


def create_models(dbpath: Hashdb, odbpath: Hashdb=None):
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
    if lefthash == righthash:
        if hardcompare(leftpath, rightpath):
            return True
        print("\n [I] Adler32 hash collision!")
    return False


def extract_duplicates_against_self(left: Hashdb):
    dupe = defaultdict(list)
    N = len(left.hashes)
    steps = ((N ** 2) - N) // 2
    for i, (lefthash, leftpath) in enumerate(zip(left.hashes, left.paths)):
        for j, (righthash, rightpath) in enumerate(zip(left.hashes[i + 1:], left.paths[i + 1:])):
            print(f"\rChecking for duplicates... {steps}/{i*N+j}", end=" ")
            if compare_entities(lefthash, leftpath, righthash, rightpath):
                dupe[leftpath].append(rightpath)
    print()
    return dupe


def extract_duplicates_against_other(left: Hashdb, right: Hashdb):
    dupe = defaultdict(list)
    thislen = len(left.hashes)
    thatlen = len(right.hashes)
    steps = thislen * thatlen
    for i, (lefthash, leftpath) in enumerate(zip(left.hashes, left.paths)):
        for j, (righthash, rightpath) in enumerate(zip(right.hashes, right.paths)):
            print(f"\rChecking for duplicates {steps}/{i*thislen+j}", end=" ")
            if compare_entities(lefthash, leftpath, righthash, rightpath):
                dupe[leftpath].append(rightpath)
    print()
    return dupe


def construct_output_string(duplicates, leftdb, rightdb):
    dchain = [f"DUPLICATES IN {leftdb.root}"]
    if rightdb:
        dchain[0] += f" vs {rightdb.root}"
    for left in duplicates.keys():
        dchain += ["-" * 50 + "\n", "\n".join(sorted([left] + duplicates[left]))]
    return "\n".join(dchain)


def dump_output_to_file(dupechain, outfl=None):
    if outfl is None:
        outfl = f"duplicates_DB_{datetime.now().strftime('%Y.%m.%d_%H.%M.%S')}.txt"
    with open(outfl, "w") as handle:
        handle.write(dupechain)
    print("Duplicate-groups dumped to", outfl)


def find_duplicates(db: Hashdb, odb: Hashdb):
    if not duplicate_hashes_exist(db, odb):
        raise RuntimeError("No duplicates!")

    duplicates = extract_duplicates_against_self(db) if odb is None else extract_duplicates_against_other(db, odb)
    print(" Done!")
    print(len(duplicates), "duplicate-groups found!")
    outchain = construct_output_string(duplicates, db, odb)
    dump_output_to_file(outchain)
