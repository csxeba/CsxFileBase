import os

from ..utilities.hashing import hashlite


def print_empties(start):
    for root, dirz, flz in os.walk(start):
        if not len(dirz) + len(flz):
            print(root)
            continue
        print("\n".join(map(str, filter(lambda hsh: hsh == 1, map(hashlite, flz)))))
