import os
import sys
from time import time

from CsxFileBase.database import Hashdb


def check_dir_for_duplicates(left=None, right=None):
    start = time()
    args = sys.argv
    if left is None:
        if len(args) > 2:
            left, right = args[1], args[2]
        elif len(args) > 1:
            left = args[1]
        else:
            print("No root directory supplied! Going into debug mode...")
            left = "D:/Data/raw/tiles/" if sys.platform == "win32" else "/data/Prog/Python/OldProjects/"
            print("Root is set to:", left)

    if not os.path.exists(left):
        raise RuntimeError("No such directory:", left)

    left = Hashdb.create_new(left)
    if right is not None:
        if not os.path.exists(right):
            raise RuntimeError("No such directory:", right)
        right = Hashdb.create_new(right)
        print("Extrinsic mode!")
    else:
        print("Intrinsic mode!")
    left.check_duplicates(other=right)
    print("Time elapsed: {} s".format(int(time() - start)))


if __name__ == '__main__':
    check_dir_for_duplicates()
