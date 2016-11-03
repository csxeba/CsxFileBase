import os
import sys
from time import time

from CsxFileBase.database import Hashdb


def check_dir_for_duplicates(root=None):
    start = time()
    args = sys.argv
    if root is None:
        if len(args) > 1:
            root = args[1]
        else:
            print("No root directory supplied! Going into debug mode...")
            root = "E:/" if sys.platform == "win32" else "/data/Prog/Python/OldProjects/"

    if not os.path.exists(root):
        raise RuntimeError("No such directory:", root)

    db = Hashdb.create_new(root)
    db.check_duplicates()
    print("Time elapsed: {} s".format(int(time() - start)))

if __name__ == '__main__':
    check_dir_for_duplicates()
