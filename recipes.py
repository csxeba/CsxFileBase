import os
import sys
from time import time

from .database import Hashdb


def check_duplicates(root=None):
    start = time()
    args = sys.argv
    if root is None:
        if args:
            root = args[0]
        else:
            raise RuntimeError("Please supply a root directory!")

    if not os.path.exists(root):
        raise RuntimeError("No such directory:", root)

    db = Hashdb.create_new(root)
    db.check_duplicates()
    print("Time elapsed: {} s".format(int(time() - start)))
