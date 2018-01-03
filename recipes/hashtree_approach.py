import time

from CsxFileBase.algo import treesearch
from CsxFileBase.utilities.common import argparse


def main():
    start = time.time()
    treepath, otherpath = argparse()
    tree, other = treesearch.create_trees(treepath, otherpath)
    duplicates = treesearch.find_duplicates(tree, other)
    print(f" [I] Treesearch finished in {time.time() - start:.2f} seconds!")
