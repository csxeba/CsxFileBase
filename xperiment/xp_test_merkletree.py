import os

from CsxFileBase.algo.hashtree import MerkleTree


mt = MerkleTree(os.path.expanduser("~/SciProjects/Project_merkle/"))
print(mt.flatview)
