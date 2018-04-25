import os

from CsxFileBase.structure import MerkleTree

tree = MerkleTree(os.path.expanduser("~/Ideglenessen/RendezetlenKépek/"))
print("Root hashes:", tree.root.litehash, tree.root.hardhash)
