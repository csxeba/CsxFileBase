import os

from CsxFileBase.structure import MerkleTree

tree = MerkleTree(os.path.expanduser("~/Ideglenessen/RendezetlenKépek/"))
print("Root hash:", tree.root.litehash)
