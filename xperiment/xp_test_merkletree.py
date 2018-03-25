import os

from structure.merkletree import MerkleTree

mt = MerkleTree(os.path.expanduser("~/SciProjects/Project_merkle/"))
dupenode, dupeleaf = mt.find_duplicates()
for lite, dupes in dupenode.items():
    print("-"*50)
    print(lite, "->")
    print("\n".join(dupes))
print("*"*50)
print(dupeleaf)
