import time
from datetime import datetime
from collections import defaultdict

from structures.merkletree import MerkleTree, Node, Leaf


def create_trees(treepath, otherpath=None):
    tree = MerkleTree(treepath, prune=True)
    if otherpath is not None:
        other = MerkleTree(otherpath)
    else:
        other = None
    return tree, other


def implicit_extract_duplicate_nodes(tree: MerkleTree):
    blacklist = set()
    nodes = list(node for node in tree.working if isinstance(node, Node))
    nodes.remove(tree.root)
    dupenodes = defaultdict(set)
    N = len(nodes)
    checks = (N ** 2 - N) // 2
    print(" [I] Extracting duplicate nodes")
    for i, leftnode in enumerate(nodes):
        if leftnode.parent.litehash in blacklist:
            blacklist.add(leftnode.litehash)
            continue
        if leftnode.litehash in dupenodes:
            dupenodes[leftnode.litehash].add(leftnode.path)
            continue
        for j, rightnode in enumerate(nodes[i + 1:]):
            if rightnode.parent.litehash in blacklist:
                blacklist.add(rightnode.litehash)
                continue
            print(f"\r{checks}/{i*checks+j}", end=" ")
            if leftnode == rightnode:
                dupenodes[leftnode.litehash].update({leftnode.path, rightnode.path})
                blacklist.add(leftnode.litehash)
    print()
    return dupenodes, blacklist


def implicit_extract_duplicate_leaves(tree: MerkleTree, blacklist: set):
    leaves = [leaf for leaf in tree.working if isinstance(leaf, Leaf)]
    dupeleaves = defaultdict(set)
    N = len(leaves)
    checks = (N ** 2 - N) // 2
    print(" [I] Extracting duplicate leaves")
    for i, leftleaf in enumerate(leaves):
        if leftleaf.parent.litehash in blacklist:
            continue
        for j, rightleaf in enumerate(leaves[i + 1:]):
            print(f"\r{checks}/{i*checks+j}", end=" ")
            if leftleaf == rightleaf:
                dupeleaves[leftleaf.litehash].update({leftleaf.path, rightleaf.path})
    print()
    return dupeleaves


def compare_trees(tree: MerkleTree, other: MerkleTree):
    if other.working is None:
        other.prune_empties()
    if tree == other:
        print(" [I] Supplied hashtrees are the same! Consider running an implicit search!")
    return None, None


def construct_output_string(dupe_nodes, dupe_leaves):
    out = [f"{len(dupe_nodes)} duplicate node-groups found!",
           f"{len(dupe_leaves)} independent duplicate leaves found!",
           "*" * 50]
    for litehash in dupe_nodes:
        out += ["-"*50, f"HASH: {litehash} ->", "\n".join(sorted(dupe_nodes[litehash]))]
    out.append("*"*50)
    for litehash in dupe_leaves:
        out += ["-"*50, f"HASH: {litehash} ->", "\n".join(sorted(dupe_nodes[litehash]))]
    out.append("*"*50)
    return "\n".join(out)


def dump_output_to_file(dupechain, outfl=None):
    if outfl is None:
        outfl = f"duplicates_MT_{datetime.now().strftime('%Y.%m.%d_%H.%M.%S')}.txt"
    with open(outfl, "w") as handle:
        handle.write(dupechain)
    print("Duplicate-groups dumped to", outfl)


def find_duplicates(tree: MerkleTree, other: MerkleTree=None):
    start = time.time()
    if tree.working is None:
        tree.prune_empties()
    if other is None:
        duplicate_nodes, blacklisted_nodes = implicit_extract_duplicate_nodes(tree)
        duplicate_leaves = implicit_extract_duplicate_leaves(tree, blacklisted_nodes)
    else:
        duplicate_nodes, duplicate_leaves = compare_trees(tree, other)
    print(f" [I] Run took {time.time() - start:.2f} seconds")
    dump_output_to_file(duplicate_nodes, duplicate_leaves)
