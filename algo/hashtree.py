import os
import itertools
from ..utilities import hashlite, hashhard


class Entity:

    def __init__(self, path, parent=None):
        self.path = path
        self.parent = parent
        self.level = 0 if parent is None else parent.level + 1

    def __ge__(self, other):
        return self.path >= other.path

    def __le__(self, other):
        return self.path <= other.path

    def __gt__(self, other):
        return self.path > other.path

    def __lt__(self, other):
        return self.path < other.path


class Leaf(Entity):

    def __init__(self, path, parent=None, hsh=None):
        super().__init__(path, parent)
        self.litehash = str(hashlite(self.path) if hsh is None else hsh).encode()
        self._hardhash = None

    @property
    def hardhash(self):
        if self._hardhash is None:
            self._hardhash = hashhard(self.path)
        return self._hardhash

    def __eq__(self, other):
        if self.litehash != other.litehash:
            return False
        else:
            return self.hardhash == other.hardhash


class Node(Entity):

    def __init__(self, path, parent=None):
        super().__init__(path, parent)
        self.path += "/"

        self.level = 0 if parent is None else parent.level+1
        try:
            _, dirz, flz = next(os.walk(self.path))
        except StopIteration:
            self.leaves, self.nodes, self.litehash = [], [], b""
        else:
            self.leaves = sorted([Leaf(self.path+fl, parent=self) for fl in flz])
            self.nodes = sorted([Node(self.path+dr, parent=self) for dr in dirz])
            if self.N:
                hashconcat = b".".join(ent.litehash for ent in self.nodes + self.leaves)
                self.litehash = str(hashlite(hashconcat)).encode()
            else:
                self.litehash = b""
        self._hardhash = None

    @property
    def hardhash(self):
        if self._hardhash is None:
            self._hardhash = hashhard(b".".join(ent.hardhash for ent in self.nodes + self.leaves))
        return self._hardhash

    @property
    def N(self):
        return len(self.nodes) + len(self.leaves)

    @property
    def subs(self):
        return self.nodes + self.leaves

    def __eq__(self, other):
        if self.N != other.N:
            return False
        return all(left == right for left, right in zip(self.subs, other.subs))


class MerkleTree:

    def __init__(self, root):
        self.root = Node(root)
        self.flatview = list(itertools.chain(self.flatnodes(self.root)))

    def flatnodes(self, node):
        if not len(node.nodes):
            return []
        else:
            return node.nodes + [self.flatnodes(subnode) for subnode in node.nodes]
