import os

from ..utility import hashlite, hashhard, logger


class Entity:

    def __init__(self, path, flatlist, parent=None):
        self.path = path
        self.parent = parent
        self.level = 0 if parent is None else parent.level + 1
        self.litehash = None
        self._hardhash = None
        flatlist.append(self)

    @property
    def hardhash(self):
        raise NotImplementedError

    def __ge__(self, other):
        return self.path >= other.path

    def __le__(self, other):
        return self.path <= other.path

    def __gt__(self, other):
        return self.path > other.path

    def __lt__(self, other):
        return self.path < other.path

    def __eq__(self, other):
        if self.empty or other.empty:
            raise RuntimeError("Attempting equality check on empty entity!")
        if self.litehash == other.litehash:
            if self.hardhash == other.hardhash:
                return True
            print("\n[I] Adler32 hash collision!")
        return False

    @property
    def empty(self):
        out = not bool(self.litehash)
        return out


class Leaf(Entity):

    def __init__(self, path, flatlist, parent=None):
        super().__init__(path, flatlist, parent)
        lh = hashlite(path)
        self.litehash = str(lh).encode() if lh != 1 else b""
        self._hardhash = None
        print(f"{'-'*(self.level+1)}>{self.__class__.__name__} {lh:>10}")

    @property
    def hardhash(self):
        if self._hardhash is None:
            self._hardhash = str(hashhard(self.path)).encode()
        return self._hardhash


class Node(Entity):

    def __init__(self, path, flatlist, parent=None):
        super().__init__(path, flatlist, parent)
        if self.path[-1] != "/":
            self.path += "/"

        self.level = 0 if parent is None else parent.level+1
        try:
            _, dirz, flz = next(os.walk(self.path))
        except StopIteration:
            self.leaves, self.nodes, self.litehash = [], [], b""
        else:
            self.leaves = sorted(Leaf(self.path+fl, flatlist, parent=self) for fl in flz)
            self.nodes = sorted(Node(self.path+dr, flatlist, parent=self) for dr in dirz)
            if self.N:
                hashconcat = b"".join(ent.litehash for ent in self.nodes + self.leaves)
                self.litehash = str(hashlite(hashconcat)).encode()
            else:
                self.litehash = b""
        print(f"{'-'*(self.level+1)}>{self.__class__.__name__} {int(self.litehash):>10}")
        self._hardhash = None

    @property
    def hardhash(self):
        if self._hardhash is None:
            hashconcat = b"".join(ent.hardhash for ent in self.nodes + self.leaves)
            self._hardhash = str(hashhard(hashconcat)).encode()
        return self._hardhash

    @property
    def N(self):
        return len(self.nodes) + len(self.leaves)

    @property
    def subs(self):
        return self.nodes + self.leaves


class MerkleTree:

    def __init__(self, root, prune=False):
        self.flatview = []
        self.root = Node(root, self.flatview)
        self.working = None
        self.deleteme = None
        if prune:
            self.prune_empties()

    def prune_empties(self):
        print("Pruning empty entities...")
        self.deleteme = list(filter(lambda ent: ent.empty, self.flatview))
        self.working = list(filter(lambda ent: not ent.empty, self.flatview))
        self.working.sort(key=lambda ent: (ent.level, ent.path))
