import os
from ..utilities import hashlite, hashhard


class Entity:

    def __init__(self, path, parent=None):
        self.path, self.name = os.path.split(path)
        self.parent = parent

    def __ge__(self, other):
        return self.path + self.name >= other.path + other.name

    def __le__(self, other):
        return self.path + self.name <= other.path + other.name

    def __gt__(self, other):
        return self.path + self.name > other.path + other.name

    def __lt__(self, other):
        return self.path + self.name < other.path + other.name


class Node(Entity):

    def __init__(self, path, parent=None):
        super().__init__(path, parent)

        _, dirz, flz = next(os.walk(path))
        self.leaves = sorted([Leaf(path+fl) for fl in flz])
        self.nodes = sorted([Node(path + dr) for dr in dirz])
        self.hashlite = hashlite(b".".join(ent.hashlite for ent in self.nodes + self.leaves))
        self._hashhard = None

    @property
    def hashhard(self):
        if self._hashhard is None:
            self._hashhard = hashhard(b".".join(ent.hashhard for ent in self.nodes + self.leaves))
        return self._hashhard

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


class Leaf(Entity):

    def __init__(self, path, hsh=None):
        super().__init__(path)
        self.litehash = hashlite(self.path) if hash is None else hsh
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
