import os
from ..utilities import hashlite, hardcompare


class Entity:

    def __init__(self, path, parent=None):
        path = os.path.split(path)
        self.path = os.path.join(path[:-1])
        self.name = path[-1]
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
        self.leaves = [Leaf(path+fl) for fl in flz]
        self.children = [Node(path+dr) for dr in dirz]

    @property
    def N(self):
        return len(self.leaves) + len(self.children)

    @property
    def subs(self):
        return sorted(self.children) + sorted(self.leaves)

    def __eq__(self, other):
        if self.N != other.N:
            return False
        return all(left == right for left, right in zip(self.subs, other.subs))


class Leaf(Entity):

    def __init__(self, path, hsh=None):
        super().__init__(path)
        self.hsh = hsh if hsh is not None else hashlite(path)

    def __hash__(self):
        return self.hsh

    def __eq__(self, other):
        if self.hsh != other.hsh:
            return False
        else:
            return hardcompare(self.path + self.name, other.path + other.flnm)
