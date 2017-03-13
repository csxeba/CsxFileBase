import os
import sys

from CsxFileBase.algo.hashtree import Node

try:
    root = sys.argv[1]
except IndexError:
    root = os.getcwd()
tree = Node(root)
