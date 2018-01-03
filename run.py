import time
from algo import dbsearch, treesearch
from utilities.common import parse_args


def main():
    start = time.time()
    arg = parse_args()
    mod = (treesearch, dbsearch)[arg.algorithm == "database"]
    root, other = mod.create_models(arg.root, arg.other if arg.other else None)
    mod.find_duplicates(root, other)
    seconds = time.time() - start
    minutes = seconds / 60
    m = minutes > 3
    print("Run took {0[0]:>.2f} {0[1]}!".format(
        (minutes, "minutes") if m else (seconds, "seconds")
    ))


if __name__ == '__main__':
    main()