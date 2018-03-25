import os
import argparse


def parse_args():
    parser = argparse.ArgumentParser(prog="dupe", description="File duplicate finder.")
    parser.add_argument("root", metavar="directory_root", nargs="?", default=".",
                        help="Root directory to search")
    parser.add_argument("other", metavar="other_directory", nargs="?",
                        help="Another directory to compare against", default="")
    parser.add_argument("-a", "--algorithm", nargs=1, choices=["database", "hashtree"],
                        default="database", required=False)
    return parser.parse_args()


def sanity_check(dbpath, odbpath):
    if not os.path.exists(dbpath):
        raise RuntimeError("No such directory:", dbpath)
    if odbpath is not None:
        if not os.path.exists(odbpath):
            raise RuntimeError("No such directory:", odbpath)
