from zlib import adler32
from hashlib import sha1


def hardcompare(pth1, pth2, blocksize=65536):
    h1, h2 = hashhard(pth1, blocksize), hashhard(pth2, blocksize)
    if h1 == h2 == -1:
        return False
    return h1 == h2


def hashlite(source, blocksize=65536):
    if isinstance(source, str):
        with open(source, "rb") as handle:
            source = handle.read()
    hsh = 1
    for slc in (source[start:start+blocksize] for start in range(0, len(source), blocksize)):
        hsh = adler32(slc, hsh)
    return hsh


def hashhard(source, blocksize=65536):
    checksummer = sha1()
    if isinstance(source, str):
        with open(source, "rb") as handle:
            source = handle.read()
    if len(source) == 0:
        return -1
    for slc in (source[start:start+blocksize] for start in range(0, len(source), blocksize)):
        checksummer.update(slc)
    return checksummer.hexdigest()


def padto(n, N):
    n, N = str(n), str(N)
    strlen = len(n)
    padlen = len(N)
    if strlen < padlen:
        n = " " * (padlen - strlen) + n
    return n
