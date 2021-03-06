from zlib import adler32
from hashlib import sha1


def hardcompare(pth1, pth2, blocksize=65536):
    if hashhard(pth1, blocksize=blocksize) == hashhard(pth2, blocksize=blocksize):
        return True
    return False


def hashlite(pth):
    with open(pth, "rb") as handle:
        chksum = adler32(handle.read())
        handle.close()
    return chksum


def hashhard(pth, blocksize=65536):
    checksummer = sha1()
    with open(pth, "rb") as handle:
        buffer = handle.read(blocksize)
        while len(buffer) > 0:
            checksummer.update(buffer)
            buffer = handle.read(blocksize)
    return checksummer.hexdigest()


def padto(n, N):
    n, N = str(n), str(N)
    strlen = len(n)
    padlen = len(N)
    if strlen < padlen:
        n = " " * (padlen - strlen) + n
    return n
