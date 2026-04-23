import hashlib
from pathlib import Path

import blake3
from timeit import timeit

betabet = "bcdfghjkmprstvxz"

def hash_file(file, algorithm):
    hash = hashlib.new(algorithm)
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            hash.update(chunk)
    digest = hash.digest()
    return digest

def hash_context(file: Path, algorithm, width):
    hash = hashlib.new(algorithm)
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hash.update(chunk)
    hex_digest = hash.hexdigest(width)
    return "".join(betabet[int(ch, 16)] for ch in hex_digest)

def hash_filename(filename:str, algorithm:str, width: int):
    hash = hashlib.new(algorithm)
    hash.update(filename.encode("utf-8"))
    hex_digest = hash.hexdigest(width)
    return "".join(betabet[int(ch, 16)] for ch in hex_digest)

def hash_url(url:str, width:int) -> bytes:
    hash = hashlib.blake2s(url.encode("utf-8"), digest_size=width)
    #hash.update(url.encode("utf-8"))
    return hash.digest()

def hash_str(text:str, width: int) -> str:
    hash = hashlib.blake2s(text.encode("utf-8"), digest_size=width)
    #hash.update(text.encode("utf-8"))
    hex_digest = hash.hexdigest()
    return hex_digest

def hash_str_betabet(text:str, width: int) -> str:
    hash = hashlib.blake2s(text.encode("utf-8"), digest_size=width)
    #hash.update(text.encode("utf-8"))
    hex_digest = hash.hexdigest()
    return "".join(betabet[int(ch, 16)] for ch in hex_digest)

def betabet_to_hex(custom_str: str) -> str:
    """
    Convert a custom-encoded hash string back to standard hexadecimal.
    """
    REV_MAP = {ch: format(i, "x") for i, ch in enumerate(betabet)}
    return "".join(REV_MAP[ch] for ch in custom_str)


def hash_file_blake3(file):
    file_hasher = blake3.blake3(max_threads=4)
    file_hasher.update_mmap(file)
    digest = file_hasher.digest()
    #print(" ", digest)
    return digest


if __name__ == "__main__":
    print("blake3")
    f = min(timeit(lambda: hash_file_blake3("/tmp/bigfile"), number=1)
            for i in range(5))
    print("\t", f)
    for digest in hashlib.algorithms_available:
        print(digest)
        try:
            f = min(timeit(lambda : hash_file("/tmp/bigfile", digest), number=1)
                    for i in range(5))
            print("\t", f)
        except Exception:
            continue

