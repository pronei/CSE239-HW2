import os
import re
import io
import hashlib
import collections
from typing import Iterable, Tuple

import rpyc
from rpyc.utils.server import ThreadedServer

DATA_DIR = os.environ.get("MR_DATA_DIR", "/data")
WORD_RE = re.compile(r"[A-Za-z0-9']+")

# same notion as WORD_RE = [A-Za-z0-9']+
def _is_word_byte(b: int) -> bool:
    return (48 <= b <= 57) or (65 <= b <= 90) or (97 <= b <= 122) or (b == 39)  # 0-9 A-Z a-z '

def _read_until_boundary(f, want_bytes: int, grow_step: int = 65536) -> bytes:
    """
    Read at least want_bytes, then keep reading in grow_step chunks
    until the last byte is NOT a word-char or we hit EOF.
    """
    buf = f.read(want_bytes)
    if not buf:
        return b""
    # grow until boundary
    while buf and _is_word_byte(buf[-1]):
        more = f.read(grow_step)
        if not more:
            break
        buf += more
    return buf

def stable_part(word: str, R: int) -> int:
    h = hashlib.md5(word.encode("utf-8")).hexdigest()
    return int(h, 16) % R

def tokenize(text: str) -> Iterable[str]:
    for m in WORD_RE.finditer(text.lower()):
        yield m.group(0)

def read_chunk_boundary_safe(path: str, start: int, end: int) -> str:
    """
    Return text for the logical byte-interval [start, end) **without overlap**:
    - If start is in the middle of a word, drop the leading partial word.
    - Read beyond `end` until the last byte is NOT a word-char, so the previous
      chunk can finish its trailing word completely.
    The previous chunk (ending at `start`) will read forward to the boundary,
    so each word is counted exactly once by some chunk.
    """
    start = max(0, start)
    need = max(0, end - start)

    with open(path, "rb") as f:
        # Include 1 byte of look-behind to detect if we start inside a word
        pre = 1 if start > 0 else 0
        f.seek(max(0, start - pre))

        # Read the nominal range (+ look-behind), then extend to boundary
        raw = _read_until_boundary(f, want_bytes=pre + need)

    if not raw:
        return ""

    # Decide if we started inside a word:
    #   inside iff we had a look-behind and BOTH the prev byte (raw[0])
    #   and the first byte of our range (raw[1]) are word-chars.
    drop_first_word = False
    if start > 0 and len(raw) >= 2 and _is_word_byte(raw[0]) and _is_word_byte(raw[1]):
        drop_first_word = True

    # Strip the look-behind byte; from here, raw begins at logical 'start'
    raw = raw[1:] if start > 0 else raw

    # If we started mid-word, skip bytes until we hit the first non-word byte
    if drop_first_word:
        i = 0
        n = len(raw)
        while i < n and _is_word_byte(raw[i]):
            i += 1
        raw = raw[i:]

    # Decode permissively; our tokenization uses only ASCII word chars, so this is safe
    return raw.decode("utf-8", errors="ignore")

class MapReduceService(rpyc.Service):
    """
    Disk-backed MapReduce:
    - map(job_id, task_id, file_path, start, end, R) -> writes /data/jobs/<job>/shuffle/map_<task>_part_<r>.tsv
    - reduce(job_id, reducer_id) -> reads all map_*_part_<rid>.tsv and writes /data/jobs/<job>/reduce/part_<rid>.tsv
    """

    def exposed_map(self, job_id: str, task_id: int, file_path: str, start: int, end: int, R: int) -> str:
        # file_path = os.path.join(DATA_DIR, os.path.relpath(file_path, "/"))
        # If coordinator sent a relative path under DATA_DIR, join; if absolute, trust it.
        if not os.path.isabs(file_path):
            file_path = os.path.join(DATA_DIR, file_path)
        job_dir = os.path.join(DATA_DIR, "jobs", job_id)
        out_dir = os.path.join(job_dir, "shuffle")
        os.makedirs(out_dir, exist_ok=True)

        text = read_chunk_boundary_safe(file_path, start, end)

        # one Counter per partition
        parts = [collections.Counter() for _ in range(R)]
        for w in tokenize(text):
            parts[stable_part(w, R)][w] += 1

        # write each partition as TSV (word \t count \n)
        wrote = []
        for r, counter in enumerate(parts):
            if not counter:
                continue
            out_path = os.path.join(out_dir, f"map_{task_id:05d}_part_{r:02d}.tsv")
            with io.open(out_path, "w", encoding="utf-8") as f:
                for w, c in counter.items():
                    f.write(f"{w}\t{c}\n")
            wrote.append(out_path)

        return f"OK {len(wrote)} files"

    def exposed_reduce(self, job_id: str, reducer_id: int) -> str:
        job_dir = os.path.join(DATA_DIR, "jobs", job_id)
        shuffle = os.path.join(job_dir, "shuffle")
        out_dir = os.path.join(job_dir, "reduce")
        os.makedirs(out_dir, exist_ok=True)

        pattern = f"part_{reducer_id:02d}.tsv"
        agg = collections.Counter()

        # read all map outputs for this reducer
        for name in os.listdir(shuffle):
            if name.endswith(pattern):
                with io.open(os.path.join(shuffle, name), "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        if not line:
                            continue
                        try:
                            w, c = line.rstrip("\n").split("\t")
                            agg[w] += int(c)
                        except ValueError:
                            pass

        out_path = os.path.join(out_dir, f"part_{reducer_id:02d}.tsv")
        with io.open(out_path, "w", encoding="utf-8") as f:
            for w, c in agg.items():
                f.write(f"{w}\t{c}\n")

        return out_path

if __name__ == "__main__":
    server = ThreadedServer(
        MapReduceService,
        hostname="0.0.0.0",
        port=18861,
        protocol_config={
            "allow_public_attrs": True,
            "sync_request_timeout": 300,
            "allow_pickle": False,  # not needed now; we pass only small strings/ints
        },
    )
    server.start()