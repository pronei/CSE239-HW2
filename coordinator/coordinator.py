import os
import sys
import glob
import math
import time
import queue
import uuid
import threading
import urllib.request
import zipfile
from typing import List, Tuple

import io
import collections
import rpyc

DATA_DIR = os.environ.get("MR_DATA_DIR", "/data")
NUM_REDUCERS = int(os.environ.get("M_REDUCERS", "3"))
WORKERS = [("worker-1", 18861), ("worker-2", 18861), ("worker-3", 18861), ("worker-4", 18861)]
RPC_TIMEOUT_SECONDS = 300.0  # large, but no big payloads now
MAX_RETRIES_PER_TASK = 5

def download_to_data(url: str) -> List[str]:
    input_dir = os.path.join(DATA_DIR, "input")
    txt_dir = os.path.join(input_dir, "txt")
    os.makedirs(txt_dir, exist_ok=True)

    zip_name = os.path.basename(url)
    zip_path = os.path.join(input_dir, zip_name)

    # Download if not present
    if not os.path.exists(zip_path):
        print(f"[download] downloading {url} -> {zip_path}")
        urllib.request.urlretrieve(url, zip_path)
    else:
        print(f"[download] {zip_path} exists")

    # Always extract (idempotent) — we’ll normalize afterward
    print(f"[download] extracting {zip_path} into {txt_dir}/")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(txt_dir)

    # Normalize: flatten one directory level, and ensure .txt extension for files
    # 1) move inner files up if there’s exactly one subdir
    for p in list(glob.glob(os.path.join(txt_dir, "*"))):
        if os.path.isdir(p):
            for inner in glob.glob(os.path.join(p, "*")):
                base = os.path.basename(inner)
                dst = os.path.join(txt_dir, base)
                if os.path.isdir(inner):
                    continue  # ignore nested dirs
                if not dst.endswith(".txt"):
                    dst += ".txt"
                if not os.path.exists(dst):
                    os.rename(inner, dst)
            # try to remove the now-empty dir
            try:
                os.rmdir(p)
            except OSError:
                pass

    # 2) ensure top-level files have .txt
    for p in list(glob.glob(os.path.join(txt_dir, "*"))):
        if os.path.isdir(p):
            continue
        if not p.endswith(".txt"):
            dst = p + ".txt"
            if not os.path.exists(dst):
                os.rename(p, dst)

    files = sorted(glob.glob(os.path.join(txt_dir, "*.txt")))
    if not files:
        print("[download] debug listing:")
        for p in glob.glob(os.path.join(txt_dir, "*")):
            print(" -", p, "(dir)" if os.path.isdir(p) else "(file)")
        raise SystemExit("No input .txt files found after extraction+normalize.")

    print(f"[download] found {len(files)} txt file(s). Example: {os.path.basename(files[0])}")
    return files


def file_size(relpath: str) -> int:
    return os.stat(os.path.join(DATA_DIR, relpath)).st_size

def plan_map_tasks(files: List[str], target_chunk_bytes: int = 8 * 1024 * 1024) -> List[Tuple[str, int, int, int]]:
    """Return list of map tasks: (task_id, file_path, start, end)."""
    tasks = []
    tid = 0
    for fp in files:
        size = file_size(fp)
        if size == 0:
            continue
        chunks = max(1, math.ceil(size / target_chunk_bytes))
        chunk = math.ceil(size / chunks)
        start = 0
        while start < size:
            end = min(size, start + chunk)
            tasks.append((tid, fp, start, end))
            tid += 1
            start = end
    return tasks

def call_map(worker, job_id, tid, fp, start, end, R):
    host, port = worker
    conn = None
    try:
        conn = rpyc.connect(host, port, config={
            "allow_public_attrs": True,
            "sync_request_timeout": RPC_TIMEOUT_SECONDS,
            "allow_pickle": False,
        })
        return conn.root.map(job_id, tid, fp, start, end, R)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def call_reduce(worker, job_id, rid):
    host, port = worker
    conn = None
    try:
        conn = rpyc.connect(host, port, config={
            "allow_public_attrs": True,
            "sync_request_timeout": RPC_TIMEOUT_SECONDS,
            "allow_pickle": False,
        })
        return conn.root.reduce(job_id, rid)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def run(job_url: str):
    input_files = download_to_data(job_url)
    rel_input_files = [os.path.relpath(p, DATA_DIR) for p in input_files]
    if not rel_input_files:
        print("No input files found in shared volume.")
        sys.exit(1)

    # Unique job directory in the shared volume
    job_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    os.makedirs(os.path.join(job_dir, "shuffle"), exist_ok=True)
    os.makedirs(os.path.join(job_dir, "reduce"), exist_ok=True)
    print(f"[job] id={job_id} files={len(input_files)} reducers={NUM_REDUCERS}")

    # ---- Start timing AFTER download ----
    start_time = time.time()

    # --- MAP phase: schedule byte-range tasks (no payloads over RPC) ---
    tasks = plan_map_tasks(input_files, target_chunk_bytes=8 * 1024 * 1024)
    print(f"[map] planned {len(tasks)} tasks")

    tqueue = queue.Queue()
    for t in tasks:
        tqueue.put(t + (0,))  # append retries=0

    def map_worker_loop(worker):
        while True:
            try:
                tid, fp, start, end, retries = tqueue.get_nowait()
            except queue.Empty:
                return
            try:
                msg = call_map(worker, job_id, tid, fp, start, end, NUM_REDUCERS)
                print(f"[map] {worker[0]} ok task {tid}: {msg}")
                tqueue.task_done()
            except Exception as e:
                print(f"[map] {worker[0]} fail task {tid} (retry {retries}): {e!r}")
                if retries + 1 < MAX_RETRIES_PER_TASK:
                    tqueue.put((tid, fp, start, end, retries + 1))
                else:
                    print(f"[map] giving up task {tid}")
                tqueue.task_done()
                time.sleep(0.2)

    threads = []
    for w in WORKERS:
        t = threading.Thread(target=map_worker_loop, args=(w,), daemon=True)
        t.start()
        threads.append(t)

    tqueue.join()
    for t in threads:
        t.join()
    print("[map] complete; intermediates are on disk")

    # --- REDUCE phase: one task per reducer id ---
    rqueue = queue.Queue()
    for rid in range(NUM_REDUCERS):
        rqueue.put((rid, 0))

    def reduce_worker_loop(worker):
        while True:
            try:
                rid, retries = rqueue.get_nowait()
            except queue.Empty:
                return
            try:
                outp = call_reduce(worker, job_id, rid)
                print(f"[reduce] {worker[0]} wrote {outp}")
                rqueue.task_done()
            except Exception as e:
                print(f"[reduce] {worker[0]} fail part {rid} (retry {retries}): {e!r}")
                if retries + 1 < MAX_RETRIES_PER_TASK:
                    rqueue.put((rid, retries + 1))
                else:
                    print(f"[reduce] giving up part {rid}")
                rqueue.task_done()
                time.sleep(0.2)

    threads = []
    # simple round-robin: start N threads, each pulls parts
    for w in WORKERS:
        t = threading.Thread(target=reduce_worker_loop, args=(w,), daemon=True)
        t.start()
        threads.append(t)

    rqueue.join()
    for t in threads:
        t.join()

    # --- Final: read reduced parts and print top 20 ---
    totals = collections.Counter()
    for rid in range(NUM_REDUCERS):
        part = os.path.join(job_dir, "reduce", f"part_{rid:02d}.tsv")
        if not os.path.exists(part):
            continue
        with io.open(part, "r", encoding="utf-8") as f:
            for line in f:
                w, c = line.rstrip("\n").split("\t")
                totals[w] += int(c)

    top = totals.most_common(20)
    print("\nTOP 20 WORDS\n")
    for i, (w, c) in enumerate(top, 1):
        print(f"{i:2d}. {w:<20} {c}")

    # ---- End timing and print summary ----
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time: {} seconds".format(round(elapsed_time, 2)))

if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "https://mattmahoney.net/dc/enwik8.zip"
    run(url)
