# RPyC MapReduce (Word Count)

Coordinator schedules map/reduce tasks over **RPyC**. Workers read inputs and write intermediates/outputs on a **shared Docker volume**. Chunking is boundary-safe (no overlap). Timing starts **after** download and prints `TOP 20 WORDS BY FREQUENCY` and `Elapsed Time: … seconds`.

## Prerequisites
- Docker Engine (v24+) and Docker Compose v2
- Internet access (downloads **enwik9**)

## Repository Layout
```
hw2/
├── docker-compose.yml
├── requirements.txt
├── worker/
│   ├── Dockerfile
│   └── worker.py
└── coordinator/
    ├── Dockerfile
    └── coordinator.py
```

## Build
```bash
cd hw2
docker compose build
```

## Run
```bash
docker compose up
```
Behavior:
- Coordinator downloads and extracts **enwik9** to `/data/input/enwik9/txt/`.
- Map phase writes shuffle files under `/data/jobs/<job_id>/shuffle/`.
- Reduce phase writes `/data/jobs/<job_id>/reduce/part_XX.tsv`.
- Coordinator prints **TOP 20** and compute **Elapsed Time**.

## Configuration (set in `docker-compose.yml`)
```
MR_DATA_DIR=/data
MR_DATASET=enwik9
MR_DATASET_URL=https://mattmahoney.net/dc/enwik9.zip
MR_NUM_REDUCERS=3
```
Workers listen on port `18861` (one instance may be published for debugging).

## Output
- Reduced parts: `/data/jobs/<job_id>/reduce/part_00.tsv`, `part_01.tsv`, …
- List results:
```bash
docker compose exec coordinator ls -al /data/jobs
```

## Enforcing enwik9 Only
If you previously ran enwik8 and the volume is `hw2_mrdata`:
```bash
docker run --rm -v hw2_mrdata:/data alpine sh -c 'rm -rf /data/input/enwik8* /data/input/txt || true'
docker compose up --build
```
Clean slate (removes all data):
```bash
docker compose down -v
docker compose up --build
```

## Notes on Requirements
- **RPC**: Coordinator↔workers use RPyC.
- **File I/O**: Workers read inputs and write intermediates/outputs on shared volume.
- **Concurrency & Rescheduling**: Per-phase task queues with per-worker threads; retries with caps.
- **Boundary-Safe Chunking**: Workers extend reads to token boundary and drop head partial tokens.
- **Timing**: Timer starts after download; prints total compute time and top-20.
