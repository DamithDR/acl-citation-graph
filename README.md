# Anthology Atlas — full ACL corpus build

A Litmaps-style citation mapper restricted to the ACL Anthology. Search seed
papers, expand by citation relationships, explore on an interactive graph.

## Files

| File | What it is |
|------|-----------|
| `acl-litmap-full.html` | The interactive app. Loads the sharded `web_data/`. |
| `acl-litmap.html` | Standalone demo (~30 papers baked in). Opens with no setup. |
| `build_graph_bulk.py` | Builds the graph for the **whole Anthology** (use this). |
| `build_graph.py` | Slower per-paper crawl. Fine for a small subset; not for 100k papers. |
| `s2_client.py` | Rate-limited (<=1 req/sec) Semantic Scholar client. Both builders import it. |
| `build_graph.slurm` | Slurm batch script to run the full build on a cluster. |

Keep `s2_client.py` in the same folder as the builders — they import it.

## Requirements

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export SEMANTIC_SCHOLAR_API_KEY=your_key      # free: semanticscholar.org/product/api
```

Python 3.11+ is required. The app itself is static (no Python) — these deps are
only for building the graph.

`s2_client.py` enforces the introductory 1 request/second limit across all
endpoints and handles HTTP 429 with Retry-After / backoff, so you never trip
the rate cap. If your key is later raised, call `s2_client.set_rate(N)` once.

## Build the full corpus — on Slurm (recommended)

The build downloads ~150-200 GB of transient Semantic Scholar snapshots into
`./data/` and needs a long single-node job.

```
export SEMANTIC_SCHOLAR_API_KEY=your_key
sbatch build_graph.slurm
```

Notes:
- Snapshots land in `./data/` (no scratch assumed). Make sure that filesystem
  has ~200 GB free: check `df -h .` and your quota before submitting.
- The `./data/` files are transient — safe to delete after the build writes
  `web_data/` and `graph.json`.
- Edit the environment section of the script (module load / conda / venv) for
  your cluster. Once your env has the deps, remove the pip-install line.
- Resumable: the download skips files already in `./data/`. On a re-run, drop
  `--download` from the `srun` line to rebuild from cached snapshots in minutes.

## Build the full corpus — locally (no Slurm)

```
export SEMANTIC_SCHOLAR_API_KEY=your_key
python build_graph_bulk.py --download    # first run: fetch snapshots into ./data/
python build_graph_bulk.py               # later: rebuild from cached ./data/
```

Override the download location with `S2_DATA_DIR=/some/path` if needed.

## Outputs

- `web_data/papers.json` — search index (metadata only, a few MB)
- `web_data/neighbors/<id>.json` — one small file per paper, its scored candidates
- `graph.json` — single-file version (only practical for subsets)

## Run the app

The full app fetches files, so serve the folder over HTTP (opening the .html
directly with file:// will hit CORS):

```
# put acl-litmap-full.html next to the web_data/ folder, then:
python -m http.server 8000
# open http://localhost:8000/acl-litmap-full.html
```

Without `web_data/`, the app falls back to a small inline sample so you can
still see it work.

## Why sharded

At full corpus scale the citation graph is too large to load into a browser.
An expansion only touches the current seeds, so the builder precomputes
per-paper neighbor files and the app fetches only the shards it needs. Memory
stays flat regardless of corpus size.

## Tuning

Relevance blend is `2*direct + 1.5*co-citation + coupling` (in `combine()` in
the HTML, mirrored in the shard payloads). Adjust to taste for your field.
