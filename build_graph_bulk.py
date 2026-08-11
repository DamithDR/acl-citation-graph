#!/usr/bin/env python3
"""
build_graph_bulk.py — Build the ACL-Anthology-only citation graph for the
FULL corpus, using the Semantic Scholar *Datasets* API (bulk snapshots)
instead of per-paper crawling.

Why this instead of build_graph.py:
  The per-paper /references crawl is ~100k rate-limited calls (~a day).
  The Datasets API ships the entire citation graph as gzipped JSON files.
  You download 3 datasets once, then filter locally in minutes.

Datasets used (see api.semanticscholar.org/datasets/v1):
  papers     - core metadata (corpusid, title, year, externalids, citationcount)
  paper-ids  - id mappings (we actually get DOI->corpusid straight from papers.externalids)
  citations  - directed edges: citingcorpusid -> citedcorpusid

Steps:
  1. ACL node set + DOIs        <- acl-anthology package
  2. Download papers + citations release files  <- Datasets API (needs API key)
  3. Build DOI -> corpusid map by streaming papers files
  4. Stream citations files, keep edges where BOTH endpoints are ACL corpusids
  5. Precompute co-citation + coupling, write graph.json

Requirements:
  pip install acl-anthology-py requests ijson tqdm
  export SEMANTIC_SCHOLAR_API_KEY=...     (free; required for Datasets API)

Usage:
  python build_graph_bulk.py --download   # first run: fetch snapshots (~large)
  python build_graph_bulk.py              # subsequent: reuse ./s2data, rebuild
"""

import os, gzip, json, argparse, itertools
from collections import defaultdict

import requests
from tqdm import tqdm
from acl_anthology import Anthology

import s2_client            # rate-limited (<=1 req/sec) API wrapper

API = "https://api.semanticscholar.org/datasets/v1"
KEY = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
DATADIR = os.environ.get("S2_DATA_DIR", "data")   # where bulk snapshots land


# ---------- 1. ACL nodes ----------
def acl_nodes():
    anth = Anthology.from_repo()
    nodes = {}
    for p in tqdm(anth.papers(), desc="ACL nodes"):
        if p.is_frontmatter or p.is_deleted:
            continue
        nodes[p.full_id] = {
            "id": p.full_id, "title": str(p.title), "year": p.year,
            "authors": [str(n) for n in (p.authors or [])][:6],
            "venue": p.venue_ids[0] if p.venue_ids else None,
            "url": p.web_url, "doi": (p.doi or "").lower() or None,
            "corpusid": None, "cited_by": 0,
        }
    return nodes


# ---------- 2. Download bulk snapshots ----------
def latest_release():
    r = s2_client.get(f"{API}/release/latest")       # rate-limited API call
    r.raise_for_status()
    return r.json()["release_id"]

def dataset_files(release, name):
    r = s2_client.get(f"{API}/release/{release}/dataset/{name}")   # rate-limited
    r.raise_for_status()
    return r.json()["files"]           # list of pre-signed download URLs

def download(name):
    rel = latest_release()
    os.makedirs(f"{DATADIR}/{name}", exist_ok=True)
    files = dataset_files(rel, name)
    print(f"{name}: {len(files)} files (release {rel})")
    # NOTE: these file URLs are pre-signed storage links, NOT API endpoints,
    # so they are exempt from the 1 req/sec limit — download them at full speed.
    for i, url in enumerate(tqdm(files, desc=f"dl {name}")):
        dest = f"{DATADIR}/{name}/{i:05d}.json.gz"
        if os.path.exists(dest):
            continue
        with requests.get(url, stream=True, timeout=600) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(1 << 20):
                    f.write(chunk)

def iter_records(name):
    d = f"{DATADIR}/{name}"
    for fn in sorted(os.listdir(d)):
        with gzip.open(f"{d}/{fn}", "rt") as f:
            for line in f:              # each line is one JSON record
                if line.strip():
                    yield json.loads(line)


# ---------- 3-5. Build ----------
def build(nodes, want_download):
    if want_download:
        download("papers")
        download("citations")

    doi_to_acl = {n["doi"]: aid for aid, n in nodes.items() if n["doi"]}

    # 3. DOI -> corpusid via papers.externalids
    print("mapping DOIs to corpusids…")
    corpus_to_acl = {}
    for rec in tqdm(iter_records("papers"), desc="papers"):
        ext = rec.get("externalids") or {}
        doi = (ext.get("DOI") or "").lower()
        if doi and doi in doi_to_acl:
            aid = doi_to_acl[doi]
            cid = rec.get("corpusid")
            nodes[aid]["corpusid"] = cid
            corpus_to_acl[cid] = aid

    acl_corpus = set(corpus_to_acl)
    print(f"resolved {len(acl_corpus)}/{len(nodes)} ACL papers to S2 corpusids")

    # 4. keep intra-ACL edges
    print("filtering citation edges…")
    out_refs = defaultdict(set)
    edges = []
    for rec in tqdm(iter_records("citations"), desc="citations"):
        c = rec.get("citingcorpusid"); t = rec.get("citedcorpusid")
        if c in acl_corpus and t in acl_corpus:
            a, b = corpus_to_acl[c], corpus_to_acl[t]
            edges.append([a, b]); out_refs[a].add(b)
            nodes[b]["cited_by"] += 1

    # 5. co-citation + coupling
    print("computing co-citation / coupling…")
    cocite, coupling = defaultdict(int), defaultdict(int)
    for refs in out_refs.values():
        for x, y in itertools.combinations(sorted(refs), 2):
            cocite[(x, y)] += 1
    keys = [k for k, v in out_refs.items() if v]
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            sh = out_refs[keys[i]] & out_refs[keys[j]]
            if sh:
                coupling[(keys[i], keys[j])] = len(sh)

    return nodes, edges, cocite, coupling, out_refs


# ---------- 6. Shard for the browser ----------
def write_sharded(nodes, edges, cocite, coupling, out_refs, outdir="web_data"):
    """
    The full graph is too big to ship as one JSON. Emit:
      papers.json          - id, title, year, authors, venue, cited_by  (search index)
      neighbors/<id>.json  - for each seed paper, its scored candidates + direct links
    The app loads papers.json once, then fetches only the neighbor files it needs.
    """
    import os
    os.makedirs(f"{outdir}/neighbors", exist_ok=True)

    # search index: metadata only, no edges — a few MB even at 100k papers
    with open(f"{outdir}/papers.json", "w") as f:
        json.dump([{k: n[k] for k in
                    ("id", "title", "year", "authors", "venue", "cited_by")}
                   for n in nodes.values()], f)

    # invert co-citation / coupling to per-node adjacency
    co_adj, cp_adj = defaultdict(dict), defaultdict(dict)
    for (a, b), w in cocite.items():
        if w > 1: co_adj[a][b] = w; co_adj[b][a] = w
    for (a, b), w in coupling.items():
        if w > 1: cp_adj[a][b] = w; cp_adj[b][a] = w
    direct = defaultdict(set)
    for a, b in edges:
        direct[a].add(b); direct[b].add(a)

    # one neighbor file per paper: everything needed to score expansions from it
    for nid in tqdm(nodes, desc="shards"):
        neigh = set(co_adj[nid]) | set(cp_adj[nid]) | direct[nid]
        if not neigh:
            continue
        payload = {c: {
            "co": co_adj[nid].get(c, 0),
            "cp": cp_adj[nid].get(c, 0),
            "dr": 1 if c in direct[nid] else 0,
        } for c in neigh}
        safe = nid.replace("/", "_")
        with open(f"{outdir}/neighbors/{safe}.json", "w") as f:
            json.dump(payload, f)
    print(f"sharded into {outdir}/ (papers.json + {len(nodes)} neighbor files)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--download", action="store_true",
                    help="fetch S2 snapshots into ./s2data (first run)")
    args = ap.parse_args()
    if not KEY:
        raise SystemExit("Set SEMANTIC_SCHOLAR_API_KEY (free key from "
                         "https://www.semanticscholar.org/product/api).")
    nodes = acl_nodes()
    nodes, edges, cocite, coupling, out_refs = build(nodes, args.download)

    # Always emit the sharded web_data/ (scales to the full corpus).
    write_sharded(nodes, edges, cocite, coupling, out_refs)

    # Also emit a single graph.json — handy for subsets (e.g. one venue/decade)
    # that comfortably fit in the browser. For the FULL corpus, use web_data/.
    single = {
        "nodes": [{k: v for k, v in n.items() if k != "corpusid"} for n in nodes.values()],
        "edges": edges,
        "cocitation": [[a, b, w] for (a, b), w in cocite.items() if w > 1],
        "coupling":   [[a, b, w] for (a, b), w in coupling.items() if w > 1],
    }
    with open("graph.json", "w") as f:
        json.dump(single, f)
    print(f"\nWrote graph.json: {len(single['nodes'])} nodes, "
          f"{len(single['edges'])} intra-ACL edges.")
    print("For the full corpus in-browser, serve web_data/ and use the "
          "sharded loader (see README).")
