#!/usr/bin/env python3
"""
build_graph.py — Offline builder for an ACL-Anthology-only citation graph.

Pipeline:
  1. Node set + metadata   <- acl-anthology Python package (official repo data)
  2. Citation edges        <- Semantic Scholar Academic Graph API
  3. Keep ONLY edges where both endpoints are ACL Anthology papers
  4. Precompute co-citation + bibliographic-coupling weights
  5. Export a compact graph.json the frontend loads (no runtime API calls)

Run once; ship the resulting graph.json with the app.

Requirements:
  pip install acl-anthology-py requests tqdm
  (optional) set SEMANTIC_SCHOLAR_API_KEY for higher rate limits.
"""

import json, os, time, itertools
from collections import defaultdict

import requests
from tqdm import tqdm
from acl_anthology import Anthology

import s2_client            # rate-limited (<=1 req/sec) API wrapper

S2_BASE = "https://api.semanticscholar.org/graph/v1"


# ---------- 1. Node set from ACL Anthology ----------
def load_acl_nodes():
    """Return {acl_id: metadata} for every paper in the Anthology."""
    anth = Anthology.from_repo()          # downloads ~120MB metadata on first run
    nodes = {}
    for paper in tqdm(anth.papers(), desc="ACL nodes"):
        if paper.is_frontmatter or paper.is_deleted:
            continue
        acl_id = paper.full_id            # e.g. "2022.acl-long.220"
        doi = paper.doi                   # 10.18653/v1/2022.acl-long.220 (often present)
        nodes[acl_id] = {
            "id": acl_id,
            "title": str(paper.title),
            "year": paper.year,
            "authors": [str(n) for n in (paper.authors or [])][:6],
            "venue": paper.venue_ids[0] if paper.venue_ids else None,
            "url": paper.web_url,
            "doi": doi,
            "s2": None,        # filled in step 2
            "cited_by": 0,     # in-corpus citation count, filled in step 4
        }
    return nodes


# ---------- 2. Resolve S2 ids + fetch references ----------
def s2_batch_lookup(dois):
    """Map DOI -> Semantic Scholar paperId in batches of 500."""
    mapping = {}
    dois = [d for d in dois if d]
    for i in tqdm(range(0, len(dois), 500), desc="S2 id map"):
        chunk = dois[i:i + 500]
        r = s2_client.post(          # throttled + 429 backoff handled inside
            f"{S2_BASE}/paper/batch",
            params={"fields": "externalIds"},
            json={"ids": [f"DOI:{d}" for d in chunk]},
        )
        if r.status_code != 200:
            continue
        for doi, obj in zip(chunk, r.json()):
            if obj and obj.get("paperId"):
                mapping[doi] = obj["paperId"]
    return mapping


def fetch_references(s2_id):
    """Return list of cited S2 paperIds for one paper."""
    refs, offset = [], 0
    while True:
        r = s2_client.get(           # throttled + 429 backoff handled inside
            f"{S2_BASE}/paper/{s2_id}/references",
            params={"fields": "citedPaper.paperId", "limit": 1000, "offset": offset},
        )
        if r.status_code != 200:
            break
        data = r.json().get("data", [])
        for item in data:
            cp = item.get("citedPaper") or {}
            if cp.get("paperId"):
                refs.append(cp["paperId"])
        if len(data) < 1000:
            break
        offset += 1000
    return refs


# ---------- 3 & 4. Build intra-anthology edges + weights ----------
def build(nodes):
    # resolve S2 ids
    doi_to_acl = {n["doi"]: aid for aid, n in nodes.items() if n["doi"]}
    doi_to_s2 = s2_batch_lookup(list(doi_to_acl))
    s2_to_acl = {}
    for doi, s2 in doi_to_s2.items():
        aid = doi_to_acl[doi]
        nodes[aid]["s2"] = s2
        s2_to_acl[s2] = aid

    # directed edges (citing -> cited), restricted to ACL papers only
    out_refs = {}   # acl_id -> set(acl_id) it references
    edges = []
    for aid, n in tqdm(nodes.items(), desc="edges"):
        if not n["s2"]:
            continue
        refs = fetch_references(n["s2"])
        acl_refs = {s2_to_acl[s] for s in refs if s in s2_to_acl}
        out_refs[aid] = acl_refs
        for tgt in acl_refs:
            edges.append([aid, tgt])
            nodes[tgt]["cited_by"] += 1

    # bibliographic coupling: papers sharing outgoing refs
    # co-citation: papers cited together by the same paper
    coupling = defaultdict(int)
    cocite = defaultdict(int)
    for aid, refs in out_refs.items():
        for a, b in itertools.combinations(sorted(refs), 2):
            cocite[(a, b)] += 1
    # invert to get, per target, who cites it (for coupling)
    cited_by_map = defaultdict(set)
    for src, refs in out_refs.items():
        for t in refs:
            cited_by_map[t].add(src)
    ref_sets = {a: r for a, r in out_refs.items() if r}
    keys = list(ref_sets)
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            shared = ref_sets[keys[i]] & ref_sets[keys[j]]
            if shared:
                coupling[(keys[i], keys[j])] = len(shared)

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
        "cocitation": [[a, b, w] for (a, b), w in cocite.items() if w > 1],
        "coupling": [[a, b, w] for (a, b), w in coupling.items() if w > 1],
    }


if __name__ == "__main__":
    nodes = load_acl_nodes()
    graph = build(nodes)
    with open("graph.json", "w") as f:
        json.dump(graph, f)
    print(f"Wrote graph.json: {len(graph['nodes'])} nodes, "
          f"{len(graph['edges'])} intra-ACL edges.")
