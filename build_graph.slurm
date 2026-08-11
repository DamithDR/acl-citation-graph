#!/bin/bash
#SBATCH --job-name=acl-graph-build
#SBATCH --partition=cpu-48h
#SBATCH --output=log/output_%A_%a.log
#SBATCH --error=log/output_%A_%a.log
#SBATCH --time=48:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=40G

# ---------------------------------------------------------------------------
# Full ACL-Anthology citation-graph build on Slurm.
#
# Runs build_graph_bulk.py --download, which:
#   1. pulls the ACL Anthology node set (~120 MB metadata)
#   2. downloads the Semantic Scholar `papers` + `citations` bulk snapshots
#      (~150-200 GB transient) via the Datasets API (pre-signed links)
#   3. filters to intra-ACL edges, computes co-citation / coupling
#   4. writes web_data/ (sharded) + graph.json
#
# This is ONE sequential job, not an array — the download and build are a
# single pipeline and cannot be split across array tasks. The %A_%a log
# pattern still resolves (task id 0) so your logging path works unchanged.
#
# Submit:  sbatch build_graph.slurm
# ---------------------------------------------------------------------------

set -euo pipefail

# --- required: Semantic Scholar API key ------------------------------------
# Prefer exporting this in your shell / a private env file rather than editing
# this script. The 1 req/sec limiter in s2_client.py handles rate limiting.
: "${SEMANTIC_SCHOLAR_API_KEY:?Set SEMANTIC_SCHOLAR_API_KEY before submitting}"

# --- paths -----------------------------------------------------------------
# No scratch on this cluster: the bulk download lands in ./data/ next to the
# project. It needs ~150-200 GB free and the files are transient (safe to
# delete after the build completes). Make sure ./data/ has the space.
PROJECT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
export S2_DATA_DIR="$PROJECT_DIR/data"

cd "$PROJECT_DIR"
mkdir -p log
mkdir -p "$S2_DATA_DIR"

echo "=== job $SLURM_JOB_ID on $(hostname) ==="
echo "start:    $(date)"
echo "project:  $PROJECT_DIR"
echo "data dir: $S2_DATA_DIR"
echo "python:   $(which python3)"
echo

# --- environment -----------------------------------------------------------
source venv/bin/activate

# First run installs deps; comment out once your venv is provisioned.
python3 -m pip install --quiet acl-anthology-py requests ijson tqdm

# --- run -------------------------------------------------------------------
# --download fetches the S2 snapshots on the first run. On a re-run where
# ./data/ is already populated, drop --download to rebuild from cached files.
srun python3 build_graph_bulk.py --download

echo
echo "end: $(date)"
echo "outputs: web_data/ (sharded)  +  graph.json"
