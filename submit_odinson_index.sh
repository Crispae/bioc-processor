#!/bin/bash
#SBATCH --job-name=odinson_index
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=08:00:00
#SBATCH --output=logs/odinson_index_%j.out
#SBATCH --error=logs/odinson_index_%j.err

set -euo pipefail
trap 'rc=$?; echo "Script exiting with code $rc"; exit $rc' EXIT

# =============================================================================
# Odinson Document Indexing with Singularity
# =============================================================================
#
# Usage:
#   sbatch submit_odinson_index.sh <docs_directory>
#
# Example:
#   sbatch submit_odinson_index.sh /shared/bfr027/bioc_output/3110
#
# =============================================================================

# Configuration - MODIFY THESE PATHS
SHARED_ROOT="/shared/bfr027"
INDEX_BASE="${SHARED_ROOT}/odinson_index"
CONTAINER="/shared/$USER/bioc-processor/odinson_indexer.sif"

# Get docs directory from argument or use default
DOCS_DIR="${1:-}"

if [ -z "$DOCS_DIR" ]; then
    echo "Error: No documents directory specified"
    echo "Usage: sbatch submit_odinson_index.sh <docs_directory>"
    exit 1
fi

if [ ! -d "$DOCS_DIR" ]; then
    echo "Error: Documents directory not found: $DOCS_DIR"
    exit 1
fi

if [ ! -f "$CONTAINER" ]; then
    echo "Error: Container not found: $CONTAINER"
    exit 1
fi

# Create index output directory based on docs directory name
DOCS_BASENAME=$(basename "$DOCS_DIR")
INDEX_DIR="${INDEX_BASE}/${DOCS_BASENAME}"

mkdir -p "$INDEX_DIR"
mkdir -p logs

echo "============================================"
echo "Odinson Document Indexer"
echo "============================================"
echo "Documents Dir: $DOCS_DIR"
echo "Index Dir: $INDEX_DIR"
echo "Container: $CONTAINER"
echo "Start Time: $(date)"
echo "============================================"

# Count documents
DOC_COUNT=$(find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" 2>/dev/null | wc -l)
echo "Found $DOC_COUNT JSON files to index"

# Run the container (using cluster-required singularity format)
# Note: Container path must be literal (not a variable) for cluster validation
singularity run -c --bind /shared/$USER:/shared/$USER,/home/$USER:/home/$USER --net --network none /shared/$USER/bioc-processor/odinson_indexer.sif --docs-dir "$DOCS_DIR" --index-dir "$INDEX_DIR"

EXIT_CODE=$?

echo "============================================"
echo "End Time: $(date)"
echo "Exit Code: $EXIT_CODE"
echo "============================================"

# Verify index was created
if [ -d "$INDEX_DIR" ] && [ "$(ls -A $INDEX_DIR 2>/dev/null)" ]; then
    echo "Index created successfully at: $INDEX_DIR"
    ls -la "$INDEX_DIR"
else
    echo "WARNING: Index directory is empty or not created"
fi

exit $EXIT_CODE