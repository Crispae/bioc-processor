#!/bin/bash
#SBATCH --job-name=odinson_bulk
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=72:00:00
#SBATCH --output=logs/odinson_bulk_%j.out
#SBATCH --error=logs/odinson_bulk_%j.err

set -euo pipefail
trap 'rc=$?; echo "Script exiting with code $rc"; exit $rc' EXIT

# =============================================================================
# Odinson BULK Document Indexing - Single Index from All Subdirectories
# =============================================================================
#
# Creates a single unified index from all JSON files across all subdirectories.
# Designed for indexing large collections (e.g., 11k+ folders).
#
# Usage:
#   sbatch submit_odinson_bulk_index.sh <parent_directory> [index_name]
#
# Examples:
#   # Index all folders under bioc_output, create index named "bioc_full"
#   sbatch submit_odinson_bulk_index.sh /shared/bfr027/bioc_output
#
#   # Specify custom index name
#   sbatch submit_odinson_bulk_index.sh /shared/bfr027/bioc_output my_full_index
#
# =============================================================================

# Configuration
SHARED_ROOT="/shared/bfr027"
INDEX_BASE="${SHARED_ROOT}/odinson_index"
CONTAINER="/shared/$USER/bioc-processor/odinson_indexer.sif"

# Get docs directory from argument
DOCS_DIR="${1:-}"
INDEX_NAME="${2:-}"

if [ -z "$DOCS_DIR" ]; then
    echo "Error: No documents directory specified"
    echo "Usage: sbatch submit_odinson_bulk_index.sh <parent_directory> [index_name]"
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

# Set index name - use provided name or derive from directory
if [ -z "$INDEX_NAME" ]; then
    INDEX_NAME="$(basename "$DOCS_DIR")_full"
fi
INDEX_DIR="${INDEX_BASE}/${INDEX_NAME}"

mkdir -p "$INDEX_DIR"
mkdir -p logs

echo "============================================"
echo "Odinson BULK Document Indexer"
echo "============================================"
echo "Documents Dir: $DOCS_DIR"
echo "Index Dir: $INDEX_DIR"
echo "Index Name: $INDEX_NAME"
echo "Container: $CONTAINER"
echo "Start Time: $(date)"
echo "============================================"

# Count subdirectories
SUBDIR_COUNT=$(find "$DOCS_DIR" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
echo "Found $SUBDIR_COUNT subdirectories"

# Count total documents (this may take a while for large collections)
echo "Counting JSON files (this may take a few minutes)..."
DOC_COUNT=$(find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" 2>/dev/null | wc -l)
echo "Found $DOC_COUNT total JSON files to index"

if [ "$DOC_COUNT" -eq 0 ]; then
    echo "ERROR: No JSON files found in $DOCS_DIR or its subdirectories"
    exit 1
fi

echo "============================================"
echo "Starting indexing at $(date)"
echo "============================================"

# Run the container with the parent directory
# Odinson will recursively find all JSON files in subdirectories
# Set Java heap to 96GB for large-scale indexing (14M+ documents)
export SINGULARITYENV_JAVA_OPTS="-Xmx96G -XX:+UseG1GC"
singularity run -c --bind /shared/$USER:/shared/$USER,/home/$USER:/home/$USER --net --network none /shared/$USER/bioc-processor/odinson_indexer.sif --docs-dir "$DOCS_DIR" --index-dir "$INDEX_DIR" 2>&1

EXIT_CODE=$?

echo "============================================"
echo "End Time: $(date)"
echo "Exit Code: $EXIT_CODE"
echo "============================================"

# Verify index was created
if [ -d "$INDEX_DIR" ] && [ "$(ls -A $INDEX_DIR 2>/dev/null)" ]; then
    echo "Index created successfully at: $INDEX_DIR"
    echo ""
    echo "Index contents:"
    ls -la "$INDEX_DIR"
    echo ""
    echo "Index size:"
    du -sh "$INDEX_DIR"
else
    echo "WARNING: Index directory is empty or not created"
fi

exit $EXIT_CODE
