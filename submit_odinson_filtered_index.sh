#!/bin/bash
#SBATCH --job-name=odinson_filtered
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=72:00:00
#SBATCH --output=logs/odinson_filtered_%j.out
#SBATCH --error=logs/odinson_filtered_%j.err

set -euo pipefail
trap 'rc=$?; echo "Script exiting with code $rc"; exit $rc' EXIT

# =============================================================================
# Odinson FILTERED Document Indexing
# =============================================================================
#
# Creates an index from only specific paper sections:
#   title, abstract, intro, methods, results, discuss, concl
#
# Usage:
#   sbatch submit_odinson_filtered_index.sh <parent_directory> [index_name]
#
# Example:
#   sbatch submit_odinson_filtered_index.sh /shared/bfr027/bioc_output
#
# =============================================================================

# Configuration
SHARED_ROOT="/shared/bfr027"
INDEX_BASE="${SHARED_ROOT}/odinson_index"
STAGING_BASE="${SHARED_ROOT}/odinson_staging"
CONTAINER="/shared/$USER/bioc-processor/odinson_indexer.sif"

# Sections to include (filename patterns: docid_section.json)
SECTIONS="title abstract intro methods results discuss concl"

# Get docs directory from argument
DOCS_DIR="${1:-}"
INDEX_NAME="${2:-}"

if [ -z "$DOCS_DIR" ]; then
    echo "Error: No documents directory specified"
    echo "Usage: sbatch submit_odinson_filtered_index.sh <parent_directory> [index_name]"
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

# Set index name
if [ -z "$INDEX_NAME" ]; then
    INDEX_NAME="$(basename "$DOCS_DIR")_filtered"
fi
INDEX_DIR="${INDEX_BASE}/${INDEX_NAME}"
STAGING_DIR="${STAGING_BASE}/${INDEX_NAME}"

# Clean up any previous staging directory
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR"
mkdir -p "$INDEX_DIR"
mkdir -p logs

echo "============================================"
echo "Odinson FILTERED Document Indexer"
echo "============================================"
echo "Source Dir: $DOCS_DIR"
echo "Staging Dir: $STAGING_DIR"
echo "Index Dir: $INDEX_DIR"
echo "Sections: $SECTIONS"
echo "Start Time: $(date)"
echo "============================================"

# Count total documents first
echo "Counting total JSON files..."
TOTAL_COUNT=$(find "$DOCS_DIR" -name "*.json" 2>/dev/null | wc -l)
echo "Total JSON files: $TOTAL_COUNT"

# Create symlinks for only the sections we want
echo ""
echo "Creating symlinks for selected sections..."
echo "Using parallel processing for speed..."

# Build find pattern for all sections at once
FIND_PATTERN=""
for section in $SECTIONS; do
    if [ -z "$FIND_PATTERN" ]; then
        FIND_PATTERN="-name *_${section}.json"
    else
        FIND_PATTERN="$FIND_PATTERN -o -name *_${section}.json"
    fi
done

# Create symlinks in parallel using xargs
# The awk command creates unique link names: parentfolder_filename
echo "Finding and linking files (this may take 10-30 minutes for 14M files)..."
find "$DOCS_DIR" \( $FIND_PATTERN \) -print0 2>/dev/null | \
    xargs -0 -P 16 -I {} sh -c '
        file="{}"
        basename_file=$(basename "$file")
        parent_folder=$(basename "$(dirname "$file")")
        link_name="${parent_folder}_${basename_file}"
        ln -sf "$file" "'"$STAGING_DIR"'/${link_name}"
    '

# Count created symlinks
LINK_COUNT=$(find "$STAGING_DIR" -type l 2>/dev/null | wc -l)

# Show per-section counts
echo ""
echo "Files per section:"
for section in $SECTIONS; do
    SECTION_COUNT=$(find "$STAGING_DIR" -name "*_${section}.json" -type l 2>/dev/null | wc -l)
    echo "  ${section}: $SECTION_COUNT"
done

echo ""
echo "============================================"
echo "Created $LINK_COUNT symlinks"
echo "Filtered from $TOTAL_COUNT to $LINK_COUNT files"
echo "============================================"

if [ "$LINK_COUNT" -eq 0 ]; then
    echo "ERROR: No matching files found!"
    echo "Expected filenames like: 12345_abstract.json, 12345_methods.json, etc."
    exit 1
fi

echo ""
echo "Starting indexing at $(date)"
echo "============================================"

# Run the container on the staging directory
export SINGULARITYENV_JAVA_OPTS="-Xmx96G -XX:+UseG1GC"
singularity run -c --bind /shared/$USER:/shared/$USER,/home/$USER:/home/$USER --net --network none /shared/$USER/bioc-processor/odinson_indexer.sif --docs-dir "$STAGING_DIR" --index-dir "$INDEX_DIR" 2>&1

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

# Optionally clean up staging directory
echo ""
echo "Staging directory preserved at: $STAGING_DIR"
echo "To remove: rm -rf $STAGING_DIR"

exit $EXIT_CODE
