#!/bin/bash
#SBATCH --job-name=bioc_convert
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=04:00:00
#SBATCH --output=logs/bioc_%A_%a.out
#SBATCH --error=logs/bioc_%A_%a.err

set -euo pipefail
trap 'rc=$?; echo "Script exiting with code $rc"; exit $rc' EXIT

# =============================================================================
# BioC to Odinson Batch Processing with Singularity
# =============================================================================
#
# Prerequisites:
#   1. Create file_list.txt with one BioC file path per line
#   2. Build container: singularity build bioc_processor.sif bioc_processor.def
#   3. Create logs directory: mkdir -p logs
#
# Usage:
#   Use submit_chunks.sh for automatic chunk-based submission:
#     ./submit_chunks.sh
#
#   Or submit manually with array range:
#     sbatch --array=1-1000%20 submit_bioc_array.sh
#
#   For local testing:
#     SLURM_ARRAY_TASK_ID=1 ./submit_bioc_array.sh
#
# To resubmit failed tasks:
#   sbatch --array=5,12,47 submit_bioc_array.sh
# =============================================================================

# Configuration - MODIFY THESE PATHS
FILE_LIST="file_list.txt"
# Default to locations under /shared/bfr027 so this script only uses allowed paths
SHARED_ROOT="/shared/bfr027"
OUTPUT_BASE="${SHARED_ROOT}/bioc_output"
# Allow overriding CONTAINER with an absolute path, but default to the shared dir
CONTAINER="${SHARED_ROOT}/bioc-processor/bioc_processor.sif"

# Basic checks
if [ ! -f "$FILE_LIST" ]; then
    echo "Error: FILE_LIST not found: $FILE_LIST"
    exit 1
fi

# Note: singularity check removed - cluster validates singularity commands

if [ ! -f "$CONTAINER" ]; then
    echo "Error: container not found: $CONTAINER"
    exit 1
fi

mkdir -p "$OUTPUT_BASE"
mkdir -p "$SHARED_ROOT"
mkdir -p logs

# Allow running outside SLURM for testing: CLI arg or SLURM_ARRAY_TASK_ID
ARRAY_TASK_ID="${SLURM_ARRAY_TASK_ID:-${1:-1}}"

# FILE_OFFSET is passed from submit_chunks.sh to handle MaxArraySize limits
# The actual line number = ARRAY_TASK_ID + FILE_OFFSET
FILE_OFFSET="${FILE_OFFSET:-0}"
TASK_ID=$((ARRAY_TASK_ID + FILE_OFFSET))

# Get the file for this array task (skip empty lines, handle CRLF)
BIOC_FILE=$(sed -n "${TASK_ID}p" "$FILE_LIST" | tr -d '\r')

if [ -z "$BIOC_FILE" ]; then
    echo "No file for task $TASK_ID"
    exit 0
fi

# Check if file exists
if [ ! -f "$BIOC_FILE" ]; then
    echo "Error: File not found: $BIOC_FILE"
    exit 1
fi

# Create output directory based on input filename (case-insensitive strip of .bioc.xml)
BASENAME=$(basename "$BIOC_FILE")
# strip .bioc.xml or .BioC.XML (case-insensitive)
BASENAME=${BASENAME%.[Bb][iI][oO][cC].[xX][mM][lL]}
OUTPUT_DIR="${OUTPUT_BASE}/${BASENAME}"

echo "============================================"
echo "Job Array Task: $ARRAY_TASK_ID (File Line: $TASK_ID, Offset: $FILE_OFFSET)"
echo "Input File: $BIOC_FILE"
echo "Output Dir: $OUTPUT_DIR"
echo "Container: $CONTAINER"
echo "Start Time: $(date)"
echo "============================================"

# Run the container (using cluster-required singularity format - must be on single line)
singularity run -c --bind /shared/$USER:/shared/$USER,/home/$USER:/home/$USER --net --network none /shared/bfr027/bioc-processor/bioc_processor.sif ${BIOC_FILE} ${OUTPUT_DIR}

EXIT_CODE=$?

echo "============================================"
echo "End Time: $(date)"
echo "Exit Code: $EXIT_CODE"
echo "============================================"

exit $EXIT_CODE
