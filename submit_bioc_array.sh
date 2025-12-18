#!/bin/bash
#SBATCH --job-name=bioc_convert
#SBATCH --array=1-100%20              # Adjust range; max 20 concurrent jobs
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=04:00:00
#SBATCH --output=logs/bioc_%A_%a.out
#SBATCH --error=logs/bioc_%A_%a.err

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
#   sbatch submit_bioc_array.sh
#
# To resubmit failed tasks:
#   sbatch --array=5,12,47 submit_bioc_array.sh
# =============================================================================

# Configuration - MODIFY THESE PATHS
FILE_LIST="file_list.txt"
OUTPUT_BASE="/scratch/$USER/bioc_output"
CONTAINER="bioc_processor.sif"

# Create output base directory
mkdir -p "$OUTPUT_BASE"

# Get the file for this array task
BIOC_FILE=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$FILE_LIST")

# Skip if empty line
if [ -z "$BIOC_FILE" ]; then
    echo "No file for task $SLURM_ARRAY_TASK_ID"
    exit 0
fi

# Check if file exists
if [ ! -f "$BIOC_FILE" ]; then
    echo "Error: File not found: $BIOC_FILE"
    exit 1
fi

# Create output directory based on input filename
BASENAME=$(basename "$BIOC_FILE" .BioC.XML)
BASENAME=$(basename "$BASENAME" .bioc.xml)  # Handle both cases
OUTPUT_DIR="${OUTPUT_BASE}/${BASENAME}"

echo "============================================"
echo "Job Array Task: $SLURM_ARRAY_TASK_ID"
echo "Input File: $BIOC_FILE"
echo "Output Dir: $OUTPUT_DIR"
echo "Start Time: $(date)"
echo "============================================"

# Run the container
singularity run \
    --bind /data:/data \
    --bind /scratch:/scratch \
    "$CONTAINER" \
    "$BIOC_FILE" \
    "$OUTPUT_DIR"

EXIT_CODE=$?

echo "============================================"
echo "End Time: $(date)"
echo "Exit Code: $EXIT_CODE"
echo "============================================"

exit $EXIT_CODE
