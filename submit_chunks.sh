#!/bin/bash
# =============================================================================
# Chunk-based Job Submission for Large File Lists
# =============================================================================
#
# Automatically detects file count and submits jobs in manageable chunks.
# This avoids the need to manually specify array ranges for large file lists.
#
# Usage:
#   ./submit_chunks.sh                          # Submit all with defaults
#   ./submit_chunks.sh --chunk-size 500         # Custom chunk size
#   ./submit_chunks.sh --dry-run                # Preview without submitting
#   ./submit_chunks.sh --file-list my_files.txt # Custom file list
#
# =============================================================================

set -euo pipefail

# Default configuration
CHUNK_SIZE=1000
MAX_CONCURRENT=50
FILE_LIST="file_list.txt"
DRY_RUN=false
JOB_SCRIPT="submit_bioc_array.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --chunk-size)
            CHUNK_SIZE="$2"
            shift 2
            ;;
        --max-concurrent)
            MAX_CONCURRENT="$2"
            shift 2
            ;;
        --file-list)
            FILE_LIST="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --chunk-size N      Number of files per chunk (default: 1000)"
            echo "  --max-concurrent N  Max concurrent jobs per chunk (default: 20)"
            echo "  --file-list FILE    Path to file list (default: file_list.txt)"
            echo "  --dry-run           Preview submissions without actually submitting"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                              # Submit all with defaults"
            echo "  $0 --chunk-size 500             # Smaller chunks"
            echo "  $0 --dry-run                    # Preview what would be submitted"
            echo "  $0 --file-list other_files.txt  # Use different file list"
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  Chunk-based HPC Job Submission${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Validate prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

if [ ! -f "$FILE_LIST" ]; then
    echo -e "${RED}Error: File list not found: $FILE_LIST${NC}"
    exit 1
fi
echo -e "  ${GREEN}✓${NC} File list: $FILE_LIST"

if [ ! -f "$JOB_SCRIPT" ]; then
    echo -e "${RED}Error: Job script not found: $JOB_SCRIPT${NC}"
    exit 1
fi
echo -e "  ${GREEN}✓${NC} Job script: $JOB_SCRIPT"

if ! command -v sbatch >/dev/null 2>&1; then
    echo -e "${RED}Error: sbatch not found. Are you on the HPC cluster?${NC}"
    exit 1
fi
echo -e "  ${GREEN}✓${NC} sbatch available"

# Create logs directory if it doesn't exist
mkdir -p logs
echo -e "  ${GREEN}✓${NC} Logs directory ready"

echo ""

# Count files (excluding empty lines)
NUM_FILES=$(grep -c -v '^[[:space:]]*$' "$FILE_LIST" 2>/dev/null || wc -l < "$FILE_LIST")
NUM_FILES=$(echo "$NUM_FILES" | tr -d '[:space:]')

if [ "$NUM_FILES" -eq 0 ]; then
    echo -e "${RED}Error: File list is empty${NC}"
    exit 1
fi

# Calculate number of chunks
NUM_CHUNKS=$(( (NUM_FILES + CHUNK_SIZE - 1) / CHUNK_SIZE ))

echo -e "${YELLOW}Configuration:${NC}"
echo "  Total files:      $NUM_FILES"
echo "  Chunk size:       $CHUNK_SIZE"
echo "  Max concurrent:   $MAX_CONCURRENT"
echo "  Number of chunks: $NUM_CHUNKS"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}DRY RUN MODE - No jobs will be submitted${NC}"
    echo ""
fi

# Submit chunks
echo -e "${YELLOW}Submitting chunks...${NC}"
echo ""

JOB_IDS=()
CHUNK_NUM=0

for ((start=1; start<=NUM_FILES; start+=CHUNK_SIZE)); do
    CHUNK_NUM=$((CHUNK_NUM + 1))
    end=$((start + CHUNK_SIZE - 1))
    
    # Don't exceed total file count
    if [ $end -gt $NUM_FILES ]; then
        end=$NUM_FILES
    fi
    
    # Calculate array size for this chunk (always start from 1 to respect MaxArraySize)
    ARRAY_SIZE=$((end - start + 1))
    ARRAY_SPEC="1-${ARRAY_SIZE}%${MAX_CONCURRENT}"
    FILE_OFFSET=$((start - 1))
    
    echo -e "  Chunk ${CHUNK_NUM}/${NUM_CHUNKS}: files ${start}-${end} (array=${ARRAY_SPEC}, offset=${FILE_OFFSET})"
    
    if [ "$DRY_RUN" = true ]; then
        echo -e "    ${YELLOW}[DRY RUN] Would run: sbatch --array=${ARRAY_SPEC} --export=ALL,FILE_OFFSET=${FILE_OFFSET} ${JOB_SCRIPT}${NC}"
    else
        # Submit the job with FILE_OFFSET environment variable (|| true prevents set -e from exiting)
        OUTPUT=$(sbatch --array="${ARRAY_SPEC}" --export=ALL,FILE_OFFSET=${FILE_OFFSET} "$JOB_SCRIPT" 2>&1) || true
        
        # Check if submission succeeded by looking for job ID in output
        if echo "$OUTPUT" | grep -q "Submitted batch job"; then
            JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | tail -1)
            JOB_IDS+=("$JOB_ID")
            echo -e "    ${GREEN}✓ Submitted: Job ID ${JOB_ID}${NC}"
        else
            echo -e "    ${RED}✗ Failed to submit: ${OUTPUT}${NC}"
        fi
        
        # Small delay between submissions to avoid rate limiting
        sleep 1
    fi
done

echo ""
echo -e "${BLUE}============================================${NC}"

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}DRY RUN COMPLETE${NC}"
    echo ""
    echo "To actually submit, run without --dry-run:"
    echo "  $0"
else
    echo -e "${GREEN}SUBMISSION COMPLETE${NC}"
    echo ""
    echo "Submitted ${#JOB_IDS[@]} chunk(s) covering $NUM_FILES files"
    echo ""
    echo "Job IDs: ${JOB_IDS[*]}"
    echo ""
    echo "Monitor progress with:"
    echo "  squeue -u \$USER"
    echo ""
    echo "View logs:"
    echo "  tail -f logs/bioc_*.out"
    echo ""
    echo "Check for errors:"
    echo "  grep -l 'Error' logs/bioc_*.err"
fi

echo -e "${BLUE}============================================${NC}"

