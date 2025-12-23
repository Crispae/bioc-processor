#!/bin/bash
#SBATCH --job-name=odinson_parallel_index
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --output=logs/parallel_index_%j.out
#SBATCH --error=logs/parallel_index_%j.err

set -euo pipefail

# =============================================================================
# Odinson Parallel Indexing - Split and Index in Parallel
# =============================================================================
#
# Creates multiple indexes in parallel by splitting documents using various
# strategies. Designed for large collections (14M+ documents).
#
# Usage:
#   sbatch submit_odinson_parallel_index.sh <parent_directory> [strategy] [num_shards] [--dry-run]
#   OR
#   ./submit_odinson_parallel_index.sh <parent_directory> [strategy] [num_shards] [--dry-run]
#
# Strategies:
#   subdir    - Split by subdirectory (one index per subdirectory)
#   count     - Split by file count (equal-sized chunks)
#   hash      - Split by hash of filename (distributes evenly)
#   range     - Split by alphabetical range of filenames
#
# Options:
#   --dry-run  - Preview what would be done without submitting jobs
#   --help     - Show this help message
#
# Examples:
#   # Dry run: Preview split by subdirectory
#   ./submit_odinson_parallel_index.sh /shared/bfr027/bioc_output subdir --dry-run
#
#   # Real run: Split by subdirectory
#   sbatch submit_odinson_parallel_index.sh /shared/bfr027/bioc_output subdir
#
#   # Dry run: Preview split into 20 equal chunks
#   ./submit_odinson_parallel_index.sh /shared/bfr027/bioc_output count 20 --dry-run
#
#   # Real run: Split into 20 equal chunks
#   sbatch submit_odinson_parallel_index.sh /shared/bfr027/bioc_output count 20
#
# =============================================================================

# Configuration
SHARED_ROOT="/shared/bfr027"
INDEX_BASE="${SHARED_ROOT}/odinson_index"
CONTAINER="/shared/$USER/bioc-processor/odinson_indexer.sif"
STAGING_BASE="${SHARED_ROOT}/odinson_staging"
MAX_PARALLEL_JOBS=50  # Max concurrent indexing jobs

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Parse arguments
DRY_RUN=false
DOCS_DIR=""
STRATEGY="subdir"
NUM_SHARDS=""
SHOW_HELP=false

# Parse positional and optional arguments
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            SHOW_HELP=true
            shift
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done

# Restore positional parameters
set -- "${POSITIONAL_ARGS[@]}"

# Show help if requested
if [ "$SHOW_HELP" = true ]; then
    cat << EOF
Odinson Parallel Indexing Script

Usage:
    sbatch submit_odinson_parallel_index.sh <parent_directory> [strategy] [num_shards] [--dry-run]
    OR
    ./submit_odinson_parallel_index.sh <parent_directory> [strategy] [num_shards] [--dry-run]

Arguments:
    parent_directory    Directory containing JSON documents to index (required)
    strategy           Splitting strategy: subdir, count, hash, or range (default: subdir)
    num_shards         Number of shards for count/hash strategies (required for those strategies)

Options:
    --dry-run          Preview what would be done without submitting jobs
    --help, -h         Show this help message

Strategies:
    subdir    Split by subdirectory (one index per subdirectory)
              Best for: Documents organized in folders
              
    count     Split by file count into equal-sized chunks
              Best for: Even distribution needed
              Requires: num_shards parameter
              
    hash      Split by hash of filename (distributes evenly)
              Best for: Random distribution
              Requires: num_shards parameter
              
    range     Split by alphabetical range of filenames
              Best for: Alphabetical organization
              Optional: num_shards (default: 26)

Examples:
    # Dry run: Preview split by subdirectory
    ./submit_odinson_parallel_index.sh /shared/bfr027/bioc_output subdir --dry-run

    # Real run: Split by subdirectory
    sbatch submit_odinson_parallel_index.sh /shared/bfr027/bioc_output subdir

    # Dry run: Preview split into 20 equal chunks
    ./submit_odinson_parallel_index.sh /shared/bfr027/bioc_output count 20 --dry-run

    # Real run: Split into 20 equal chunks
    sbatch submit_odinson_parallel_index.sh /shared/bfr027/bioc_output count 20

    # Dry run: Preview hash-based split into 10 shards
    ./submit_odinson_parallel_index.sh /shared/bfr027/bioc_output hash 10 --dry-run

Output:
    In dry-run mode, shows:
    - Total documents found
    - How documents would be split
    - Number of jobs that would be submitted
    - Estimated resource usage
    
    In real-run mode, submits SLURM jobs and shows job IDs.
EOF
    exit 0
fi

# Get positional arguments
DOCS_DIR="${1:-}"
STRATEGY="${2:-subdir}"
NUM_SHARDS="${3:-}"

# Validate required arguments
if [ -z "$DOCS_DIR" ]; then
    echo -e "${RED}Error: No documents directory specified${NC}"
    echo "Usage: sbatch submit_odinson_parallel_index.sh <parent_directory> [strategy] [num_shards] [--dry-run]"
    echo "Use --help for more information"
    exit 1
fi

if [ ! -d "$DOCS_DIR" ]; then
    echo -e "${RED}Error: Documents directory not found: $DOCS_DIR${NC}"
    exit 1
fi

if [ ! -f "$CONTAINER" ]; then
    echo -e "${RED}Error: Container not found: $CONTAINER${NC}"
    exit 1
fi

# Validate strategy
case "$STRATEGY" in
    subdir|count|hash|range)
        ;;
    *)
        echo -e "${RED}Error: Invalid strategy '$STRATEGY'${NC}"
        echo "Valid strategies: subdir, count, hash, range"
        echo "Use --help for more information"
        exit 1
        ;;
esac

# Validate num_shards for strategies that require it
if [[ "$STRATEGY" == "count" || "$STRATEGY" == "hash" ]] && [ -z "$NUM_SHARDS" ]; then
    echo -e "${RED}Error: Strategy '$STRATEGY' requires num_shards parameter${NC}"
    echo "Example: sbatch submit_odinson_parallel_index.sh $DOCS_DIR $STRATEGY 20"
    exit 1
fi

# Set default num_shards for range strategy
if [ "$STRATEGY" == "range" ] && [ -z "$NUM_SHARDS" ]; then
    NUM_SHARDS=26
fi

mkdir -p logs
mkdir -p "$INDEX_BASE"

# Display header
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Odinson Parallel Indexing${NC}"
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}*** DRY RUN MODE - No jobs will be submitted ***${NC}"
fi
echo -e "${BLUE}============================================${NC}"
echo "Documents Dir: $DOCS_DIR"
echo "Strategy: $STRATEGY"
echo "Num Shards: ${NUM_SHARDS:-auto}"
echo "Container: $CONTAINER"
echo "Mode: $([ "$DRY_RUN" = true ] && echo "DRY RUN" || echo "REAL RUN")"
echo "Start Time: $(date)"
echo -e "${BLUE}============================================${NC}"

# Count total documents
echo -e "${CYAN}Counting documents...${NC}"
TOTAL_DOCS=$(find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" 2>/dev/null | wc -l)
echo "Total documents: $TOTAL_DOCS"

if [ "$TOTAL_DOCS" -eq 0 ]; then
    echo -e "${RED}ERROR: No JSON files found${NC}"
    exit 1
fi

# Create staging directory for this indexing run
RUN_ID="parallel_$(date +%Y%m%d_%H%M%S)"
STAGING_DIR="${STAGING_BASE}/${RUN_ID}"

if [ "$DRY_RUN" = false ]; then
    mkdir -p "$STAGING_DIR"
else
    echo -e "${YELLOW}[DRY RUN] Would create staging directory: $STAGING_DIR${NC}"
fi

# Track job submissions
JOB_IDS=()
TOTAL_SHARDS=0

# Function to create index from a file list
create_index_from_list() {
    local shard_id=$1
    local file_list=$2
    local index_name="${RUN_ID}_shard_${shard_id}"
    local index_dir="${INDEX_BASE}/${index_name}"
    local staging_shard_dir="${STAGING_DIR}/shard_${shard_id}"
    
    if [ "$DRY_RUN" = false ]; then
        mkdir -p "$staging_shard_dir"
        mkdir -p "$index_dir"
        
        # Create symlinks for files in this shard
        echo -e "${CYAN}Creating symlinks for shard $shard_id...${NC}"
        while IFS= read -r file; do
            if [ -n "$file" ] && [ -f "$file" ]; then
                basename_file=$(basename "$file")
                ln -sf "$file" "${staging_shard_dir}/${basename_file}"
            fi
        done < "$file_list"
    else
        echo -e "${YELLOW}[DRY RUN] Would create: $staging_shard_dir${NC}"
    fi
    
    # Count files in this shard
    if [ "$DRY_RUN" = false ]; then
        local shard_count=$(find "$staging_shard_dir" -type l 2>/dev/null | wc -l)
    else
        local shard_count=$(wc -l < "$file_list" 2>/dev/null || echo "0")
    fi
    
    if [ "$shard_count" -eq 0 ]; then
        echo -e "${YELLOW}WARNING: Shard $shard_id has no files, skipping${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Shard $shard_id: $shard_count files${NC}"
    
    # Submit indexing job (or simulate in dry-run)
    local job_name="odinson_shard_${shard_id}"
    local job_output="logs/${job_name}_%j.out"
    local job_error="logs/${job_name}_%j.err"
    
    if [ "$DRY_RUN" = true ]; then
        echo -e "${YELLOW}[DRY RUN] Would submit job:${NC}"
        echo -e "  ${CYAN}Job Name:${NC} $job_name"
        echo -e "  ${CYAN}Resources:${NC} 16 CPUs, 128GB RAM, 72h time limit"
        echo -e "  ${CYAN}Staging Dir:${NC} $staging_shard_dir"
        echo -e "  ${CYAN}Index Dir:${NC} $index_dir"
        echo -e "  ${CYAN}Java Options:${NC} -Xmx96G -XX:+UseG1GC"
        echo ""
        return 0
    else
        # Submit indexing job
        OUTPUT=$(sbatch \
            --job-name="$job_name" \
            --cpus-per-task=16 \
            --mem=128G \
            --time=72:00:00 \
            --output="$job_output" \
            --error="$job_error" \
            --wrap="
                export SINGULARITYENV_JAVA_OPTS=\"-Xmx96G -XX:+UseG1GC\"
                singularity run -c \
                    --bind /shared/\$USER:/shared/\$USER,/home/\$USER:/home/\$USER \
                    --net --network none \
                    $CONTAINER \
                    --docs-dir \"$staging_shard_dir\" \
                    --index-dir \"$index_dir\" 2>&1
            " 2>&1) || true
        
        # Check if submission succeeded
        if echo "$OUTPUT" | grep -q "Submitted batch job"; then
            JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | tail -1)
            JOB_IDS+=("$JOB_ID")
            echo -e "${GREEN}✓ Submitted: Job ID ${JOB_ID}${NC}"
            return 0
        else
            echo -e "${RED}✗ Failed to submit: ${OUTPUT}${NC}"
            return 1
        fi
    fi
}

# Strategy 1: Split by subdirectory
if [ "$STRATEGY" == "subdir" ]; then
    echo ""
    echo -e "${CYAN}Strategy: Split by subdirectory${NC}"
    echo "Finding subdirectories..."
    
    # Get list of subdirectories
    SUBDIRS=$(find "$DOCS_DIR" -mindepth 1 -maxdepth 1 -type d | sort)
    SUBDIR_COUNT=$(echo "$SUBDIRS" | grep -c . || echo "0")
    
    echo "Found $SUBDIR_COUNT subdirectories"
    
    SHARD_ID=0
    while IFS= read -r subdir; do
        if [ -n "$subdir" ]; then
            SHARD_ID=$((SHARD_ID + 1))
            
            # Create file list for this subdirectory
            FILE_LIST="${STAGING_DIR}/shard_${SHARD_ID}.list"
            if [ "$DRY_RUN" = false ]; then
                find "$subdir" -name "*.json" -o -name "*.json.gz" > "$FILE_LIST" 2>/dev/null
            else
                # In dry-run, just count without creating file
                DOC_COUNT=$(find "$subdir" -name "*.json" -o -name "*.json.gz" 2>/dev/null | wc -l)
                echo -e "${YELLOW}[DRY RUN] Would create file list: $FILE_LIST ($DOC_COUNT files)${NC}"
                # Create temp file for counting
                find "$subdir" -name "*.json" -o -name "*.json.gz" > "$FILE_LIST" 2>/dev/null || true
            fi
            
            DOC_COUNT=$(wc -l < "$FILE_LIST" 2>/dev/null || echo "0")
            if [ "$DOC_COUNT" -gt 0 ]; then
                echo -e "${GREEN}Subdirectory $(basename "$subdir"): $DOC_COUNT files -> Shard $SHARD_ID${NC}"
                create_index_from_list "$SHARD_ID" "$FILE_LIST"
                
                # In dry-run, don't limit concurrent operations
                if [ "$DRY_RUN" = false ]; then
                    # Limit concurrent job submissions
                    while [ $(jobs -r | wc -l) -ge $MAX_PARALLEL_JOBS ]; do
                        sleep 1
                    done
                fi
            fi
        fi
    done <<< "$SUBDIRS"
    
    TOTAL_SHARDS=$SHARD_ID

# Strategy 2: Split by file count
elif [ "$STRATEGY" == "count" ]; then
    echo ""
    echo -e "${CYAN}Strategy: Split by file count into $NUM_SHARDS shards${NC}"
    
    # Create master file list
    MASTER_LIST="${STAGING_DIR}/all_files.list"
    if [ "$DRY_RUN" = false ]; then
        find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" | sort > "$MASTER_LIST" 2>/dev/null
    else
        echo -e "${YELLOW}[DRY RUN] Would create master file list: $MASTER_LIST${NC}"
        find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" | sort > "$MASTER_LIST" 2>/dev/null
    fi
    
    FILES_PER_SHARD=$(( (TOTAL_DOCS + NUM_SHARDS - 1) / NUM_SHARDS ))
    echo "Files per shard: ~$FILES_PER_SHARD"
    
    for ((shard=1; shard<=NUM_SHARDS; shard++)); do
        START_LINE=$(( (shard - 1) * FILES_PER_SHARD + 1 ))
        END_LINE=$(( shard * FILES_PER_SHARD ))
        
        FILE_LIST="${STAGING_DIR}/shard_${shard}.list"
        sed -n "${START_LINE},${END_LINE}p" "$MASTER_LIST" > "$FILE_LIST"
        
        SHARD_COUNT=$(wc -l < "$FILE_LIST" 2>/dev/null || echo "0")
        if [ "$SHARD_COUNT" -gt 0 ]; then
            echo -e "${GREEN}Shard $shard: lines $START_LINE-$END_LINE ($SHARD_COUNT files)${NC}"
            create_index_from_list "$shard" "$FILE_LIST"
            
            if [ "$DRY_RUN" = false ]; then
                while [ $(jobs -r | wc -l) -ge $MAX_PARALLEL_JOBS ]; do
                    sleep 1
                done
            fi
        fi
    done
    
    TOTAL_SHARDS=$NUM_SHARDS

# Strategy 3: Split by hash
elif [ "$STRATEGY" == "hash" ]; then
    echo ""
    echo -e "${CYAN}Strategy: Split by hash into $NUM_SHARDS shards${NC}"
    
    # Create master file list
    MASTER_LIST="${STAGING_DIR}/all_files.list"
    if [ "$DRY_RUN" = false ]; then
        find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" | sort > "$MASTER_LIST" 2>/dev/null
    else
        echo -e "${YELLOW}[DRY RUN] Would create master file list: $MASTER_LIST${NC}"
        find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" | sort > "$MASTER_LIST" 2>/dev/null
    fi
    
    # Create file lists for each shard based on hash
    for shard in $(seq 1 $NUM_SHARDS); do
        FILE_LIST="${STAGING_DIR}/shard_${shard}.list"
        > "$FILE_LIST"  # Create empty file
    done
    
    # Distribute files by hash
    echo "Distributing files by hash..."
    while IFS= read -r file; do
        if [ -n "$file" ]; then
            # Hash the filename to determine shard
            HASH=$(echo -n "$(basename "$file")" | md5sum | cut -d' ' -f1)
            # Convert hex to decimal and modulo
            HASH_DEC=$((0x${HASH:0:8}))
            SHARD=$(( (HASH_DEC % NUM_SHARDS) + 1 ))
            echo "$file" >> "${STAGING_DIR}/shard_${SHARD}.list"
        fi
    done < "$MASTER_LIST"
    
    # Submit jobs for each shard
    for shard in $(seq 1 $NUM_SHARDS); do
        FILE_LIST="${STAGING_DIR}/shard_${shard}.list"
        SHARD_COUNT=$(wc -l < "$FILE_LIST" 2>/dev/null || echo "0")
        if [ "$SHARD_COUNT" -gt 0 ]; then
            echo -e "${GREEN}Shard $shard: $SHARD_COUNT files${NC}"
            create_index_from_list "$shard" "$FILE_LIST"
            
            if [ "$DRY_RUN" = false ]; then
                while [ $(jobs -r | wc -l) -ge $MAX_PARALLEL_JOBS ]; do
                    sleep 1
                done
            fi
        fi
    done
    
    TOTAL_SHARDS=$NUM_SHARDS

# Strategy 4: Split by alphabetical range
elif [ "$STRATEGY" == "range" ]; then
    echo ""
    echo -e "${CYAN}Strategy: Split by alphabetical range into $NUM_SHARDS shards${NC}"
    
    # Create master file list sorted alphabetically
    MASTER_LIST="${STAGING_DIR}/all_files.list"
    if [ "$DRY_RUN" = false ]; then
        find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" | sort > "$MASTER_LIST" 2>/dev/null
    else
        echo -e "${YELLOW}[DRY RUN] Would create master file list: $MASTER_LIST${NC}"
        find "$DOCS_DIR" -name "*.json" -o -name "*.json.gz" | sort > "$MASTER_LIST" 2>/dev/null
    fi
    
    FILES_PER_SHARD=$(( (TOTAL_DOCS + NUM_SHARDS - 1) / NUM_SHARDS ))
    echo "Files per shard: ~$FILES_PER_SHARD"
    
    for ((shard=1; shard<=NUM_SHARDS; shard++)); do
        START_LINE=$(( (shard - 1) * FILES_PER_SHARD + 1 ))
        END_LINE=$(( shard * FILES_PER_SHARD ))
        
        FILE_LIST="${STAGING_DIR}/shard_${shard}.list"
        sed -n "${START_LINE},${END_LINE}p" "$MASTER_LIST" > "$FILE_LIST"
        
        SHARD_COUNT=$(wc -l < "$FILE_LIST" 2>/dev/null || echo "0")
        if [ "$SHARD_COUNT" -gt 0 ]; then
            echo -e "${GREEN}Shard $shard: $SHARD_COUNT files (alphabetical range)${NC}"
            create_index_from_list "$shard" "$FILE_LIST"
            
            if [ "$DRY_RUN" = false ]; then
                while [ $(jobs -r | wc -l) -ge $MAX_PARALLEL_JOBS ]; do
                    sleep 1
                done
            fi
        fi
    done
    
    TOTAL_SHARDS=$NUM_SHARDS
fi

# Wait for all job submissions to complete (only in real run)
if [ "$DRY_RUN" = false ]; then
    wait
fi

echo ""
echo -e "${BLUE}============================================${NC}"
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}DRY RUN SUMMARY${NC}"
    echo -e "${YELLOW}============================================${NC}"
    echo "Total documents: $TOTAL_DOCS"
    echo "Total shards that would be created: $TOTAL_SHARDS"
    echo ""
    echo "Estimated resource usage:"
    echo "  Jobs: $TOTAL_SHARDS"
    echo "  CPUs per job: 16"
    echo "  Memory per job: 128GB"
    echo "  Time limit per job: 72 hours"
    echo "  Total CPUs: $((TOTAL_SHARDS * 16))"
    echo "  Total Memory: $((TOTAL_SHARDS * 128))GB"
    echo ""
    echo "To actually submit jobs, run without --dry-run:"
    echo "  sbatch submit_odinson_parallel_index.sh $DOCS_DIR $STRATEGY ${NUM_SHARDS:-}"
else
    echo -e "${GREEN}SUBMISSION COMPLETE${NC}"
    echo -e "${GREEN}============================================${NC}"
    echo "Total shards: $TOTAL_SHARDS"
    echo "Jobs submitted: ${#JOB_IDS[@]}"
    echo "Staging directory: $STAGING_DIR"
    echo "Index base: $INDEX_BASE"
    echo ""
    if [ ${#JOB_IDS[@]} -gt 0 ]; then
        echo "Job IDs: ${JOB_IDS[*]}"
        echo ""
    fi
    echo "Monitor jobs with:"
    echo "  squeue -u \$USER | grep odinson_shard"
    echo ""
    echo "Check progress:"
    echo "  ls -lh $INDEX_BASE/${RUN_ID}_shard_*/"
fi

echo ""
echo "After all jobs complete, indexes will be at:"
echo "  $INDEX_BASE/${RUN_ID}_shard_*/"
echo ""
echo "To merge indexes later (if needed), use Lucene's IndexWriter.addIndexes()"
echo -e "${BLUE}============================================${NC}"

# Save metadata about this run (only in real run)
if [ "$DRY_RUN" = false ]; then
    METADATA_FILE="${INDEX_BASE}/${RUN_ID}_metadata.txt"
    cat > "$METADATA_FILE" <<EOF
Parallel Indexing Run Metadata
===============================
Run ID: $RUN_ID
Date: $(date)
Strategy: $STRATEGY
Total Documents: $TOTAL_DOCS
Total Shards: $TOTAL_SHARDS
Source Directory: $DOCS_DIR
Staging Directory: $STAGING_DIR
Index Base: $INDEX_BASE
Job IDs: ${JOB_IDS[*]}
EOF

    echo "Metadata saved to: $METADATA_FILE"
fi

