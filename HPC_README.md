# HPC Setup for BioC Processing and Odinson Indexing

This guide covers the complete workflow for processing BioC XML files and creating Odinson indexes on HPC clusters using Singularity containers.

## Workflow Overview

```
Stage 1: Document Processing          Stage 2: Indexing
┌─────────────────────────┐          ┌─────────────────────────┐
│  BioC XML Files         │          │  Odinson JSON Files     │
│  (*.BioC.XML)           │          │  (*.json)               │
│           │             │          │           │             │
│           ▼             │          │           ▼             │
│  bioc_processor.sif     │   ───►   │  odinson_indexer.sif    │
│  (Python + spaCy)       │          │  (JVM + Scala)          │
│           │             │          │           │             │
│           ▼             │          │           ▼             │
│  Odinson JSON Files     │          │  Lucene Index           │
└─────────────────────────┘          └─────────────────────────┘
```

---

## Stage 1: BioC Document Processing

## Quick Start

### 1. Build the Singularity Container

```bash
# On a build node (with sudo) or use --fakeroot
cd /path/to/bio-processor
sudo singularity build bioc_processor.sif bioc_processor.def
```

### 2. Create File List

```bash
# List all BioC files you want to process
find /path/to/bioc/files -name "*.BioC.XML" > file_list.txt

# Or manually create file_list.txt:
# /data/bioc/file1.BioC.XML
# /data/bioc/file2.BioC.XML
# ...
```

### 3. Update Job Script

Edit `submit_bioc_array.sh`:
- Set `FILE_LIST` to your file list path
- Set `OUTPUT_BASE` to your output directory
- Set `CONTAINER` to your container path
- Adjust `--bind` paths for your HPC

### 4. Submit Job

For large file lists (1000+ files), use the chunk-based submission script:

```bash
# Recommended: Auto-detects file count and submits in chunks
./submit_chunks.sh

# Preview what will be submitted without actually submitting
./submit_chunks.sh --dry-run

# Custom chunk size (default: 1000)
./submit_chunks.sh --chunk-size 500
```

For smaller file lists, you can submit manually:

```bash
# Create logs directory
mkdir -p logs

# Count files and submit with correct range
NUM_FILES=$(wc -l < file_list.txt)
sbatch --array=1-${NUM_FILES}%20 submit_bioc_array.sh
```

### 5. Monitor Progress

```bash
# Check job status
squeue -u $USER

# View output logs
tail -f logs/bioc_*.out

# Check for failures
grep -l "Error" logs/bioc_*.err
```

## Container Usage

### Single File Processing

```bash
singularity run \
    --bind /data:/data \
    bioc_processor.sif \
    /data/input.BioC.XML \
    /data/output/
```

### With Options

```bash
# Reprocess everything (ignore existing files)
singularity run bioc_processor.sif input.xml output/ --no-resume

# Create single combined output instead of per-section
singularity run bioc_processor.sif input.xml output/ --combined
```

### Interactive Shell

```bash
singularity shell --bind /data:/data bioc_processor.sif
# Then inside container:
python /opt/process_bioc.py --help
```

## Resume Capability

The processor automatically resumes from where it left off:

1. If a job times out or fails, just resubmit it
2. Already-processed documents are skipped
3. No duplicate work!

```bash
# Resubmit specific failed tasks
sbatch --array=5,12,47 submit_bioc_array.sh
```

## Chunk-based Submission for Large File Lists

For file lists with thousands of files, use `submit_chunks.sh` to automatically submit jobs in manageable chunks:

```bash
# Basic usage - auto-detects file count
./submit_chunks.sh

# Options
./submit_chunks.sh --chunk-size 500      # Files per chunk (default: 1000)
./submit_chunks.sh --max-concurrent 10   # Max parallel jobs (default: 20)
./submit_chunks.sh --file-list other.txt # Use different file list
./submit_chunks.sh --dry-run             # Preview without submitting
```

Benefits of chunk-based submission:
- No need to manually count files or edit array ranges
- Better progress tracking (completed chunks vs pending)
- Easier to identify and resubmit failed chunks
- Smaller jobs may start sooner on busy clusters

Example output:
```
  Chunk 1/12: files 1-1000 (1-1000%20)
    ✓ Submitted: Job ID 12345
  Chunk 2/12: files 1001-2000 (1001-2000%20)
    ✓ Submitted: Job ID 12346
  ...
```

## Output Structure

```
/scratch/user/bioc_output/
├── file1/
│   ├── 12345_title.json
│   ├── 12345_abstract.json
│   ├── 12345_intro.json
│   └── ...
├── file2/
│   └── ...
```

## Resource Recommendations

| File Size | Memory | CPUs | Time |
|-----------|--------|------|------|
| Small (<100 docs) | 8G | 2 | 1h |
| Medium (100-500 docs) | 16G | 4 | 4h |
| Large (>500 docs) | 32G | 4 | 8h |

## Troubleshooting

### Out of Memory
```bash
#SBATCH --mem=32G  # Increase memory
```

### Job Timeout
```bash
#SBATCH --time=08:00:00  # Increase time limit
# Resume will pick up where it left off
```

### Missing spaCy Model
The container includes `en_core_sci_lg`. If you need a different model, rebuild the container or add:
```bash
singularity exec bioc_processor.sif python -m spacy download <model_name>
```

---

## Stage 2: Odinson Indexing

After processing BioC files into Odinson JSON format, you can create a Lucene index for fast querying.

### Build the Indexer Container

```bash
# On a build node (with sudo) or use --fakeroot
cd /path/to/bio-processor
sudo singularity build odinson_indexer.sif odinson_indexer.def
```

**Note:** Building this container takes longer than `bioc_processor.sif` because it compiles the Odinson Scala project. Plan for 10-15 minutes.

### Run Indexing Manually

```bash
# Basic usage with bind mounts
singularity run \
    --bind /shared/bfr027/bioc_output:/data/odinson/docs \
    --bind /shared/bfr027/odinson_index:/data/odinson/index \
    odinson_indexer.sif

# Specify directories explicitly
singularity run \
    --bind /shared/$USER:/shared/$USER \
    odinson_indexer.sif \
    --docs-dir /shared/$USER/bioc_output \
    --index-dir /shared/$USER/odinson_index

# Use a single data directory (expects docs/ and index/ subdirectories)
singularity run \
    --bind /shared/$USER/odinson_data:/data/odinson \
    odinson_indexer.sif
```

### Submit Indexing Job

Create a job script `submit_odinson_index.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=odinson_index
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=08:00:00
#SBATCH --output=logs/odinson_index_%j.out
#SBATCH --error=logs/odinson_index_%j.err

set -euo pipefail

# Configuration - MODIFY THESE PATHS
SHARED_ROOT="/shared/bfr027"
DOCS_DIR="${SHARED_ROOT}/bioc_output"      # Where JSON files are stored
INDEX_DIR="${SHARED_ROOT}/odinson_index"   # Where to create the index
CONTAINER="${SHARED_ROOT}/odinson_indexer.sif"

# Create output directory
mkdir -p "$INDEX_DIR"
mkdir -p logs

echo "============================================"
echo "Odinson Indexing Job"
echo "Documents: $DOCS_DIR"
echo "Index Output: $INDEX_DIR"
echo "Start Time: $(date)"
echo "============================================"

# Run the indexer
singularity run \
    --bind /shared/$USER:/shared/$USER \
    "$CONTAINER" \
    --docs-dir "$DOCS_DIR" \
    --index-dir "$INDEX_DIR"

echo "============================================"
echo "End Time: $(date)"
echo "============================================"
```

Submit with:
```bash
sbatch submit_odinson_index.sh
```

### Indexing Resource Recommendations

| Document Count | Memory | CPUs | Time |
|----------------|--------|------|------|
| < 10,000 docs  | 16G    | 4    | 1h   |
| 10,000-50,000  | 32G    | 4    | 4h   |
| > 50,000 docs  | 64G    | 4    | 8h+  |

For very large datasets, increase Java heap:
```bash
SINGULARITYENV_JAVA_OPTS="-Xmx48G" singularity run ...
```

### Indexing Output Structure

```
/shared/user/odinson_index/
├── segments_1
├── write.lock
├── _0.cfe
├── _0.cfs
├── _0.si
└── ...
```

### Verify the Index

After indexing, you can verify the index was created successfully:
```bash
ls -la /shared/$USER/odinson_index/
# Should see segments_* and other Lucene index files
```

---

## Complete Workflow Example

```bash
# 1. Build both containers
sudo singularity build bioc_processor.sif bioc_processor.def
sudo singularity build odinson_indexer.sif odinson_indexer.def

# 2. Create file list for BioC processing
find /data/pubmed -name "*.BioC.XML" > file_list.txt

# 3. Submit document processing jobs
./submit_chunks.sh

# 4. Wait for all processing jobs to complete
squeue -u $USER  # Monitor until all jobs finish

# 5. Submit indexing job
sbatch submit_odinson_index.sh

# 6. Use the index for querying (requires Odinson extractor setup)
```

---

## Troubleshooting Indexing

### Out of Memory During Indexing
```bash
#SBATCH --mem=64G  # Increase memory
# Also increase Java heap:
SINGULARITYENV_JAVA_OPTS="-Xmx48G" singularity run ...
```

### Index Build Fails Partway Through
The indexer does not have automatic resume. You may need to:
1. Delete the partial index: `rm -rf /path/to/odinson_index/*`
2. Re-run the indexing job

### Missing or Malformed JSON Documents
Check the indexer logs for errors:
```bash
grep -i "error\|failed\|exception" logs/odinson_index_*.out
```

Problematic files will be logged but won't stop the entire indexing process.

