# HPC Setup for BioC Processing

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
- Adjust `--array=1-N` where N is number of files
- Adjust `--bind` paths for your HPC

### 4. Submit Job

```bash
# Create logs directory
mkdir -p logs

# Submit the job array
sbatch submit_bioc_array.sh
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
