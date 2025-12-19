#!/usr/bin/env bash
# Helper to submit a SLURM array for all files listed in file_list.txt
# Usage: ./submit_bioc_array_auto.sh [CONCURRENCY] [PARTITION] [--dry-run] [--auto-concurrency]
# CONCURRENCY defaults to 100 (max concurrent tasks)
# PARTITION defaults to 'cpu'. If --auto-concurrency is provided the script will
# query the scheduler for available CPUs in the partition and compute a
# recommended concurrency = floor(free_cpus / cpus_per_task).

set -euo pipefail

FILE_LIST="file_list.txt"
SCRIPT="submit_bioc_array.sh"
DEFAULT_CONCURRENCY=100
CONCURRENCY=""
PARTITION="cpu"
DRY_RUN=0
AUTO_CONCURRENCY=0
CHUNK_SIZE=${CHUNK_SIZE:-10000}
SAFETY_FACTOR=0.8

# Parse args (supports flags in any order):
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --auto-concurrency) AUTO_CONCURRENCY=1; shift ;;
    --concurrency|-c) CONCURRENCY="$2"; shift 2 ;;
    --partition|-p) PARTITION="$2"; shift 2 ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --safety-factor) SAFETY_FACTOR="$2"; shift 2 ;;
    --help) echo "Usage: $0 [--concurrency N] [--partition P] [--chunk-size N] [--auto-concurrency] [--dry-run]"; exit 0 ;;
    *)
      # positional first arg as legacy CONCURRENCY
      if [ -z "$CONCURRENCY" ]; then
        CONCURRENCY="$1"
        shift
      else
        echo "Unknown arg: $1"; exit 1
      fi
      ;;
  esac
done

# If concurrency still empty, set default
if [ -z "$CONCURRENCY" ]; then
  CONCURRENCY=$DEFAULT_CONCURRENCY
fi

if [ ! -f "$FILE_LIST" ]; then
  echo "Error: $FILE_LIST not found"
  exit 1
fi

# Count non-empty, non-comment lines
TOTAL=$(grep -v -E '^\s*(#|$)' "$FILE_LIST" | wc -l | tr -d '[:space:]')

if [ "$TOTAL" -eq 0 ]; then
  echo "No files to submit (found 0 non-empty lines)"
  exit 0
fi

# Sanity cap: if user supplies non-numeric concurrency, fall back
if ! [[ "$CONCURRENCY" =~ ^[0-9]+$ ]]; then
  echo "Warning: invalid concurrency '$CONCURRENCY', using default $DEFAULT_CONCURRENCY"
  CONCURRENCY=$DEFAULT_CONCURRENCY
fi

# Auto-concurrency: query scheduler to compute free CPUs in the partition
if [ "$AUTO_CONCURRENCY" -eq 1 ]; then
  # detect cpus-per-task from the submission script (fallback to 4)
  detected=$(grep -oE '--cpus-per-task=[0-9]+' "$SCRIPT" 2>/dev/null || true)
  if [ -n "$detected" ]; then
    cpus_per_task=${detected#*=}
  else
    cpus_per_task=4
  fi

  # Sum free CPUs (CPUTot - CPUAlloc) for nodes in the chosen partition and not DOWN
  total_free=0
  while read -r tot alloc; do
    # guard against empty lines
    if [ -z "$tot" ]; then
      continue
    fi
    free=$(( tot - alloc ))
    if [ $free -gt 0 ]; then
      total_free=$(( total_free + free ))
    fi
  done < <(scontrol show nodes | awk -v part="$PARTITION" '
    BEGIN{RS="\n\n"}
    {
      cputot=0; cpualloc=0; parts=""; state="";
      if (match($0, /CPUTot=[0-9]+/)) { m=substr($0, RSTART, RLENGTH); split(m,a,"="); cputot=a[2]; }
      if (match($0, /CPUAlloc=[0-9]+/)) { m=substr($0, RSTART, RLENGTH); split(m,a,"="); cpualloc=a[2]; }
      if (match($0, /Partitions=[^ \n]+/)) { m=substr($0, RSTART, RLENGTH); split(m,a,"="); parts=a[2]; }
      if (match($0, /State=[^ \n]+/)) { m=substr($0, RSTART, RLENGTH); split(m,a,"="); state=a[2]; }
      if (index(parts, part) && state != "DOWN") { print cputot, cpualloc }
    }')

  if [ "$total_free" -le 0 ]; then
    echo "Warning: no free CPUs detected in partition '$PARTITION' (total_free=$total_free). Using default concurrency $CONCURRENCY"
  else
    recommended=$(( total_free / cpus_per_task ))
    if [ "$recommended" -lt 1 ]; then
      recommended=1
    fi
    # apply safety factor (float mul, round down)
    safe=$(awk -v r="$recommended" -v f="$SAFETY_FACTOR" 'BEGIN{printf "%d", int(r * f)}')
    if [ "$safe" -lt 1 ]; then
      safe=1
    fi
    echo "Auto-concurrency: free_cpus=$total_free, cpus_per_task=$cpus_per_task => raw=$recommended, safety_factor=$SAFETY_FACTOR => using concurrency=$safe"
    CONCURRENCY=$safe
  fi
fi

echo "Total tasks: $TOTAL; concurrency: $CONCURRENCY; chunk_size: $CHUNK_SIZE"

# ensure logs dir exists for submit ids
mkdir -p logs
SUBMIT_IDS_FILE="logs/submit_ids.txt"
: > "$SUBMIT_IDS_FILE" || true

# Adaptive submit_range: on 'Invalid job array specification' recursively split range
submit_range() {
  local start=$1
  local end=$2
  local array="${start}-${end}%${CONCURRENCY}"
  echo "Attempting chunk: sbatch --partition=${PARTITION} --array=${array} ${SCRIPT}"
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "Dry-run: not submitting"
    return 0
  fi

  out=$(sbatch --partition="${PARTITION}" --array="${array}" "$SCRIPT" 2>&1) || rc=$?
  rc=${rc:-0}
  if [ $rc -eq 0 ]; then
    echo "$out"
    # record job id line if it contains 'Submitted batch job'
    if echo "$out" | grep -q "Submitted batch job"; then
      echo "$out" >> "$SUBMIT_IDS_FILE"
    fi
    return 0
  fi

  if echo "$out" | grep -qi "Invalid job array specification"; then
    # If single index fails, give up
    if [ "$start" -eq "$end" ]; then
      echo "Scheduler rejected single-index array for $start: $out"
      return 2
    fi
    mid=$(( (start + end) / 2 ))
    echo "Array spec rejected for $start-$end; splitting into $start-$mid and $((mid+1))-$end"
    submit_range "$start" "$mid" || return $?
    submit_range $((mid + 1)) "$end" || return $?
    return 0
  fi

  echo "sbatch failed: $out"
  return $rc
}

if [ "$TOTAL" -le "$CHUNK_SIZE" ]; then
  submit_range 1 "$TOTAL"
  rc=$?
  if [ $rc -eq 2 ]; then
    echo "Scheduler rejected the array specification. Try lowering CHUNK_SIZE (current $CHUNK_SIZE)."
    exit 1
  fi
  exit $rc
fi

# Submit multiple top-level chunks with adaptive splitting
i=1
while [ $i -le $TOTAL ]; do
  j=$(( i + CHUNK_SIZE - 1 ))
  if [ $j -gt $TOTAL ]; then
    j=$TOTAL
  fi
  submit_range $i $j
  rc=$?
  if [ $rc -eq 2 ]; then
    echo "Scheduler rejected chunk array specification for $i-$j. Try lowering CHUNK_SIZE (current $CHUNK_SIZE)."
    exit 1
  elif [ $rc -ne 0 ]; then
    echo "sbatch returned exit code $rc for chunk $i-$j"
    exit $rc
  fi
  i=$(( j + 1 ))
done

echo "All chunks submitted (or dry-run completed). Job IDs (one line per sbatch response) saved to $SUBMIT_IDS_FILE"

exit 0
