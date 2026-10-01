#!/usr/bin/env bash
# Run the export workers planned by plan_workers.py, N at a time.
#
# Usage:
#   export AZURE_STORAGE_CONNECTION_STRING="..." AZURE_TABLE_NAME=audit S3_BUCKET=... GZIP=true
#   MAX_PARALLEL=10 nohup ./run_workers.sh > run.log 2>&1 &
#
# Safe to re-run: finished workers are skipped, interrupted or failed ones
# resume from their checkpoint. Workers run in random order so concurrent
# workers are spread across different partitions.
# Each worker logs to <workers_dir>/<name>.log.
set -euo pipefail

DIR="${1:-workers}"
export MAX_PARALLEL="${MAX_PARALLEL:-10}"
export EXPORT_SCRIPT="${EXPORT_SCRIPT:-$(cd "$(dirname "$0")" && pwd)/azure_table_to_s3.py}"

: "${AZURE_TABLE_NAME:?export AZURE_TABLE_NAME first}"
: "${S3_BUCKET:?export S3_BUCKET first}"
if [[ -z "${AZURE_STORAGE_CONNECTION_STRING:-}" && -z "${AZURE_STORAGE_ACCOUNT_KEY:-}" ]]; then
  echo "export AZURE_STORAGE_CONNECTION_STRING first" >&2; exit 1
fi

total=$(find "$DIR" -maxdepth 1 -name '*.env' | wc -l)
echo "$(date '+%F %T') Running $total workers from $DIR, $MAX_PARALLEL at a time"

find "$DIR" -maxdepth 1 -name '*.env' | shuf | xargs -P "$MAX_PARALLEL" -I{} bash -c '
  f="$1"; name="$(basename "$f" .env)"; logf="${f%.env}.log"
  source "$f"
  if grep -qs "\"done\": true" "$WORK_DIR/$AZURE_TABLE_NAME.checkpoint.json"; then
    echo "$(date "+%F %T") skip   $name (already done)"; exit 0
  fi
  echo "$(date "+%F %T") start  $name"
  if python3 "$EXPORT_SCRIPT" >> "$logf" 2>&1; then
    echo "$(date "+%F %T") done   $name"
  else
    echo "$(date "+%F %T") FAILED $name  (see $logf)"
  fi
' _ {}

echo "$(date '+%F %T') Pass finished. Re-run this script to retry anything that FAILED."
