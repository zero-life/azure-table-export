# Azure Table Export

[![CI](https://github.com/zero-life/azure-table-export/actions/workflows/ci.yml/badge.svg)](https://github.com/zero-life/azure-table-export/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Parallel, resumable export of a very large Azure Table Storage table to CSV (or gzipped CSV) files in Amazon S3.

I built this to export a multi-terabyte table that the usual tools (Azure Storage Explorer, AzCopy, Azure Data Factory) could not handle. It exports every row. The work is split across hundreds of independent workers, and you can stop and resume it at any time without losing more than one chunk per worker.

## Features

- **Resumable:** each worker checkpoints its Azure continuation token after every uploaded chunk. If you kill it, it picks up where it left off.
- **Constant memory:** server-side paging means only one page (up to 1,000 rows) is in memory at a time.
- **Low disk use:** each chunk is uploaded to S3 and then deleted locally.
- **Parallel:** split the table into non-overlapping key ranges and run many workers at once.
- **Handles schemaless tables:** properties not in a worker's CSV header are kept as JSON in an `_extra` column instead of being dropped.
- **Retries:** transient Azure and S3 errors are retried with exponential backoff.
- **Least privilege:** a read-only SAS connection string is enough.

## Files

| File | Purpose | Reusable as-is? |
|---|---|---|
| [`azure_table_to_s3.py`](azure_table_to_s3.py) | Exports one table, or one filtered range of it, to CSV chunks in S3. | ✅ Works with any Azure table |
| [`run_workers.sh`](run_workers.sh) | Runs a directory of worker configs in parallel with a concurrency cap. Skips finished workers and resumes failed ones. | ✅ Works with any worker configs |
| [`plan_workers.py`](plan_workers.py) | Samples the table and writes one `.env` config per worker, covering the whole key space with no gaps or overlaps. | ⚠️ Assumes a specific schema (see below) |

## Quick start: a single export

For small and medium tables, one worker is enough:

Requires Python 3.9+ (see [Requirements](#requirements)).

```bash
pip install azure-data-tables boto3

export AZURE_STORAGE_CONNECTION_STRING="TableEndpoint=https://<account>.table.core.windows.net/;SharedAccessSignature=..."
export AZURE_TABLE_NAME=mytable
export S3_BUCKET=my-bucket
export S3_PREFIX=exports/mytable     # optional
export GZIP=true                     # optional

python3 azure_table_to_s3.py
```

If you interrupt it, run the same command again and it resumes from `export_work/<table>.checkpoint.json`.

## Large tables: parallel workers

Each worker is just `azure_table_to_s3.py` with its own `AZURE_TABLE_FILTER`, `S3_PREFIX` and `WORK_DIR`. Put one `.env` file per worker in a directory and `run_workers.sh` runs them for you.

### Option A: write your own worker configs

If you know how your keys are distributed, write the configs yourself. The filters must not overlap:

```bash
# workers/a-to-m.env
export AZURE_TABLE_FILTER="PartitionKey ge 'a' and PartitionKey lt 'n'"
export S3_PREFIX=exports/mytable/a-to-m
export WORK_DIR=./export_work/a-to-m
```

```bash
# workers/n-onward.env
export AZURE_TABLE_FILTER="PartitionKey ge 'n'"
export S3_PREFIX=exports/mytable/n-onward
export WORK_DIR=./export_work/n-onward
```

Filters on `PartitionKey` and `RowKey` are indexed and efficient. Filters on other properties cause full table scans.

### Option B: use the planner

`plan_workers.py` was written for an audit-log table with this layout:

- Each record is written to several partitions: an hourly partition named `audit_YYYYMMDD_HH`, plus partitions by service, level, user, tenant, and so on.
- Each record has an `AllPartitions` column that lists all of its partitions, separated by `~`.
- Each `RowKey` starts with a reverse .NET ticks timestamp (`DateTime.MaxValue.Ticks - timestamp.Ticks`), so newer rows sort first.

If your table looks like this, the planner can split it with no prior knowledge of the partitions. It samples rows from the hourly partitions to estimate the size of every other partition, then creates:

- **Hourly partitions:** one worker per month (indexed `PartitionKey` range).
- **Large partitions:** split by year, or by month for the largest (indexed `RowKey` range).
- **Everything else:** about 400 contiguous `PartitionKey` ranges with roughly equal numbers of rows.

The ranges run end to end over the whole key space, so partitions that the sample missed are still exported.

If your schema is different, use the planner as a starting point. The constants and the `sample_at` / `build_plan` functions are where the schema assumptions live. Pull requests that make it more general are welcome.

```bash
export AZURE_STORAGE_CONNECTION_STRING="..."
export AZURE_TABLE_NAME=audit
export S3_BUCKET=my-bucket
export S3_BASE_PREFIX=exports/audit
export GZIP=true

python3 plan_workers.py                                   # writes workers/*.env and workers/manifest.csv
MAX_PARALLEL=20 nohup bash run_workers.sh > run.log 2>&1 &
```

Do not re-run `plan_workers.py` during an export. The files in `workers/` tie each worker to its checkpoint.

### Monitoring

```bash
tail -f run.log                                   # start/done/FAILED for each worker
echo "done $(grep -c ' done ' run.log)"           # number of finished workers
grep -h "Azure error" workers/*.log | wc -l       # retries/throttling (should stay near 0)
```

### Stopping and resuming

```bash
pkill -f run_workers.sh; pkill -f azure_table_to_s3.py
# ...later, with the same environment variables set:
MAX_PARALLEL=20 nohup bash run_workers.sh > run.log 2>&1 &
```

Each worker loses at most the chunk it was building. Keep `workers/` and `export_work/` on persistent disk.

## Requirements

- **Python 3.9 or newer** (`python3 --version` to check)
- **Python packages:** `azure-data-tables` and `boto3`. Install them with:

  ```bash
  pip install azure-data-tables boto3
  ```

  or, from a clone of this repo, `pip install -r requirements.txt`. Using a virtualenv (`python3 -m venv .venv && source .venv/bin/activate`) keeps them separate from your system Python.
- `run_workers.sh` needs bash and GNU `shuf` (on macOS: `brew install coreutils`)
- AWS credentials with `s3:PutObject` on the target bucket and prefix, from the standard boto3 chain (instance role, `AWS_PROFILE`, or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`)
- Azure credentials, either:
  - `AZURE_STORAGE_CONNECTION_STRING` (recommended: a SAS with service **Table**, resource types **Container + Object**, permissions **Read + List** only), or
  - `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY`

SAS tips:
- Set the **end date** past the expected runtime. The portal defaults to 8 hours.
- If you set **Allowed IP addresses**, use a fixed public IP (for example an AWS Elastic IP) so the SAS still works after the instance stops and starts.

## Configuration

All configuration is through environment variables.

**Exporter (`azure_table_to_s3.py`)**

| Variable | Default | Description |
|---|---|---|
| `AZURE_TABLE_NAME` | *required* | Table to export |
| `S3_BUCKET` | *required* | Destination bucket |
| `S3_PREFIX` | `azure-table-export/<table>` | Destination key prefix |
| `AZURE_TABLE_FILTER` | none | OData filter, e.g. `PartitionKey ge 'A' and PartitionKey lt 'M'` |
| `CHUNK_ROWS` | `1000000` | Rows per CSV file |
| `PAGE_SIZE` | `1000` | Rows per request (Azure max is 1000) |
| `GZIP` | `false` | `true` to write `.csv.gz` |
| `WORK_DIR` | `./export_work` | Local scratch directory |
| `CHECKPOINT_FILE` | `<WORK_DIR>/<table>.checkpoint.json` | Checkpoint location |

**Planner (`plan_workers.py`)**

| Variable | Default | Description |
|---|---|---|
| `S3_BASE_PREFIX` | `azure-table-export/<table>` | Root prefix for all workers |
| `START_YEAR` | `2016` | First time range (older rows go into a `pre-<year>` range) |
| `GROUPS` | `400` | Number of range groups for ordinary partitions |
| `SAMPLE_HOURS` | `6` | Random hours sampled per month |
| `SAMPLE_ROWS` | `500` | Rows read per sampled hour |
| `THREADS` | `16` | Parallel planning queries |
| `SEED` | `42` | Random seed for sampling |
| `OUTPUT_DIR` | `./workers` | Where worker configs are written |
| `WORK_BASE` | `./export_work` | Local scratch root, with one subfolder per worker |

**Runner (`run_workers.sh`)**: `MAX_PARALLEL` (default 10). The first argument is the workers directory (default `workers`).

## Output

Each worker writes `<S3_PREFIX>/<table>_partNNNNNN.csv[.gz]` and, when it finishes, `<S3_PREFIX>/_export_manifest.json` with its row and chunk counts. With the planner, the layout is:

```
s3://<bucket>/<prefix>/audit-hourly/<YYYY-MM>/
s3://<bucket>/<prefix>/dense/<dNNNN-partition>/<YYYY or YYYY-MM>/
s3://<bucket>/<prefix>/groups/<gNNNNN>/
```

Each file starts with a header row: `PartitionKey`, `RowKey`, `Timestamp`, the entity's properties (sorted), and `_extra` (JSON for any property that isn't in that worker's header). Headers can differ between workers.

With the planner's table layout, a record appears once for each partition it was written to. Deduplicate on `RowKey`, which is the same in every copy, to get one row per record.

## Troubleshooting

| Error in worker logs | Cause |
|---|---|
| `AuthorizationFailure` | The request IP isn't allowed by the SAS or the storage account firewall (for example, the instance IP changed) |
| `AuthenticationFailed` | The SAS has expired or the connection string is incomplete |
| `ModuleNotFoundError` | The Python packages aren't installed, or the virtualenv isn't active |
| `shuf: command not found` | Install GNU coreutils (`brew install coreutils` on macOS) |

## Security

The generated `workers/` files, checkpoints and logs contain partition keys from your table, and those keys may include identifiers. Don't commit or share them. See [SECURITY.md](SECURITY.md) for how to report vulnerabilities.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
