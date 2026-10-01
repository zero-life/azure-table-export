#!/usr/bin/env python3
"""
Resumable export of a large Azure Table Storage table to CSV chunks in S3.

How it works
  - Pages through the table with Azure continuation tokens (server-side paging;
    only one page is held in memory at a time).
  - Writes rows to a local CSV until CHUNK_ROWS is reached, uploads the chunk
    to S3, deletes the local file, then saves a checkpoint.
  - The checkpoint stores the continuation token for the START of the next
    chunk, so a crash at any point costs at most one partial chunk. Re-running
    the script resumes from the checkpoint and overwrites that partial chunk.
  - Transient Azure errors are retried with backoff, resuming from the last
    good page.

Requirements
  pip install azure-data-tables boto3

Configuration (environment variables)
  Azure auth (one of):
    AZURE_STORAGE_CONNECTION_STRING        (a read-only SAS connection string works)
    AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY
  AZURE_TABLE_NAME     required
  AZURE_TABLE_FILTER   optional OData filter, e.g.
                       "PartitionKey ge 'A' and PartitionKey lt 'M'"
                       (PartitionKey/RowKey filters are indexed and efficient)
  S3_BUCKET            required
  S3_PREFIX            optional, default "azure-table-export/<table>"
  AWS credentials      standard boto3 chain (AWS_PROFILE, AWS_ACCESS_KEY_ID /
                       AWS_SECRET_ACCESS_KEY, or an instance role)
  CHUNK_ROWS           rows per CSV file, default 1000000
  PAGE_SIZE            rows per request, max 1000, default 1000
  WORK_DIR             local scratch directory, default ./export_work
  CHECKPOINT_FILE      default <WORK_DIR>/<table>.checkpoint.json
  GZIP                 "true" to write .csv.gz (smaller, faster uploads), default false

Running parallel workers
  Give each worker its own AZURE_TABLE_FILTER (non-overlapping PartitionKey
  ranges), its own CHECKPOINT_FILE, and its own S3_PREFIX.

CSV layout
  PartitionKey, RowKey, Timestamp, then every property found on the first page
  (sorted). Azure Tables are schemaless, so any property not in that set is
  kept as JSON in the final "_extra" column rather than dropped.
"""

import base64
import csv
import datetime as dt
import gzip
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path

import boto3
from azure.core.credentials import AzureNamedKeyCredential
from azure.core.exceptions import AzureError
from azure.data.tables import TableClient

log = logging.getLogger("azure_table_export")


def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


TABLE_NAME = env("AZURE_TABLE_NAME", required=True)
TABLE_FILTER = env("AZURE_TABLE_FILTER")
S3_BUCKET = env("S3_BUCKET", required=True)
S3_PREFIX = env("S3_PREFIX", f"azure-table-export/{TABLE_NAME}").strip("/")
CHUNK_ROWS = int(env("CHUNK_ROWS", "1000000"))
PAGE_SIZE = min(int(env("PAGE_SIZE", "1000")), 1000)
WORK_DIR = Path(env("WORK_DIR", "./export_work"))
CHECKPOINT_FILE = Path(env("CHECKPOINT_FILE", str(WORK_DIR / f"{TABLE_NAME}.checkpoint.json")))
USE_GZIP = env("GZIP", "false").lower() == "true"

MAX_RETRIES = 8
PROGRESS_EVERY_PAGES = 50
META_COLS = ["PartitionKey", "RowKey", "Timestamp"]
EXTRA_COL = "_extra"


# ---------------------------------------------------------------- clients

def make_table_client():
    conn = env("AZURE_STORAGE_CONNECTION_STRING")
    if conn:
        return TableClient.from_connection_string(conn, table_name=TABLE_NAME)
    account = env("AZURE_STORAGE_ACCOUNT_NAME", required=True)
    key = env("AZURE_STORAGE_ACCOUNT_KEY", required=True)
    endpoint = f"https://{account}.table.core.windows.net"
    return TableClient(endpoint, TABLE_NAME, credential=AzureNamedKeyCredential(account, key))


# ---------------------------------------------------------------- checkpoint

def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"chunk_index": 0, "token": None, "columns": None, "rows_exported": 0, "done": False}


def save_checkpoint(cp):
    tmp = CHECKPOINT_FILE.with_name(CHECKPOINT_FILE.name + ".tmp")
    tmp.write_text(json.dumps(cp, indent=2))
    tmp.replace(CHECKPOINT_FILE)  # atomic, so a crash never leaves a half-written checkpoint


# ---------------------------------------------------------------- rows

def to_cell(value):
    if value is None:
        return ""
    if hasattr(value, "value") and hasattr(value, "edm_type"):  # EntityProperty (e.g. Int64)
        value = value.value
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def discover_columns(entities):
    props = set()
    for e in entities:
        props.update(k for k in e.keys() if k not in ("PartitionKey", "RowKey"))
    return META_COLS + sorted(props) + [EXTRA_COL]


def entity_to_row(entity, colset):
    metadata = getattr(entity, "metadata", {}) or {}
    row = {
        "PartitionKey": entity.get("PartitionKey"),
        "RowKey": entity.get("RowKey"),
        "Timestamp": to_cell(metadata.get("timestamp")),
    }
    extra = {}
    for k, v in entity.items():
        if k in ("PartitionKey", "RowKey"):
            continue
        if k in colset:
            row[k] = to_cell(v)
        else:
            extra[k] = to_cell(v)
    row[EXTRA_COL] = json.dumps(extra, default=str) if extra else ""
    return row


class ChunkWriter:
    def __init__(self, index, columns):
        ext = ".csv.gz" if USE_GZIP else ".csv"
        self.name = f"{TABLE_NAME}_part{index:06d}{ext}"
        self.path = WORK_DIR / self.name
        if USE_GZIP:
            self.fh = gzip.open(self.path, "wt", newline="", encoding="utf-8")
        else:
            self.fh = open(self.path, "w", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.fh, fieldnames=columns)
        self.writer.writeheader()
        self.rows = 0

    def write(self, row):
        self.writer.writerow(row)
        self.rows += 1

    def close(self):
        self.fh.close()


# ---------------------------------------------------------------- I/O

def iter_pages(table, start_token):
    """Yield (entities, token_for_next_page). On transient errors, rebuild the
    pager from the last good token and carry on."""
    token = start_token
    attempt = 0
    while True:
        try:
            if TABLE_FILTER:
                pager = table.query_entities(TABLE_FILTER, results_per_page=PAGE_SIZE)
            else:
                pager = table.list_entities(results_per_page=PAGE_SIZE)
            pages = pager.by_page(continuation_token=token)
            for page in pages:
                entities = list(page)
                token = pages.continuation_token
                attempt = 0
                yield entities, token
                if token is None:
                    return
            return
        except AzureError as exc:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            wait = min(2 ** attempt, 120)
            log.warning("Azure error (attempt %d/%d), retrying in %ds: %s", attempt, MAX_RETRIES, wait, exc)
            time.sleep(wait)


def upload_with_retry(s3, path, key):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            s3.upload_file(str(path), S3_BUCKET, key)  # handles multipart for large files
            return
        except Exception as exc:  # boto3 raises several unrelated exception types
            if attempt == MAX_RETRIES:
                raise
            wait = min(2 ** attempt, 120)
            log.warning("S3 upload failed (attempt %d/%d), retrying in %ds: %s", attempt, MAX_RETRIES, wait, exc)
            time.sleep(wait)


# ---------------------------------------------------------------- main

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)  # hide per-request HTTP logs
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    cp = load_checkpoint()
    if cp.get("done"):
        log.info("Export already complete per %s. Delete it to start over.", CHECKPOINT_FILE)
        return

    table = make_table_client()
    s3 = boto3.client("s3")

    columns = cp["columns"]
    colset = set(columns) if columns else set()
    chunk = None
    session_rows = 0
    pages_seen = 0
    started = time.time()
    last_rows, last_time = 0, started

    log.info("Table %s -> s3://%s/%s | resuming at chunk %d, %s rows already exported",
             TABLE_NAME, S3_BUCKET, S3_PREFIX, cp["chunk_index"], f"{cp['rows_exported']:,}")

    def finish_chunk(next_token):
        nonlocal chunk
        chunk.close()
        key = f"{S3_PREFIX}/{chunk.name}"
        upload_with_retry(s3, chunk.path, key)
        chunk.path.unlink()
        cp["rows_exported"] += chunk.rows
        cp["chunk_index"] += 1
        cp["token"] = next_token
        cp["columns"] = columns
        save_checkpoint(cp)  # only after the upload succeeds
        log.info("Uploaded s3://%s/%s (%s rows) | total %s",
                 S3_BUCKET, key, f"{chunk.rows:,}", f"{cp['rows_exported']:,}")
        chunk = None

    for entities, next_token in iter_pages(table, cp["token"]):
        pages_seen += 1
        if entities:
            if columns is None:
                columns = discover_columns(entities)
                colset = set(columns)
                log.info("Columns: %s", ", ".join(columns))
            if chunk is None:
                chunk = ChunkWriter(cp["chunk_index"], columns)
            for entity in entities:
                chunk.write(entity_to_row(entity, colset))
            session_rows += len(entities)

        if chunk and chunk.rows >= CHUNK_ROWS and next_token is not None:
            finish_chunk(next_token)

        if pages_seen % PROGRESS_EVERY_PAGES == 0:
            now = time.time()
            interval_rate = (session_rows - last_rows) / (now - last_time) if now > last_time else 0
            last_rows, last_time = session_rows, now
            log.info("Progress: %s rows this session | %.0f rows/sec (last %d pages) | ~%s rows/hour",
                     f"{session_rows:,}", interval_rate, PROGRESS_EVERY_PAGES, f"{int(interval_rate * 3600):,}")

    if chunk and chunk.rows:
        finish_chunk(None)

    cp["done"] = True
    save_checkpoint(cp)
    s3.upload_file(str(CHECKPOINT_FILE), S3_BUCKET, f"{S3_PREFIX}/_export_manifest.json")
    log.info("Export complete: %s rows in %d chunks", f"{cp['rows_exported']:,}", cp["chunk_index"])


if __name__ == "__main__":
    main()
