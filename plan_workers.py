#!/usr/bin/env python3
"""
Plan a complete, parallel export of the whole table for azure_table_to_s3.py.

Every row in the table is exported (all partitions, including the duplicate
copies each audit record has). Deduplicate afterwards by RowKey if you want
one copy per record.

How it works (no partition enumeration needed)
  Each record's AllPartitions column lists every partition it was written to,
  and every record has an hourly audit_YYYYMMDD_HH partition. A sample of rows
  from the hourly partitions therefore shows roughly how many rows every other
  partition holds. The planner:
    1. Samples SAMPLE_ROWS rows at SAMPLE_HOURS random hours in every month
       (AllPartitions column only) and tallies how often each partition appears.
    2. Builds worker filters that tile the ENTIRE PartitionKey space with no
       gaps or overlaps, so every row is exported exactly once whether or not
       its partition showed up in the sample:
         - hourly audit_ partitions: one worker per month (PartitionKey range)
         - large partitions (bigger than a normal group): split by year, or by
           month for the very largest (RowKey range; RowKey starts with
           reverse .NET ticks)
         - everything else: contiguous PartitionKey ranges holding roughly equal
           shares of rows, with boundaries taken from the sample
    3. Drops empty time ranges with 1-row probes, then writes one <name>.env per
       worker plus manifest.csv. Launch with run_workers.sh.
  The sample is cached in OUTPUT_DIR/sample.json; delete it to resample.

Requirements
  pip install azure-data-tables

Configuration (environment variables)
  AZURE_STORAGE_CONNECTION_STRING   (or AZURE_STORAGE_ACCOUNT_NAME + _KEY)
  AZURE_TABLE_NAME                  required
  S3_BASE_PREFIX   default azure-table-export/<table>. Layout:
                     <base>/audit-hourly/<YYYY-MM>/
                     <base>/dense/<dNNNN-name>/<YYYY or YYYY-MM>/
                     <base>/groups/<gNNNNN>/
  START_YEAR       first time range, default 2016 (older rows go to "pre-<year>")
  GROUPS           number of range groups for ordinary partitions, default 400
  SAMPLE_HOURS     random hours sampled per month, default 6
  SAMPLE_ROWS      rows read per sampled hour, default 500
  THREADS          parallel planning queries, default 16
  OUTPUT_DIR       default ./workers
  WORK_BASE        local scratch root, one subfolder per worker, default ./export_work
"""

import calendar
import collections
import csv
import datetime as dt
import itertools
import json
import logging
import os
import random
import re
import shlex
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableClient

log = logging.getLogger("plan_workers")

MAX_TICKS = 3155378975999999999          # .NET DateTime.MaxValue.Ticks
DOTNET_EPOCH = dt.datetime(1, 1, 1)
AUDIT_PREFIX = "audit_"
AUDIT_END = "audit`"                     # '`' sorts right after '_', bounding every 'audit_...' key


def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


TABLE_NAME = env("AZURE_TABLE_NAME", required=True)
S3_BASE_PREFIX = env("S3_BASE_PREFIX", f"azure-table-export/{TABLE_NAME}").strip("/")
START_YEAR = int(env("START_YEAR", "2016"))
GROUPS = int(env("GROUPS", "400"))
SAMPLE_HOURS = int(env("SAMPLE_HOURS", "6"))
SAMPLE_ROWS = int(env("SAMPLE_ROWS", "500"))
THREADS = int(env("THREADS", "16"))
OUTPUT_DIR = Path(env("OUTPUT_DIR", "./workers"))
WORK_BASE = env("WORK_BASE", "./export_work").rstrip("/")
SAMPLE_FILE = OUTPUT_DIR / "sample.json"


# ---------------------------------------------------------------- Azure helpers

def make_table_client():
    conn = env("AZURE_STORAGE_CONNECTION_STRING")
    if conn:
        return TableClient.from_connection_string(conn, table_name=TABLE_NAME)
    account = env("AZURE_STORAGE_ACCOUNT_NAME", required=True)
    key = env("AZURE_STORAGE_ACCOUNT_KEY", required=True)
    endpoint = f"https://{account}.table.core.windows.net"
    return TableClient(endpoint, TABLE_NAME, credential=AzureNamedKeyCredential(account, key))


_local = threading.local()


def client():
    if not hasattr(_local, "table"):
        _local.table = make_table_client()
    return _local.table


def take(query_filter, select, n):
    """Up to n entities matching the filter."""
    pager = client().query_entities(query_filter, select=select, results_per_page=min(n, 1000))
    return list(itertools.islice(pager, n))


def first_entity(query_filter, select):
    rows = take(query_filter, select, 1)
    return rows[0] if rows else None


def odata_str(value):
    return "'" + value.replace("'", "''") + "'"


def pk_filter(lower, upper):
    """lower: None or ("ge"|"gt", key); upper: None or key (exclusive)."""
    parts = []
    if lower:
        parts.append(f"PartitionKey {lower[0]} {odata_str(lower[1])}")
    if upper is not None:
        parts.append(f"PartitionKey lt {odata_str(upper)}")
    return " and ".join(parts)


# ---------------------------------------------------------------- time ranges

def period_starts(by):
    now = dt.datetime.now()
    if by == "year":
        return [(dt.datetime(y, 1, 1), f"{y:04d}") for y in range(START_YEAR, now.year + 1)]
    months, y, m = [], START_YEAR, 1
    while (y, m) <= (now.year, now.month):
        months.append((dt.datetime(y, m, 1), f"{y:04d}-{m:02d}"))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def rowkey_bound(d):
    """Rows stamped at/after d have RowKey < bound; earlier rows have RowKey >= bound."""
    delta = d - DOTNET_EPOCH
    ticks = (delta.days * 86400 + delta.seconds) * 10_000_000 + delta.microseconds * 10
    return str(MAX_TICKS - ticks + 1)


def rowkey_ranges(by):
    """[(label, rk_lower, rk_upper)] tiling the whole RowKey space, open-ended at both ends."""
    starts = period_starts(by)
    ranges = [(f"pre-{START_YEAR}", rowkey_bound(starts[0][0]), None)]
    for i, (start, label) in enumerate(starts):
        lower = rowkey_bound(starts[i + 1][0]) if i + 1 < len(starts) else None
        ranges.append((label, lower, rowkey_bound(start)))
    return ranges


def audit_month_ranges():
    """[(label, pk_lower, pk_upper)] tiling 'audit_' .. 'audit`'."""
    starts = period_starts("month")
    keys = [f"{AUDIT_PREFIX}{d.year:04d}{d.month:02d}" for d, _ in starts]
    ranges = [(f"pre-{START_YEAR}", AUDIT_PREFIX, keys[0])]
    for i, (_, label) in enumerate(starts):
        ranges.append((label, keys[i], keys[i + 1] if i + 1 < len(keys) else AUDIT_END))
    return ranges


# ---------------------------------------------------------------- step 1: sample

def sample_points():
    rng = random.Random(int(env("SEED", "42")))
    now = dt.datetime.now()
    points = []
    for start, _ in period_starts("month"):
        days = calendar.monthrange(start.year, start.month)[1]
        for _ in range(SAMPLE_HOURS):
            d = start.replace(day=rng.randint(1, days), hour=rng.randint(0, 23))
            if d <= now:
                points.append(d)
    return points


def sample_at(d):
    # From this hour to the end of that day, so it works whether or not hours are zero-padded
    day = f"{AUDIT_PREFIX}{d:%Y%m%d}"
    flt = f"PartitionKey ge '{day}_{d.hour:02d}' and PartitionKey lt '{day}`'"
    return [r.get("AllPartitions") or "" for r in take(flt, ["AllPartitions"], SAMPLE_ROWS)]


def load_sample():
    if SAMPLE_FILE.exists():
        data = json.loads(SAMPLE_FILE.read_text())
        log.info("Using cached sample (%s rows) from %s", f"{data['rows']:,}", SAMPLE_FILE)
        return data

    points = sample_points()
    log.info("Sampling up to %d rows at %d points across the hourly partitions (%d threads)...",
             SAMPLE_ROWS, len(points), THREADS)
    started = time.time()
    weights, rows = collections.Counter(), 0
    with ThreadPoolExecutor(THREADS) as pool:
        for i, batch in enumerate(pool.map(sample_at, points), 1):
            for value in batch:
                if not value:
                    continue
                rows += 1
                for p in set(value.split("~")):
                    if p and not p.startswith(AUDIT_PREFIX):
                        weights[p] += 1
            if i % 100 == 0:
                log.info("  ...%d/%d points, %s rows", i, len(points), f"{rows:,}")
    log.info("Sampled %s rows naming %s distinct partitions in %.0fs",
             f"{rows:,}", f"{len(weights):,}", time.time() - started)
    if rows == 0:
        sys.exit("Sample came back empty. Check AZURE_TABLE_NAME, START_YEAR and the connection string.")

    data = {"rows": rows, "weights": dict(weights)}
    SAMPLE_FILE.parent.mkdir(parents=True, exist_ok=True)
    SAMPLE_FILE.write_text(json.dumps(data))
    return data


# ---------------------------------------------------------------- step 2: plan

def slug(text, length=40):
    return re.sub(r"[^A-Za-z0-9]+", "-", text).strip("-")[:length] or "x"


def build_plan(weights):
    total = sum(weights.values())
    target = max(total / GROUPS, 1)
    parts = sorted(weights.items())
    workers = []
    state = {"group": [], "acc": 0, "lower": None, "gidx": 0, "didx": 0}

    def add(name, kind, query_filter, s3_suffix, detail, probe):
        workers.append({"name": name, "kind": kind, "filter": query_filter,
                        "s3_prefix": f"{S3_BASE_PREFIX}/{s3_suffix}",
                        "work_dir": f"{WORK_BASE}/{name}", "detail": detail, "probe": probe})

    def close_group(upper):
        # Emitted even when the sample saw no partitions in the range, so the plan stays
        # airtight for partitions the sample missed (such workers finish quickly if empty).
        group = state["group"]
        state["gidx"] += 1
        name = f"g{state['gidx']:05d}"
        detail = (f"~{state['acc'] / total:.3%} of rows; {len(group)} sampled partitions: {group[0]} .. {group[-1]}"
                  if group else "no partitions in sample (gap range)")
        add(name, "group", pk_filter(state["lower"], upper), f"groups/{name}", detail, False)
        state["group"], state["acc"] = [], 0

    def add_audit():
        close_group(AUDIT_PREFIX)
        for label, lo, hi in audit_month_ranges():
            add(f"audit_{label}", "audit-hourly",
                f"PartitionKey ge '{lo}' and PartitionKey lt '{hi}'",
                f"audit-hourly/{label}", "hourly audit partitions", True)
        state["lower"] = ("ge", AUDIT_END)

    audit_done = False
    dense = []
    for pk, w in parts:
        if not audit_done and pk > AUDIT_PREFIX:
            add_audit()
            audit_done = True
        if w >= target and w >= 20:
            close_group(pk)
            state["didx"] += 1
            dname = f"d{state['didx']:04d}-{slug(pk)}"
            by = "month" if w >= 12 * target else "year"
            dense.append((pk, w, by))
            for label, lo, hi in rowkey_ranges(by):
                flt = f"PartitionKey eq {odata_str(pk)}"
                if lo:
                    flt += f" and RowKey ge '{lo}'"
                if hi:
                    flt += f" and RowKey lt '{hi}'"
                add(f"{dname}_{label}", "dense", flt, f"dense/{dname}/{label}", f"~{w / total:.3%} of rows: {pk}", True)
            state["lower"] = ("gt", pk)
        else:
            if state["group"] and state["acc"] + w > target:
                close_group(pk)
                state["lower"] = ("ge", pk)
            state["group"].append(pk)
            state["acc"] += w
    if not audit_done:
        add_audit()
    close_group(None)
    return workers, dense, total


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if any(OUTPUT_DIR.glob("*.env")):
        sys.exit(f"{OUTPUT_DIR} already has .env files. Move or delete them before re-planning.")

    sample = load_sample()
    workers, dense, total = build_plan(sample["weights"])

    log.info("%d large partitions will be split by time. Largest:", len(dense))
    for pk, w, by in sorted(dense, key=lambda x: -x[1])[:15]:
        log.info("  %6.2f%%  by %-5s  %s", 100 * w / total, by, pk)

    to_probe = [w for w in workers if w["probe"]]
    log.info("Probing %d time ranges for data...", len(to_probe))
    with ThreadPoolExecutor(THREADS) as pool:
        has_data = list(pool.map(lambda w: first_entity(w["filter"], ["PartitionKey"]) is not None, to_probe))
    empty = {id(w) for w, ok in zip(to_probe, has_data) if not ok}
    workers = [w for w in workers if id(w) not in empty]

    for w in workers:
        (OUTPUT_DIR / f"{w['name']}.env").write_text(
            f"export AZURE_TABLE_FILTER={shlex.quote(w['filter'])}\n"
            f"export S3_PREFIX={shlex.quote(w['s3_prefix'])}\n"
            f"export WORK_DIR={shlex.quote(w['work_dir'])}\n"
        )
    with open(OUTPUT_DIR / "manifest.csv", "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["name", "kind", "filter", "s3_prefix", "work_dir", "detail"],
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(workers)

    counts = collections.Counter(w["kind"] for w in workers)
    log.info("Wrote %d workers to %s: %s", len(workers), OUTPUT_DIR,
             ", ".join(f"{v} {k}" for k, v in sorted(counts.items())))


if __name__ == "__main__":
    main()
