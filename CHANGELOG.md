# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-10-01

### Added
- `azure_table_to_s3.py`: resumable, checkpointed export of an Azure Table (or a filtered range of it) to CSV / gzipped CSV chunks in S3.
- `plan_workers.py`: sample-based planner that splits a table into hundreds of non-overlapping worker ranges.
- `run_workers.sh`: parallel runner with a concurrency cap that skips finished workers and resumes failed ones.
