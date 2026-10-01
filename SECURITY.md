# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Report vulnerabilities privately through GitHub's
[private vulnerability reporting](../../security/advisories/new)
("Security" tab → "Report a vulnerability"). You should get a response within a
week. Once a fix is available, the advisory will be published with credit to the
reporter unless you prefer to stay anonymous.

## Supported versions

Only the latest release on the `main` branch receives security fixes.

## Handling credentials safely

These scripts read credentials only from environment variables and never write
them to disk, but the files they generate can still be sensitive:

- Use a **read-only SAS** (Table service; Read + List) rather than an account key, and give it the shortest expiry that covers the export.
- `workers/*.env`, `workers/manifest.csv` and `workers/sample.json` contain your table's partition keys, which may include user, tenant or other identifiers. Treat them like the data itself.
- Checkpoint files contain Azure continuation tokens (partition/row keys).
- Never paste connection strings, SAS tokens or log excerpts containing table data into issues or pull requests.
