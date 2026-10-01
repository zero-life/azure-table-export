# Contributing

Thanks for your interest in improving Azure Table Export! Bug reports, fixes and
ideas are all welcome.

## Reporting bugs and requesting features

Open an [issue](../../issues) using one of the templates. For bugs, please include:

- Python version and `azure-data-tables` / `boto3` versions
- The command you ran and the relevant environment variables (**redact connection strings, SAS tokens, keys, bucket names and table data**)
- The error or unexpected output from the worker log

Security problems should **not** be reported in public issues. See [SECURITY.md](SECURITY.md).

## Development setup

```bash
git clone https://github.com/zero-life/azure-table-export.git
cd azure-table-export
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt ruff
```

## Before opening a pull request

```bash
ruff check .                 # lint Python
shellcheck --severity=warning run_workers.sh
```

CI runs the same checks on every pull request.

Guidelines:

- Keep changes focused; one logical change per pull request.
- Preserve resumability: a worker must be safe to kill at any point and restart from its checkpoint without losing or duplicating uploaded chunks.
- Keep configuration in environment variables, and document any new variable in the script's docstring and the README.
- Never commit real connection strings, SAS tokens, keys, bucket names, or exported data. The `.gitignore` excludes `workers/`, `export_work/`, `*.env` and CSV output for this reason.
- Add an entry under `[Unreleased]` in [CHANGELOG.md](CHANGELOG.md).

## Code of conduct

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md). By participating you agree to uphold it.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
