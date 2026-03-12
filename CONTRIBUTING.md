# Contributing to FLUID Forge

Thank you for your interest in contributing to FLUID Forge! This guide will help you get started.

## Code of Conduct

By participating in this project you agree to abide by the [Code of Conduct](CODE_OF_CONDUCT.md). Please read it before contributing.

## How to Contribute

### Reporting Bugs

1. Search [existing issues](https://github.com/agentics-rising/fluid-forge-cli/issues) to avoid duplicates.
2. Open a new issue using the **Bug Report** template.
3. Include:
   - FLUID Forge version (`fluid version`)
   - Python version (`python3 --version`)
   - Operating system
   - Steps to reproduce
   - Expected vs actual behaviour
   - Contract YAML (redact any secrets)

### Suggesting Features

1. Open an issue using the **Feature Request** template.
2. Describe the problem you'd like to solve, not just the solution.
3. If the feature involves a new provider, use the **Provider Request** template instead.

### Submitting Code

1. **Fork** the repository and create a branch from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```

2. **Install** in development mode:
   ```bash
   pip install -e ".[dev,local]"
   ```

3. **Make your changes.** Follow the coding standards below.

4. **Add tests.** All new code must include tests. Run the suite:
   ```bash
   pytest
   ```

5. **Lint and format:**
   ```bash
   ruff check fluid_build/ tests/
   black fluid_build/ tests/
   ```

6. **Add license headers** to any new Python files:
   ```bash
   python scripts/add_license_headers.py
   ```

7. **Commit** with a clear message following [Conventional Commits](https://www.conventionalcommits.org/):
   ```
   feat(provider): add Databricks provider skeleton
   fix(cli): handle empty contract in validate
   docs: update quickstart for v0.8
   ```

8. **Push** and open a Pull Request against `main`.

## Coding Standards

- **Python 3.9+** — no walrus operators in hot paths, use `from __future__ import annotations` sparingly.
- **Type hints** on all public function signatures.
- **Logging** — use `logging.getLogger(__name__)` in production code, never bare `print()`.
- **No bare `except:`** — always catch specific exceptions.
- **Tests** — use `pytest`. Place unit tests in `tests/`, provider integration tests under `tests/providers/`.
- **Imports** — standard library → third-party → local, separated by blank lines. Use `ruff` to auto-sort.

## Provider Contributions

If you're building a new provider:

1. Read the [Provider SDK documentation](https://fluidhq.io/docs/providers/sdk).
2. Subclass `BaseProvider` from `fluid_provider_sdk`.
3. Register via entry points in your `pyproject.toml`:
   ```toml
   [project.entry-points."fluid_build.providers"]
   my_provider = "my_package:MyProvider"
   ```
4. Include at least one working example contract.
5. See [Architecture Deep-Dive](https://fluidhq.io/docs/providers/architecture) for internals.

## Development Setup

```bash
# Clone
git clone https://github.com/agentics-rising/fluid-forge-cli.git
cd fluid-forge-cli

# Create virtualenv (recommended)
python3 -m venv .venv && source .venv/bin/activate

# Install with all dev + provider extras
pip install -e ".[dev,local,gcp,snowflake,viz]"

# Run tests
pytest

# Run a single test file
pytest tests/providers/test_registry.py -v
```

## Contributor License Agreement (CLA)

By submitting a pull request, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE) and that you have the right to license them.

## Getting Help

- **Discussions:** [GitHub Discussions](https://github.com/agentics-rising/fluid-forge-cli/discussions)
- **Bugs:** [Issue Tracker](https://github.com/agentics-rising/fluid-forge-cli/issues)
- **Docs:** [fluidhq.io/docs](https://fluidhq.io/docs)

Thank you for helping make FLUID Forge better!
