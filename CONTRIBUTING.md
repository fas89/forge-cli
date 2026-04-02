# Contributing to FLUID Forge

Thank you for your interest in contributing to FLUID Forge! This guide will help you get started.

## Code of Conduct

By participating in this project you agree to abide by the [Code of Conduct](CODE_OF_CONDUCT.md). Please read it before contributing.

## How to Contribute

### Reporting Bugs

1. Search [existing issues](https://github.com/Agentics-Rising/forge-cli/issues) to avoid duplicates.
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

### Branching Strategy

All contributions target the `main` branch. Use the following branch name prefixes:

| Prefix | Use for | Example |
|--------|---------|---------|
| `feat/` | New features | `feat/databricks-provider` |
| `fix/` | Bug fixes | `fix/validate-empty-contract` |
| `docs/` | Documentation changes (in this repo) | `docs/update-quickstart` |
| `provider/` | New or updated providers | `provider/azure-support` |
| `refactor/` | Code cleanup, no behaviour change | `refactor/simplify-loader` |
| `chore/` | CI, dependencies, maintenance | `chore/bump-ruff-version` |
| `test/` | Test improvements | `test/add-provider-registry-tests` |

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

8. **Address documentation** — see [Documentation Requirements](#documentation-requirements) below.

9. **Push** and open a Pull Request against `main`.

## Documentation Requirements

We maintain documentation in a **separate repository**: [Agentics-Rising/forge_docs](https://github.com/Agentics-Rising/forge_docs).

Every PR that changes user-facing behaviour must be paired with a documentation update. When you open a PR, the template asks you to choose one of:

1. **Link a docs PR** — open a companion PR in [forge_docs](https://github.com/Agentics-Rising/forge_docs) and paste the link in your CLI PR description.
2. **No docs needed** — check the box and provide a justification (e.g. internal refactor, test-only change, CI config update).
3. **Docs TODO** — acknowledge that docs are needed and commit to creating the docs PR before your CLI PR is merged.

A GitHub Actions workflow (`docs-reminder`) will automatically label your PR with `needs-docs` if none of these are addressed.

### What counts as needing docs?

- New CLI commands or flags
- Changed command behaviour or output
- New or updated providers
- Configuration changes
- Breaking changes
- New environment variables or setup steps

### What does NOT need docs?

- Internal refactors with no behaviour change
- Test-only changes
- CI/CD configuration updates
- Dependency bumps

## Coding Standards

- **Python 3.9+** — no walrus operators in hot paths, use `from __future__ import annotations` sparingly.
- **Type hints** on all public function signatures.
- **Logging** — use `logging.getLogger(__name__)` in production code, never bare `print()`.
- **No bare `except:`** — always catch specific exceptions.
- **Tests** — use `pytest`. Place unit tests in `tests/`, provider integration tests under `tests/providers/`.
- **Imports** — standard library → third-party → local, separated by blank lines. Use `ruff` to auto-sort.

## Provider Contributions

If you're building a new provider:

1. Read the [Provider SDK documentation](https://agentics-rising.github.io/forge_docs/providers/).
2. Subclass `BaseProvider` from `fluid_provider_sdk`.
3. Register via entry points in your `pyproject.toml`:
   ```toml
   [project.entry-points."fluid_build.providers"]
   my_provider = "my_package:MyProvider"
   ```
4. Include at least one working example contract.
5. See the [provider documentation](https://agentics-rising.github.io/forge_docs/providers/) for internals.

## Development Setup

```bash
# Clone
git clone https://github.com/Agentics-Rising/forge-cli.git
cd forge-cli

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

## Your First Contribution

New to FLUID Forge? Here's how to get started:

1. Browse issues labelled [`good first issue`](https://github.com/Agentics-Rising/forge-cli/labels/good%20first%20issue) for beginner-friendly tasks.
2. Comment on the issue to let maintainers know you'd like to work on it.
3. Fork the repo, create a branch following the [naming conventions](#branching-strategy), and make your changes.
4. Open a PR — our welcome bot will guide you through the checklist.
5. A maintainer will review your PR and provide feedback.

Don't be afraid to ask questions! Open a [Discussion](https://github.com/Agentics-Rising/forge-cli/discussions) if you need help.

## Getting Help

- **Discussions:** [GitHub Discussions](https://github.com/Agentics-Rising/forge-cli/discussions)
- **Bugs:** [Issue Tracker](https://github.com/Agentics-Rising/forge-cli/issues)
- **Docs:** [agentics-rising.github.io/forge_docs](https://agentics-rising.github.io/forge_docs/)

Thank you for helping make FLUID Forge better!
