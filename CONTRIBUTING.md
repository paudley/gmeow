<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc. -->
<!-- SPDX-License-Identifier: AGPL-3.0-only -->

# Contributing to Gmeow

Gmeow is an open source local Gmail intelligence, archive, REST, MCP, and
read-only IMAP service maintained by Blackcat Informatics Inc. Contributions to
code, tests, documentation, migrations, and release tooling are welcome.

## Code of Conduct

Please read the [Code of Conduct](CODE_OF_CONDUCT.md) before participating. We
expect respectful, professional collaboration. To report unacceptable behavior,
email <conduct@blackcat.ca>.

## Security and Privacy First

Gmeow works with private mailbox data. Treat privacy as part of the engineering
contract.

- Do not commit `config.toml`, credentials, mailbox exports, object-store data,
  database dumps, SOPS files, or runtime data.
- Use synthetic fixtures in tests and examples.
- Do not paste real Gmail messages, access tokens, service-account keys,
  database passwords, or private attachment content into issues or pull
  requests.
- Report vulnerabilities through [SECURITY.md](SECURITY.md), not public issues.

## Ways to Contribute

### Report Bugs

Before opening an issue:

- search existing issues first
- verify the problem on the latest code in `main` when possible
- reproduce against synthetic data or a disposable local database

Include:

- a clear title
- exact reproduction steps
- expected behavior
- actual behavior
- relevant command output or stack traces
- OS, Python version, PostgreSQL version, and `uv` version

### Suggest Enhancements

Good enhancement requests usually include:

- the workflow or mailbox-management problem you are solving
- the current limitation
- the proposed behavior
- concrete examples of expected REST, MCP, CLI, or database behavior

### Submit Pull Requests

1. Fork the repository and create a branch from `main`.
2. Install dependencies.
3. Make the smallest coherent change that solves the problem.
4. Add or update tests when behavior changes.
5. Update README or docs when public behavior, commands, endpoints, schemas, or
   outputs change.
6. Run the verification steps below before requesting review.

## Contributor License Agreement

External contributions may require the project Contributor License Agreement,
enforced by CLA Assistant:

The CLA link is provided by CLA Assistant on pull requests when it is required.

When CLA Assistant comments on your pull request, follow the signing link and
sign in with GitHub to accept the agreement. The CLA confirms that you have the
right to submit the contribution and grants the project the rights needed to use
and redistribute it.

Developer Certificate of Origin style sign-off trailers are welcome as
additional contribution evidence, but DCO sign-off does not replace a required
CLA Assistant check:

```text
Signed-off-by: Your Name <you@example.com>
```

## Governance and Continuity

Gmeow is maintained by Blackcat Informatics Inc. Project decisions are made
through GitHub issues, pull requests, discussions, security reports, and release
reviews, using the repository quality bar, security policy, and release process
as the decision framework.

Repository owners and code owners are listed in `.github/CODEOWNERS` when that
file exists.

## Development Setup

### Prerequisites

- Git
- Python 3.13+
- PostgreSQL suitable for integration testing
- `uv`

### Local Setup

```bash
uv sync --extra test
cp config.toml-example config.toml
```

Use a disposable database for integration testing. Do not point tests or local
development at a production mailbox database.

Example local test DSN:

```text
postgresql://gmeow-test:gmeow-test@127.0.0.1:5432/gmeow-test
```

## Project-Specific Guidance

- If REST endpoint behavior changes, update API examples and tests.
- If MCP tools change, update help text and any client-facing examples.
- If CLI behavior changes, update README usage and command verification.
- If database schema changes, add or update Alembic migrations and migration
  comments.
- If attachment analysis behavior changes, cover privacy and hostile-file
  handling risks in tests or docs.
- If archive, retention, or object-store behavior changes, verify data-loss and
  restore paths explicitly.

## Coding Style

Contributions must pass the project lint, formatting, and test gates.

- Python follows PEP 8 plus the stricter Ruff configuration in this repository.
- Type hints are expected for public functions and non-trivial helpers.
- Prefer structural fixes over lint suppressions.
- Keep changes focused; avoid unrelated refactors.
- Use standard library or existing project helpers before adding dependencies.

## Verification

Before requesting review, run the checks relevant to your change:

```bash
uvx ruff format --config ruff.toml main.py migrations scripts src
uvx ruff check --config ruff.toml main.py migrations scripts src
uv run python -m compileall main.py src migrations tests
uv run pytest
uv build
uv run python scripts/public_release_check.py
```

For database-backed changes, also run against a disposable local PostgreSQL
database and document the DSN shape used, without committing private
credentials.

## Commit Messages

We prefer Conventional Commits:

- `feat:` new functionality
- `fix:` bug fixes
- `docs:` documentation-only changes
- `refactor:` internal restructuring without behavior change
- `test:` test additions or updates
- `chore:` maintenance work

Examples:

```text
feat: add archive verification endpoint
fix: preserve raw RFC822 during retention preview
docs: clarify local database setup
test: cover category exclusion in text search
```

## Questions

For public questions, open an issue or discussion in the repository. For private
matters, email <oss@blackcat.ca>.

## License

By contributing, you agree that your contributions will be licensed under the
project license defined in [LICENSE](LICENSE).
