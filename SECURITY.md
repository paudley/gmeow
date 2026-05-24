<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc. -->
<!-- SPDX-License-Identifier: MIT -->

# Gmeow Security Policy

Gmeow is maintained by Blackcat Informatics Inc. We take security reports
seriously, especially because the project handles local Gmail data, OAuth or
service-account credentials, attachment content, and a local PostgreSQL cache.

Please report vulnerabilities privately so they can be validated and fixed
responsibly.

## Supported Versions

| Version | Supported |
| --- | --- |
| `0.1.x` | Yes |
| `< 0.1` | No |

Until formal release branches exist, security fixes are handled on the current
default branch.

## Security Model

Gmeow is intended for trusted, single-user local systems.

- REST, MCP, and optional IMAP interfaces should bind to loopback only unless
  you add an external authentication and transport-security layer.
- The local PostgreSQL database should be treated as sensitive mailbox data.
- `config.toml`, OAuth files, service-account JSON, SOPS files, object-store
  content, mailbox exports, and runtime data must not be committed.
- Attachment analysis can process untrusted files. Keep external tools and
  dependencies current, and run analysis in a suitably isolated local
  environment when handling hostile samples.

## Reporting a Vulnerability

Do not open a public GitHub issue for a security vulnerability.

Instead:

- email <security@blackcat.ca>
- include `GMEOW SECURITY` in the subject line
- describe the issue, impact, and affected versions or commits
- include reproduction steps, proof of concept, or patches when possible
- use synthetic mailbox data in reproductions

Never include real Gmail message content, access tokens, service-account keys,
OAuth client secrets, local passwords, database credentials, or private
attachment data in a public report.

## What to Expect

- acknowledgment within 48 hours
- initial triage within 7 days
- coordinated remediation and disclosure after validation

Resolution timelines depend on severity, exploitability, and release
constraints, but we aim to address confirmed issues as quickly as practical.

## Responsible Disclosure Process

1. Report the issue privately.
2. Maintainers validate and triage the report.
3. A fix is developed, reviewed, and tested.
4. A release or advisory is prepared.
5. Public disclosure follows after users have had reasonable time to update.

## Reporter Credit

For each vulnerability report resolved in the last 12 months, public release
notes or security advisories should credit the reporter unless the reporter
requests anonymity or private handling. If multiple reporters contributed to a
confirmed issue, credit each reporter who wants public recognition.

There are currently no publicly disclosed Gmeow vulnerabilities resolved in the
last 12 months.

## Security Updates

- watch the repository for releases and advisories
- keep dependencies current
- rotate Gmail and database credentials if you suspect local compromise
- update to the latest supported release when fixes are published

## Contact

- Email: <security@blackcat.ca>

For non-security questions or ordinary bug reports, use the normal public issue
or discussion channels.
