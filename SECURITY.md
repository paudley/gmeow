# Security Policy

## Supported Versions

Security fixes are handled on the current default branch until formal release branches exist.

## Reporting a Vulnerability

Report vulnerabilities privately to the project maintainers. Do not open public issues containing Gmail message content, service-account keys, OAuth client secrets, local passwords, database credentials, or other sensitive data.

When reporting, include:

- affected version or commit,
- operating system and Python version,
- a minimal reproduction that does not include real mailbox data,
- expected and observed behavior.

## Security Model

Gmeow is intended for trusted single-user local systems. It exposes unauthenticated local REST/MCP access to a configured Gmail mailbox. Bind it only to loopback unless you add an external authentication and transport-security layer.
