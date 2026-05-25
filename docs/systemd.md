# systemd

Phase 00 does not ship a systemd unit. The retired Python service path has been removed, and the Go
Phase 00 binaries only validate config and report startup status.

Systemd units return in a later Go migration phase after the service runtime exists.
