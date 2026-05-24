# Running Gmeow With systemd

Gmeow is designed as a trusted single-user local service, so the recommended
setup is a user-level systemd unit.

## Install

```bash
mkdir -p ~/.config/systemd/user
cp systemd/gmeow.service ~/.config/systemd/user/gmeow.service
systemctl --user daemon-reload
systemctl --user enable --now gmeow.service
```

If you want Gmeow to start before you log in interactively, enable lingering for
your account:

```bash
loginctl enable-linger "$USER"
```

## Operate

```bash
systemctl --user status gmeow.service
journalctl --user -u gmeow.service -f
systemctl --user restart gmeow.service
systemctl --user stop gmeow.service
```

The service runs:

```bash
/home/paudley/stage/root/bin/uv run gmeow --config /home/paudley/Active.running/gmeow/config.toml serve --host 127.0.0.1 --port 8765
```

REST remains available at `http://127.0.0.1:8765/api/v1`, and MCP remains
available at `http://127.0.0.1:8765/mcp`.
