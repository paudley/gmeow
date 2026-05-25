# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Provide read-only IMAP access to the Gmeow archive.

Implements a minimal RFC 3501 server that exposes cached Gmail bodies through standard IMAP
clients while preserving the loopback-only trust boundary. The server reuses the durable cache
so IMAP fetches always return canonical RFC822 bytes rather than re-hydrating Gmail.
"""

import asyncio
import re
from dataclasses import dataclass
from typing import Any

from .app import build_services
from .config import GmeowConfig

IMAP_COMMAND_EXCEPTIONS = (RuntimeError, ValueError, KeyError, TypeError, OSError, UnicodeError)
MIN_COMMAND_PARTS = 2


@dataclass(slots=True)
class ImapSession:
    """Represent ImapSession data and behavior."""

    authenticated: bool = False
    selected: str = ""


class ReadOnlyImapServer:
    """Represent ReadOnlyImapServer data and behavior."""

    def __init__(self, config: GmeowConfig) -> None:
        """Initialize ReadOnlyImapServer."""
        self.config = config
        self.cache, self.attachments, self.semantic, self.graph, self.sync = build_services(config)
        self.password = _read_password(config)

    async def serve(self) -> None:
        """Serve."""
        server = await asyncio.start_server(self._handle_client, self.config.imap.host, self.config.imap.port)
        self.cache.record_operational_event(
            "imap.started",
            "info",
            "imap",
            "",
            "Read-only IMAP server started.",
            {"host": self.config.imap.host, "port": self.config.imap.port},
        )
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = ImapSession()
        await _write_line(writer, "* OK Gmeow read-only IMAP4rev1 ready")
        while not reader.at_eof():
            raw = await reader.readline()
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            if not line:
                continue
            tag, command, rest = _parse_command(line)
            try:
                close = await self._dispatch(writer, session, tag, command, rest)
            except IMAP_COMMAND_EXCEPTIONS as exc:
                await _write_line(writer, f"{tag} NO {type(exc).__name__}: {str(exc)[:200]}")
                close = False
            if close:
                break
        writer.close()
        await writer.wait_closed()

    async def _dispatch(self, writer: asyncio.StreamWriter, session: ImapSession, tag: str, command: str, rest: str) -> bool:
        upper = command.upper()
        if upper in {"CAPABILITY", "NOOP", "LOGOUT", "LOGIN"}:
            return await self._dispatch_session_command(writer, session, tag, upper, rest)
        if not session.authenticated:
            await _write_line(writer, f"{tag} NO authenticate first")
        elif upper in {"LIST", "LSUB", "SELECT", "EXAMINE", "STATUS"}:
            await self._dispatch_mailbox_command(writer, session, tag, upper, rest)
        elif upper == "UID":
            await self._dispatch_uid_command(writer, session, tag, rest)
        elif upper == "FETCH":
            await self._fetch(writer, session, tag, rest, uid_mode=False)
        elif upper == "SEARCH":
            await self._search(writer, session, tag, rest, uid_mode=False)
        elif upper in {"STORE", "COPY", "APPEND", "EXPUNGE", "CREATE", "DELETE", "RENAME", "SUBSCRIBE", "UNSUBSCRIBE"}:
            await _write_line(writer, f"{tag} NO gmeow IMAP is read-only")
        else:
            await _write_line(writer, f"{tag} BAD unsupported command")
        return False

    async def _dispatch_uid_command(self, writer: asyncio.StreamWriter, session: ImapSession, tag: str, rest: str) -> None:
        sub, _, sub_rest = rest.partition(" ")
        if sub.upper() == "FETCH":
            await self._fetch(writer, session, tag, sub_rest, uid_mode=True)
        elif sub.upper() == "SEARCH":
            await self._search(writer, session, tag, sub_rest, uid_mode=True)
        else:
            await _write_line(writer, f"{tag} BAD unsupported UID command")

    async def _dispatch_session_command(self, writer: asyncio.StreamWriter, session: ImapSession, tag: str, upper: str, rest: str) -> bool:
        if upper == "CAPABILITY":
            await _write_line(writer, "* CAPABILITY IMAP4rev1 UIDPLUS LITERAL+")
            await _write_line(writer, f"{tag} OK CAPABILITY completed")
            return False
        if upper == "NOOP":
            await _write_line(writer, f"{tag} OK NOOP completed")
            return False
        if upper == "LOGOUT":
            await _write_line(writer, "* BYE Gmeow logging out")
            await _write_line(writer, f"{tag} OK LOGOUT completed")
            return True
        username, password = _login_args(rest)
        if username == self.config.imap.username and password == self.password:
            session.authenticated = True
            await _write_line(writer, f"{tag} OK LOGIN completed")
        else:
            await _write_line(writer, f"{tag} NO authentication failed")
        return False

    async def _dispatch_mailbox_command(self, writer: asyncio.StreamWriter, session: ImapSession, tag: str, upper: str, rest: str) -> None:
        if upper in {"LIST", "LSUB"}:
            for mailbox in self.cache.imap_mailboxes():
                await _write_line(writer, f'* LIST (\\HasNoChildren) "/" "{_quote(mailbox["name"])}"')
            await _write_line(writer, f"{tag} OK {upper} completed")
            return
        if upper in {"SELECT", "EXAMINE"}:
            await self._select_mailbox(writer, session, tag, upper, rest)
            return
        await self._status_mailbox(writer, tag, rest)

    async def _select_mailbox(self, writer: asyncio.StreamWriter, session: ImapSession, tag: str, upper: str, rest: str) -> None:
        name = _unquote(rest.strip() or "INBOX")
        mailbox = self.cache.imap_mailbox(name)
        if not mailbox:
            await _write_line(writer, f"{tag} NO no such mailbox")
            return
        session.selected = mailbox["name"]
        await _write_line(writer, "* FLAGS (\\Seen \\Flagged \\Answered \\Deleted \\Draft)")
        await _write_line(writer, "* OK [PERMANENTFLAGS ()] Read-only mailbox")
        await _write_line(writer, f"* {int(mailbox['messages'])} EXISTS")
        await _write_line(writer, f"* OK [UIDVALIDITY {int(mailbox['uidvalidity'])}] UIDs valid")
        await _write_line(writer, f"* OK [UIDNEXT {int(mailbox['uidnext'])}] Predicted next UID")
        await _write_line(writer, f"{tag} OK [{upper}-READ-ONLY] {upper} completed")

    async def _status_mailbox(self, writer: asyncio.StreamWriter, tag: str, rest: str) -> None:
        name = _unquote((rest.split("(", 1)[0] or "").strip())
        mailbox = self.cache.imap_mailbox(name)
        if not mailbox:
            await _write_line(writer, f"{tag} NO no such mailbox")
            return
        await _write_line(
            writer,
            f'* STATUS "{_quote(mailbox["name"])}" '
            f"(MESSAGES {int(mailbox['messages'])} UIDNEXT {int(mailbox['uidnext'])} "
            f"UIDVALIDITY {int(mailbox['uidvalidity'])})",
        )
        await _write_line(writer, f"{tag} OK STATUS completed")

    async def _search(self, writer: asyncio.StreamWriter, session: ImapSession, tag: str, rest: str, *, uid_mode: bool) -> None:
        if not session.selected:
            await _write_line(writer, f"{tag} NO select a mailbox first")
            return
        messages = self.cache.imap_messages(session.selected)
        if "ALL" not in rest.upper() and rest.strip():
            messages = _filter_search(messages, rest)
        values = [str(message["uid"] if uid_mode else index + 1) for index, message in enumerate(messages)]
        await _write_line(writer, "* SEARCH " + " ".join(values))
        await _write_line(writer, f"{tag} OK SEARCH completed")

    async def _fetch(self, writer: asyncio.StreamWriter, session: ImapSession, tag: str, rest: str, *, uid_mode: bool) -> None:
        if not session.selected:
            await _write_line(writer, f"{tag} NO select a mailbox first")
            return
        sequence, _, _items = rest.partition(" ")
        messages = self.cache.imap_messages(session.selected)
        selected = _select_messages(messages, sequence, uid_mode=uid_mode)
        for seq, message in selected:
            content = self.cache.imap_message_bytes(session.selected, int(message["uid"]))
            flags = "(\\Seen)" if "UNREAD" not in self.cache.get_message(message["id"]).get("label_ids", []) else "()"
            prefix = f"* {seq} FETCH (UID {int(message['uid'])} FLAGS {flags} RFC822 {{{len(content)}}}\r\n"
            writer.write(prefix.encode("utf-8") + content + b"\r\n)\r\n")
            await writer.drain()
        await _write_line(writer, f"{tag} OK FETCH completed")


def run_imap_server(config: GmeowConfig) -> None:
    """Run imap server."""
    try:
        asyncio.run(ReadOnlyImapServer(config).serve())
    except KeyboardInterrupt as exc:
        raise SystemExit(130) from exc


def _parse_command(line: str) -> tuple[str, str, str]:
    parts = line.split(" ", 2)
    tag = parts[0] if parts else "*"
    command = parts[1] if len(parts) >= MIN_COMMAND_PARTS else ""
    rest = parts[2] if len(parts) > MIN_COMMAND_PARTS else ""
    return tag, command, rest


async def _write_line(writer: asyncio.StreamWriter, line: str) -> None:
    writer.write((line + "\r\n").encode("utf-8"))
    await writer.drain()


def _read_password(config: GmeowConfig) -> str:
    password = config.imap_password()
    if not password:
        msg = "IMAP password is not configured in SOPS secrets or the configured password file."
        raise RuntimeError(msg)
    return password


def _quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _unquote(value: str) -> str:
    value = value.strip()
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return value.replace('\\"', '"').replace("\\\\", "\\")


def _login_args(rest: str) -> tuple[str, str]:
    matches = re.findall(r'"([^"]*)"|(\S+)', rest)
    values = [quoted or bare for quoted, bare in matches]
    return (values[0] if values else "", values[1] if len(values) > 1 else "")


def _select_messages(messages: list[dict[str, Any]], sequence: str, *, uid_mode: bool) -> list[tuple[int, dict[str, Any]]]:
    if not sequence or sequence == "*":
        return [(len(messages), messages[-1])] if messages else []
    selected: list[tuple[int, dict[str, Any]]] = []
    wanted: set[int] | None = None
    if sequence.upper() == "ALL" or sequence == "1:*":
        wanted = None
    elif ":" in sequence:
        start_s, end_s = sequence.split(":", 1)
        start = int(start_s or "1")
        end = len(messages) if end_s == "*" else int(end_s)
        wanted = set(range(start, end + 1))
    else:
        wanted = {int(sequence)}
    for index, message in enumerate(messages, start=1):
        key = int(message["uid"]) if uid_mode else index
        if wanted is None or key in wanted:
            selected.append((index, message))
    return selected


def _filter_search(messages: list[dict[str, Any]], rest: str) -> list[dict[str, Any]]:
    match = re.search(r'(?:TEXT|SUBJECT|FROM|TO)\s+"?([^"]+)"?', rest, re.IGNORECASE)
    if not match:
        return messages
    needle = match.group(1).lower()
    return [
        message
        for message in messages
        if needle in " ".join(str(message.get(key) or "") for key in ["subject", "sender", "recipients"]).lower()
    ]
