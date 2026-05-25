# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Runtime-only configuration records for transitional Python services.

The Go Phase 00 config parser is the only config/SOPS bootstrap path. This module remains only so
later-phase Python runtime code and tests can receive already-resolved settings from callers.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self, cast

DEFAULT_INT = cast(int, None)
DEFAULT_PATH = cast(Path, None)
DEFAULT_STR = cast(str, None)

GMAIL_MODIFY_SCOPE = "https://www.googleapis.com/auth/gmail.modify"
DEFAULT_PURGE_LABELS = ["SPAM", "TRASH"]


def default_purge_labels() -> list[str]:
    """Return the default labels eligible for purge policy handling."""
    return list(DEFAULT_PURGE_LABELS)


def default_priority_rules() -> list["PriorityRule"]:
    """Return an empty priority rule list."""
    return []


def default_string_list() -> list[str]:
    """Return an empty string list."""
    return []


def default_string_dict() -> dict[str, str]:
    """Return an empty string mapping."""
    return {}


def default_secrets() -> dict[str, Any]:
    """Return an empty decrypted secrets mapping."""
    return {}


@dataclass(slots=True)
class PriorityRule:
    """Represent PriorityRule data and behavior."""

    name: str
    gmail_query: str = DEFAULT_STR
    labels: list[str] = field(default_factory=default_string_list)
    from_domains: list[str] = field(default_factory=default_string_list)
    senders: list[str] = field(default_factory=default_string_list)
    recipients: list[str] = field(default_factory=default_string_list)
    header_contains: dict[str, str] = field(default_factory=default_string_dict)
    attachment_mime: list[str] = field(default_factory=default_string_list)
    attachment_filename_contains: list[str] = field(default_factory=default_string_list)
    newer_than_days: int = DEFAULT_INT
    priority: int = 100

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """From dict."""
        return cls(
            name=str(data["name"]),
            gmail_query=str(data.get("gmail_query") or data.get("query") or ""),
            labels=_string_list(data.get("labels", [])),
            from_domains=_string_list(data.get("from_domains", [])),
            senders=_string_list(data.get("senders", [])),
            recipients=_string_list(data.get("recipients", [])),
            header_contains=_string_dict(data.get("header_contains", {})),
            attachment_mime=_string_list(data.get("attachment_mime", [])),
            attachment_filename_contains=_string_list(data.get("attachment_filename_contains", [])),
            newer_than_days=_optional_int(data.get("newer_than_days")) or DEFAULT_INT,
            priority=int(data.get("priority", 100)),
        )

    def to_gmail_query(self) -> str:
        """To gmail query."""
        parts: list[str] = []
        if self.gmail_query:
            parts.append(f"({self.gmail_query})")
        parts.extend(f"label:{label}" for label in self.labels)
        parts.extend(f"from:{sender}" for sender in self.senders)
        parts.extend(f"from:{domain}" for domain in self.from_domains)
        parts.extend(f"to:{recipient}" for recipient in self.recipients)
        if self.newer_than_days:
            parts.append(f"newer_than:{self.newer_than_days}d")
        return " ".join(parts).strip()


@dataclass(slots=True)
class MaintenanceConfig:
    """Represent MaintenanceConfig data and behavior."""

    enabled: bool = True
    sync_history_seconds: int = 300
    sync_history_limit: int = 500
    sync_priority_seconds: int = 3600
    sync_priority_limit_per_rule: int = 100
    intelligence_seconds: int = 30
    intelligence_limit: int = 25
    attachment_hydration_seconds: int = 30
    attachment_hydration_limit: int = 10
    backfill_enabled: bool = False
    backfill_seconds: int = 5
    backfill_batch_size: int = 50
    backfill_max_empty_windows: int = 120
    derived_refresh_seconds: int = 900
    analyze_seconds: int = 3600
    attachment_sidecars_seconds: int = DEFAULT_INT
    run_on_startup: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """From dict."""
        raw = data or {}
        return cls(
            enabled=bool(raw.get("enabled", True)),
            sync_history_seconds=_interval_from_raw(raw, "sync_history_seconds", 300),
            sync_history_limit=int(raw.get("sync_history_limit", 500)),
            sync_priority_seconds=_interval_from_raw(raw, "sync_priority_seconds", 3600),
            sync_priority_limit_per_rule=int(raw.get("sync_priority_limit_per_rule", 100)),
            intelligence_seconds=_interval_from_raw(raw, "intelligence_seconds", 30),
            intelligence_limit=int(raw.get("intelligence_limit", 25)),
            attachment_hydration_seconds=_interval_from_raw(raw, "attachment_hydration_seconds", 30),
            attachment_hydration_limit=int(raw.get("attachment_hydration_limit", 10)),
            backfill_enabled=bool(raw.get("backfill_enabled", False)),
            backfill_seconds=_interval_from_raw(raw, "backfill_seconds", 5),
            backfill_batch_size=int(raw.get("backfill_batch_size", 50)),
            backfill_max_empty_windows=int(raw.get("backfill_max_empty_windows", 120)),
            derived_refresh_seconds=_interval_from_raw(raw, "derived_refresh_seconds", 900),
            analyze_seconds=_interval_from_raw(raw, "analyze_seconds", 3600),
            attachment_sidecars_seconds=_interval_from_raw(raw, "attachment_sidecars_seconds", DEFAULT_INT),
            run_on_startup=bool(raw.get("run_on_startup", False)),
        )


@dataclass(slots=True)
class AttachmentAnalysisConfig:
    """Represent AttachmentAnalysisConfig data and behavior."""

    enabled: bool = True
    max_text_chars: int = 200_000
    pdf_text_enabled: bool = True
    ocr_enabled: bool = True
    ocr_languages: str = "eng"
    archive_listing_enabled: bool = True
    pandoc_enabled: bool = True
    vision_caption_enabled: bool = False
    vision_caption_endpoint: str = DEFAULT_STR
    vision_caption_model: str = DEFAULT_STR
    vision_caption_timeout_seconds: int = 60

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """From dict."""
        raw = data or {}
        return cls(
            enabled=bool(raw.get("enabled", True)),
            max_text_chars=int(raw.get("max_text_chars", 200_000)),
            pdf_text_enabled=bool(raw.get("pdf_text_enabled", True)),
            ocr_enabled=bool(raw.get("ocr_enabled", True)),
            ocr_languages=str(raw.get("ocr_languages", "eng")),
            archive_listing_enabled=bool(raw.get("archive_listing_enabled", True)),
            pandoc_enabled=bool(raw.get("pandoc_enabled", True)),
            vision_caption_enabled=bool(raw.get("vision_caption_enabled", False)),
            vision_caption_endpoint=str(raw.get("vision_caption_endpoint") or ""),
            vision_caption_model=str(raw.get("vision_caption_model") or ""),
            vision_caption_timeout_seconds=int(raw.get("vision_caption_timeout_seconds", 60)),
        )


@dataclass(slots=True)
class ImapConfig:
    """Represent ImapConfig data and behavior."""

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 1143
    username: str = "gmeow"
    password_file: Path = Path("data/secrets/imap-password")

    @classmethod
    def from_dict(cls, data: dict[str, Any], data_dir: Path) -> Self:
        """From dict."""
        raw = data or {}
        return cls(
            enabled=bool(raw.get("enabled", False)),
            host=str(raw.get("host", "127.0.0.1")),
            port=int(raw.get("port", 1143)),
            username=str(raw.get("username", "gmeow")),
            password_file=Path(raw.get("password_file", data_dir / "secrets" / "imap-password")).expanduser(),
        )


@dataclass(slots=True)
class ArchiveConfig:
    """Represent ArchiveConfig data and behavior."""

    require_rfc822: bool = True
    delete_policy_default: str = "tombstone"
    purge_labels: list[str] = field(default_factory=default_purge_labels)
    purge_categories: list[str] = field(default_factory=default_string_list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """From dict."""
        raw = data or {}
        delete_policy = _string_object_dict(raw.get("delete_policy", {}))
        return cls(
            require_rfc822=bool(raw.get("require_rfc822", True)),
            delete_policy_default=str(delete_policy.get("default", raw.get("delete_policy_default", "tombstone"))),
            purge_labels=_string_list(delete_policy.get("purge_labels", raw.get("purge_labels", ["SPAM", "TRASH"]))),
            purge_categories=_string_list(delete_policy.get("purge_categories", raw.get("purge_categories", []))),
        )


@dataclass(slots=True)
class RuntimeConfig:
    """Represent resolved Python runtime settings."""

    data_dir: Path = Path("data")
    host: str = "127.0.0.1"
    port: int = 8765
    postgres_dsn: str = "postgresql://gmeow:gmeow@127.0.0.1:5432/gmeow?sslmode=require"
    auth_mode: str = "service_account"
    subject: str = DEFAULT_STR
    service_account_file: Path = Path("data/secrets/service-account.json")
    user_credentials_file: Path = Path.home() / ".config/gcloud/application_default_credentials.json"
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    embedding_endpoint: str = "http://127.0.0.1:8090/v1/embeddings"
    semantic_chunk_size: int = 192
    semantic_chunk_overlap: int = 32
    priority_rules: list[PriorityRule] = field(default_factory=default_priority_rules)
    maintenance: MaintenanceConfig = field(default_factory=MaintenanceConfig)
    attachment_analysis: AttachmentAnalysisConfig = field(default_factory=AttachmentAnalysisConfig)
    imap: ImapConfig = field(default_factory=ImapConfig)
    archive: ArchiveConfig = field(default_factory=ArchiveConfig)
    service_account_json: dict[str, Any] = field(default_factory=default_secrets)
    user_credentials_json: dict[str, Any] = field(default_factory=default_secrets)
    imap_password_value: str = DEFAULT_STR

    @property
    def object_store_dir(self) -> Path:
        """Object store dir."""
        return self.data_dir / "objects"

    @property
    def tantivy_dir(self) -> Path:
        """Tantivy dir."""
        return self.data_dir / "tantivy"

    def ensure_dirs(self) -> None:
        """Ensure dirs."""
        for path in [self.object_store_dir, self.tantivy_dir, self.service_account_file.parent, self.imap.password_file.parent]:
            path.mkdir(parents=True, exist_ok=True)

    def service_account_info(self) -> dict[str, Any]:
        """Service account info."""
        return self.service_account_json

    def user_credentials_info(self) -> dict[str, Any]:
        """User credentials info."""
        return self.user_credentials_json

    def database_dsn(self) -> str:
        """Database dsn."""
        return self.postgres_dsn

    def imap_password(self) -> str:
        """Imap password."""
        if self.imap_password_value:
            return self.imap_password_value.strip()
        if self.imap.password_file.exists():
            return self.imap.password_file.read_text().strip()
        return ""


def _optional_int(value: object) -> int:
    if value is None or value is False:
        return 0
    if isinstance(value, int):
        return max(value, 0)
    if isinstance(value, float):
        return max(int(value), 0)
    if isinstance(value, str) and value:
        return max(int(value), 0)
    msg = f"Expected integer-compatible value, got {type(value).__name__}"
    raise TypeError(msg)


def _interval_from_raw(raw: dict[str, Any], key: str, default: int) -> int:
    value = raw[key] if key in raw else default
    if value is None or value is False:
        return DEFAULT_INT
    interval = _optional_int(value)
    return interval if interval > 0 else DEFAULT_INT


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in cast(list[object], value)]


def _string_dict(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(key): str(item) for key, item in cast(dict[object, object], value).items()}


def _string_object_dict(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in cast(dict[object, Any], value).items()}

