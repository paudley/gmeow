# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Load and normalize Gmeow configuration.

The module defines typed configuration records for Gmail, maintenance, storage, analysis, and server
settings. It keeps TOML parsing and default behavior in one place so runtime services receive
consistent values.
"""

import json
import os
import subprocess
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self, cast

from ._typing import ensure_dict

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
class SopsSecretsConfig:
    """Represent SopsSecretsConfig data and behavior."""

    file: Path = DEFAULT_PATH
    unlock_key: str = DEFAULT_STR
    age_key: str = DEFAULT_STR

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """From dict."""
        raw = data or {}
        file_value = raw.get("file") or raw.get("sops_file")
        return cls(
            file=Path(file_value).expanduser() if file_value else DEFAULT_PATH,
            unlock_key=str(raw.get("unlock_key") or ""),
            age_key=str(raw.get("age_key") or ""),
        )


@dataclass(slots=True)
class GmeowConfig:
    """Represent GmeowConfig data and behavior."""

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
    secrets: SopsSecretsConfig = field(default_factory=SopsSecretsConfig)
    _decrypted_secrets: dict[str, Any] = field(default_factory=default_secrets, init=False, repr=False)

    @property
    def object_store_dir(self) -> Path:
        """Object store dir."""
        return self.data_dir / "objects"

    @property
    def tantivy_dir(self) -> Path:
        """Tantivy dir."""
        return self.data_dir / "tantivy"

    @classmethod
    def load(cls, path: Path | str = "config.toml") -> Self:
        """Load."""
        config_path = Path(path)
        if not config_path.exists():
            return cls()
        parsed = _string_object_dict(tomllib.loads(config_path.read_text()))
        raw = parsed.get("gmeow")
        if not isinstance(raw, dict):
            msg = f"{config_path} must contain a [gmeow] table."
            raise TypeError(msg)
        raw = cast(dict[str, Any], raw)
        data_dir = Path(raw.get("data_dir", "data"))
        service_account_file = Path(raw.get("service_account_file", data_dir / "secrets" / "service-account.json")).expanduser()
        rules = [PriorityRule.from_dict(rule) for rule in _dict_list(raw.get("priority_rules", []))]
        return cls(
            data_dir=data_dir,
            host=str(raw.get("host", "127.0.0.1")),
            port=int(raw.get("port", 8765)),
            postgres_dsn=str(raw.get("postgres_dsn", "postgresql://gmeow:gmeow@127.0.0.1:5432/gmeow?sslmode=require")),
            auth_mode=str(raw.get("auth_mode", "service_account")),
            subject=str(raw.get("subject") or ""),
            service_account_file=service_account_file,
            user_credentials_file=Path(
                raw.get("user_credentials_file", Path.home() / ".config/gcloud/application_default_credentials.json")
            ).expanduser(),
            embedding_model=str(raw.get("embedding_model", "sentence-transformers/all-MiniLM-L6-v2")),
            embedding_endpoint=str(raw.get("embedding_endpoint", "http://127.0.0.1:8090/v1/embeddings")),
            semantic_chunk_size=int(raw.get("semantic_chunk_size", 192)),
            semantic_chunk_overlap=int(raw.get("semantic_chunk_overlap", 32)),
            priority_rules=rules,
            maintenance=MaintenanceConfig.from_dict(_string_object_dict(raw.get("maintenance", {}))),
            attachment_analysis=AttachmentAnalysisConfig.from_dict(_string_object_dict(raw.get("attachment_analysis", {}))),
            imap=ImapConfig.from_dict(_string_object_dict(raw.get("imap", {})), data_dir),
            archive=ArchiveConfig.from_dict(_string_object_dict(raw.get("archive", {}))),
            secrets=SopsSecretsConfig.from_dict(_string_object_dict(raw.get("secrets", {}))),
        )

    def ensure_dirs(self) -> None:
        """Ensure dirs."""
        for path in [self.object_store_dir, self.tantivy_dir, self.service_account_file.parent, self.imap.password_file.parent]:
            path.mkdir(parents=True, exist_ok=True)

    def load_secrets(self) -> dict[str, Any]:
        """Load secrets."""
        if self._decrypted_secrets:
            return self._decrypted_secrets
        if not self.secrets.file:
            self._decrypted_secrets = {}
            return self._decrypted_secrets
        if not self.secrets.file.exists():
            msg = f"Configured SOPS secrets file does not exist: {self.secrets.file}"
            raise RuntimeError(msg)
        self._decrypted_secrets = self._decrypt_sops_secrets()
        return self._decrypted_secrets

    def service_account_info(self) -> dict[str, Any]:
        """Service account info."""
        return ensure_dict(self.load_secrets(), "service_account_json")

    def user_credentials_info(self) -> dict[str, Any]:
        """User credentials info."""
        return ensure_dict(self.load_secrets(), "user_credentials_json")

    def database_dsn(self) -> str:
        """Database dsn."""
        value = self.load_secrets().get("postgres_dsn")
        return value if isinstance(value, str) and value else self.postgres_dsn

    def imap_password(self) -> str:
        """Imap password."""
        value = self.load_secrets().get("imap_password")
        if isinstance(value, str) and value:
            return value.strip()
        if self.imap.password_file.exists():
            return self.imap.password_file.read_text().strip()
        return ""

    def _decrypt_sops_secrets(self) -> dict[str, Any]:
        if not self.secrets.file:
            return {}
        env = os.environ.copy()
        if self.secrets.age_key:
            env["SOPS_AGE_KEY"] = self.secrets.age_key
        if self.secrets.unlock_key:
            env["GMEOW_SOPS_UNLOCK_KEY"] = self.secrets.unlock_key
        try:
            output = subprocess.check_output(
                ["sops", "decrypt", "--input-type", "yaml", "--output-type", "json", str(self.secrets.file)],
                env=env,
                text=True,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            msg = "sops is required to decrypt configured secrets."
            raise RuntimeError(msg) from exc
        except subprocess.CalledProcessError as exc:
            detail = exc.stderr.strip() if exc.stderr else str(exc)
            msg = f"Unable to decrypt configured SOPS secrets file: {detail}"
            raise RuntimeError(msg) from exc
        decoded = json.loads(output or "{}")
        if not isinstance(decoded, dict):
            msg = "Configured SOPS secrets file must decrypt to a mapping."
            raise TypeError(msg)
        return cast(dict[str, Any], decoded)


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


def _dict_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, Any], item) for item in cast(list[object], value) if isinstance(item, dict)]
