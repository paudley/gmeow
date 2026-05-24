# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import tomllib


GMAIL_MODIFY_SCOPE = "https://www.googleapis.com/auth/gmail.modify"


@dataclass(slots=True)
class PriorityRule:
    name: str
    gmail_query: str | None = None
    labels: list[str] = field(default_factory=list)
    from_domains: list[str] = field(default_factory=list)
    senders: list[str] = field(default_factory=list)
    recipients: list[str] = field(default_factory=list)
    header_contains: dict[str, str] = field(default_factory=dict)
    attachment_mime: list[str] = field(default_factory=list)
    attachment_filename_contains: list[str] = field(default_factory=list)
    newer_than_days: int | None = None
    priority: int = 100

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PriorityRule":
        return cls(
            name=str(data["name"]),
            gmail_query=data.get("gmail_query") or data.get("query"),
            labels=list(data.get("labels", [])),
            from_domains=list(data.get("from_domains", [])),
            senders=list(data.get("senders", [])),
            recipients=list(data.get("recipients", [])),
            header_contains=dict(data.get("header_contains", {})),
            attachment_mime=list(data.get("attachment_mime", [])),
            attachment_filename_contains=list(data.get("attachment_filename_contains", [])),
            newer_than_days=data.get("newer_than_days"),
            priority=int(data.get("priority", 100)),
        )

    def to_gmail_query(self) -> str:
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
    enabled: bool = True
    sync_history_seconds: int | None = 300
    sync_history_limit: int = 500
    sync_priority_seconds: int | None = 3600
    sync_priority_limit_per_rule: int = 100
    intelligence_seconds: int | None = 30
    intelligence_limit: int = 25
    derived_refresh_seconds: int | None = 900
    analyze_seconds: int | None = 3600
    attachment_sidecars_seconds: int | None = None
    run_on_startup: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "MaintenanceConfig":
        raw = data or {}
        return cls(
            enabled=bool(raw.get("enabled", True)),
            sync_history_seconds=_optional_int(raw.get("sync_history_seconds", 300)),
            sync_history_limit=int(raw.get("sync_history_limit", 500)),
            sync_priority_seconds=_optional_int(raw.get("sync_priority_seconds", 3600)),
            sync_priority_limit_per_rule=int(raw.get("sync_priority_limit_per_rule", 100)),
            intelligence_seconds=_optional_int(raw.get("intelligence_seconds", 30)),
            intelligence_limit=int(raw.get("intelligence_limit", 25)),
            derived_refresh_seconds=_optional_int(raw.get("derived_refresh_seconds", 900)),
            analyze_seconds=_optional_int(raw.get("analyze_seconds", 3600)),
            attachment_sidecars_seconds=_optional_int(raw.get("attachment_sidecars_seconds")),
            run_on_startup=bool(raw.get("run_on_startup", False)),
        )


@dataclass(slots=True)
class AttachmentAnalysisConfig:
    enabled: bool = True
    max_text_chars: int = 200_000
    pdf_text_enabled: bool = True
    ocr_enabled: bool = True
    ocr_languages: str = "eng"
    archive_listing_enabled: bool = True
    pandoc_enabled: bool = True
    vision_caption_enabled: bool = False
    vision_caption_endpoint: str | None = None
    vision_caption_model: str | None = None
    vision_caption_timeout_seconds: int = 60

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "AttachmentAnalysisConfig":
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
            vision_caption_endpoint=raw.get("vision_caption_endpoint"),
            vision_caption_model=raw.get("vision_caption_model"),
            vision_caption_timeout_seconds=int(raw.get("vision_caption_timeout_seconds", 60)),
        )


@dataclass(slots=True)
class ImapConfig:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 1143
    username: str = "gmeow"
    password_file: Path = Path("data/secrets/imap-password")

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None, data_dir: Path) -> "ImapConfig":
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
    require_rfc822: bool = True
    delete_policy_default: str = "tombstone"
    purge_labels: list[str] = field(default_factory=lambda: ["SPAM", "TRASH"])
    purge_categories: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ArchiveConfig":
        raw = data or {}
        delete_policy = raw.get("delete_policy") or {}
        return cls(
            require_rfc822=bool(raw.get("require_rfc822", True)),
            delete_policy_default=str(delete_policy.get("default", raw.get("delete_policy_default", "tombstone"))),
            purge_labels=list(delete_policy.get("purge_labels", raw.get("purge_labels", ["SPAM", "TRASH"]))),
            purge_categories=list(delete_policy.get("purge_categories", raw.get("purge_categories", []))),
        )


@dataclass(slots=True)
class SopsSecretsConfig:
    file: Path | None = None
    unlock_key: str | None = None
    age_key: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "SopsSecretsConfig":
        raw = data or {}
        file_value = raw.get("file") or raw.get("sops_file")
        return cls(
            file=Path(file_value).expanduser() if file_value else None,
            unlock_key=raw.get("unlock_key"),
            age_key=raw.get("age_key"),
        )


@dataclass(slots=True)
class GmeowConfig:
    data_dir: Path = Path("data")
    host: str = "127.0.0.1"
    port: int = 8765
    postgres_dsn: str = "postgresql://gmeow:gmeow@127.0.0.1:5432/gmeow?sslmode=require"
    auth_mode: str = "service_account"
    subject: str | None = None
    service_account_file: Path = Path("data/secrets/service-account.json")
    user_credentials_file: Path = Path.home() / ".config/gcloud/application_default_credentials.json"
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    embedding_endpoint: str = "http://127.0.0.1:8090/v1/embeddings"
    semantic_chunk_size: int = 192
    semantic_chunk_overlap: int = 32
    priority_rules: list[PriorityRule] = field(default_factory=list)
    maintenance: MaintenanceConfig = field(default_factory=MaintenanceConfig)
    attachment_analysis: AttachmentAnalysisConfig = field(default_factory=AttachmentAnalysisConfig)
    imap: ImapConfig = field(default_factory=ImapConfig)
    archive: ArchiveConfig = field(default_factory=ArchiveConfig)
    secrets: SopsSecretsConfig = field(default_factory=SopsSecretsConfig)
    _decrypted_secrets: dict[str, Any] | None = field(default=None, init=False, repr=False)

    @property
    def object_store_dir(self) -> Path:
        return self.data_dir / "objects"

    @property
    def tantivy_dir(self) -> Path:
        return self.data_dir / "tantivy"

    @classmethod
    def load(cls, path: Path | str = "config.toml") -> "GmeowConfig":
        config_path = Path(path)
        if not config_path.exists():
            return cls()
        parsed = tomllib.loads(config_path.read_text()) or {}
        raw = parsed.get("gmeow")
        if not isinstance(raw, dict):
            raise ValueError(f"{config_path} must contain a [gmeow] table.")
        data_dir = Path(raw.get("data_dir", "data"))
        service_account_file = Path(raw.get("service_account_file", data_dir / "secrets" / "service-account.json")).expanduser()
        rules = [PriorityRule.from_dict(rule) for rule in raw.get("priority_rules", [])]
        return cls(
            data_dir=data_dir,
            host=str(raw.get("host", "127.0.0.1")),
            port=int(raw.get("port", 8765)),
            postgres_dsn=str(raw.get("postgres_dsn", "postgresql://gmeow:gmeow@127.0.0.1:5432/gmeow?sslmode=require")),
            auth_mode=str(raw.get("auth_mode", "service_account")),
            subject=raw.get("subject"),
            service_account_file=service_account_file,
            user_credentials_file=Path(raw.get("user_credentials_file", Path.home() / ".config/gcloud/application_default_credentials.json")).expanduser(),
            embedding_model=str(raw.get("embedding_model", "sentence-transformers/all-MiniLM-L6-v2")),
            embedding_endpoint=str(raw.get("embedding_endpoint", "http://127.0.0.1:8090/v1/embeddings")),
            semantic_chunk_size=int(raw.get("semantic_chunk_size", 192)),
            semantic_chunk_overlap=int(raw.get("semantic_chunk_overlap", 32)),
            priority_rules=rules,
            maintenance=MaintenanceConfig.from_dict(raw.get("maintenance")),
            attachment_analysis=AttachmentAnalysisConfig.from_dict(raw.get("attachment_analysis")),
            imap=ImapConfig.from_dict(raw.get("imap"), data_dir),
            archive=ArchiveConfig.from_dict(raw.get("archive")),
            secrets=SopsSecretsConfig.from_dict(raw.get("secrets")),
        )

    def ensure_dirs(self) -> None:
        for path in [self.object_store_dir, self.tantivy_dir, self.service_account_file.parent, self.imap.password_file.parent]:
            path.mkdir(parents=True, exist_ok=True)

    def load_secrets(self) -> dict[str, Any]:
        if self._decrypted_secrets is not None:
            return self._decrypted_secrets
        if not self.secrets.file:
            self._decrypted_secrets = {}
            return self._decrypted_secrets
        if not self.secrets.file.exists():
            raise RuntimeError(f"Configured SOPS secrets file does not exist: {self.secrets.file}")
        self._decrypted_secrets = self._decrypt_sops_secrets()
        return self._decrypted_secrets

    def service_account_info(self) -> dict[str, Any] | None:
        value = self.load_secrets().get("service_account_json")
        return value if isinstance(value, dict) else None

    def user_credentials_info(self) -> dict[str, Any] | None:
        value = self.load_secrets().get("user_credentials_json")
        return value if isinstance(value, dict) else None

    def database_dsn(self) -> str:
        value = self.load_secrets().get("postgres_dsn")
        return value if isinstance(value, str) and value else self.postgres_dsn

    def imap_password(self) -> str | None:
        value = self.load_secrets().get("imap_password")
        if isinstance(value, str) and value:
            return value.strip()
        if self.imap.password_file.exists():
            return self.imap.password_file.read_text().strip()
        return None

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
            raise RuntimeError("sops is required to decrypt configured secrets.") from exc
        except subprocess.CalledProcessError as exc:
            detail = exc.stderr.strip() if exc.stderr else str(exc)
            raise RuntimeError(f"Unable to decrypt configured SOPS secrets file: {detail}") from exc
        decoded = json.loads(output or "{}")
        if not isinstance(decoded, dict):
            raise RuntimeError("Configured SOPS secrets file must decrypt to a mapping.")
        return decoded


def _optional_int(value: Any) -> int | None:
    if value is None or value is False:
        return None
    number = int(value)
    return number if number > 0 else None
