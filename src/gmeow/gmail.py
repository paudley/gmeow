# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide gmail functionality for Gmeow."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any, Protocol

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from .config import GMAIL_MODIFY_SCOPE


class GmailClient(Protocol):
    """Represent GmailClient data and behavior."""

    def list_labels(self) -> list[dict[str, Any]]:
        """List labels."""
        ...

    def search_messages(self, query: str, limit: int = 100) -> list[str]:
        """Search messages."""
        ...

    def list_history(self, start_history_id: str, label_id: str | None = None, limit: int = 500) -> dict[str, Any]:
        """List history."""
        ...

    def get_message(self, message_id: str, fmt: str = "full") -> dict[str, Any]:
        """Get message."""
        ...

    def get_thread(self, thread_id: str) -> dict[str, Any]:
        """Get thread."""
        ...

    def get_attachment(self, message_id: str, attachment_id: str) -> bytes:
        """Get attachment."""
        ...

    def modify_message(
        self, message_id: str, add_label_ids: list[str] | None = None, remove_label_ids: list[str] | None = None
    ) -> dict[str, Any]:
        """Modify message."""
        ...


class GoogleGmailClient:
    """Represent GoogleGmailClient data and behavior."""

    def __init__(self, service_account_file: Path | None, subject: str, service_account_info: dict[str, Any] | None = None) -> None:
        """Initialize GoogleGmailClient."""
        if not subject:
            msg = "A delegated Workspace subject is required."
            raise ValueError(msg)
        if service_account_info is not None:
            credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=[GMAIL_MODIFY_SCOPE])
        elif service_account_file is not None:
            credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=[GMAIL_MODIFY_SCOPE])
        else:
            msg = "A service-account file or SOPS service_account_json is required."
            raise ValueError(msg)
        credentials = credentials.with_subject(subject)
        self.service = build("gmail", "v1", credentials=credentials, cache_discovery=False)
        self.user_id = "me"

    def list_labels(self) -> list[dict[str, Any]]:
        """List labels."""
        response = self.service.users().labels().list(userId=self.user_id).execute()
        return list(response.get("labels", []))

    def search_messages(self, query: str, limit: int = 100) -> list[str]:
        """Search messages."""
        ids: list[str] = []
        request = self.service.users().messages().list(userId=self.user_id, q=query, maxResults=min(limit, 500))
        while request is not None and len(ids) < limit:
            response = request.execute()
            ids.extend(item["id"] for item in response.get("messages", []))
            request = self.service.users().messages().list_next(request, response)
        return ids[:limit]

    def list_history(self, start_history_id: str, label_id: str | None = None, limit: int = 500) -> dict[str, Any]:
        """List history."""
        records: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {
            "userId": self.user_id,
            "startHistoryId": start_history_id,
            "maxResults": min(limit, 500),
            "historyTypes": ["messageAdded", "messageDeleted", "labelAdded", "labelRemoved"],
        }
        if label_id:
            kwargs["labelId"] = label_id
        page_token: str | None = None
        history_id: str | None = None
        while len(records) < limit:
            request_kwargs = dict(kwargs)
            if page_token:
                request_kwargs["pageToken"] = page_token
            response = self.service.users().history().list(**request_kwargs).execute()
            records.extend(response.get("history", []))
            history_id = response.get("historyId") or history_id
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return {"history": records[:limit], "history_id": history_id, "truncated": len(records) > limit}

    def get_message(self, message_id: str, fmt: str = "full") -> dict[str, Any]:
        """Get message."""
        return self.service.users().messages().get(userId=self.user_id, id=message_id, format=fmt).execute()

    def get_thread(self, thread_id: str) -> dict[str, Any]:
        """Get thread."""
        return self.service.users().threads().get(userId=self.user_id, id=thread_id, format="full").execute()

    def get_attachment(self, message_id: str, attachment_id: str) -> bytes:
        """Get attachment."""
        response = (
            self.service.users()
            .messages()
            .attachments()
            .get(
                userId=self.user_id,
                messageId=message_id,
                id=attachment_id,
            )
            .execute()
        )
        data = response.get("data", "")
        padding = "=" * (-len(data) % 4)
        return base64.urlsafe_b64decode(data + padding)

    def modify_message(
        self, message_id: str, add_label_ids: list[str] | None = None, remove_label_ids: list[str] | None = None
    ) -> dict[str, Any]:
        """Modify message."""
        body = {
            "addLabelIds": add_label_ids or [],
            "removeLabelIds": remove_label_ids or [],
        }
        return self.service.users().messages().modify(userId=self.user_id, id=message_id, body=body).execute()


class UserOAuthGmailClient(GoogleGmailClient):
    """Represent UserOAuthGmailClient data and behavior."""

    def __init__(self, credentials_file: Path | None = None, credentials_info: dict[str, Any] | None = None) -> None:
        """Initialize UserOAuthGmailClient."""
        if credentials_info is not None:
            credentials = Credentials.from_authorized_user_info(credentials_info, scopes=[GMAIL_MODIFY_SCOPE])
        elif credentials_file is not None:
            credentials = Credentials.from_authorized_user_file(credentials_file, scopes=[GMAIL_MODIFY_SCOPE])
        else:
            msg = "A user OAuth credentials file or SOPS user_credentials_json is required."
            raise ValueError(msg)
        self.service = build("gmail", "v1", credentials=credentials, cache_discovery=False)
        self.user_id = "me"
