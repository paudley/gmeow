# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Wrap Gmail API access for Gmeow.

The module defines the Gmail client protocol and concrete delegated-service-account and OAuth
clients. It centralizes Gmail search, hydration, raw RFC822 retrieval, history, and label mutation
calls.
"""

import base64
from pathlib import Path
from typing import Any, Protocol, Self, cast

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from .config import GMAIL_MODIFY_SCOPE

DEFAULT_LIST_STR = cast(list[str], None)
DEFAULT_STR = cast(str, None)


class _DelegatedCredentials(Protocol):
    """Credentials that can impersonate a Workspace subject."""

    def with_subject(self, subject: str) -> object:
        """Return credentials delegated to subject."""
        ...


class _GmailApiNode(Protocol):
    """Typed facade for googleapiclient's dynamic Gmail resource tree."""

    def users(self) -> "_GmailApiNode":
        """Return the users API node."""
        ...

    def labels(self) -> "_GmailApiNode":
        """Return the labels API node."""
        ...

    def messages(self) -> "_GmailApiNode":
        """Return the messages API node."""
        ...

    def threads(self) -> "_GmailApiNode":
        """Return the threads API node."""
        ...

    def history(self) -> "_GmailApiNode":
        """Return the history API node."""
        ...

    def attachments(self) -> "_GmailApiNode":
        """Return the attachments API node."""
        ...

    def list(self, **kwargs: object) -> "_GmailApiNode":
        """Create a list request node."""
        ...

    def get(self, **kwargs: object) -> "_GmailApiNode":
        """Create a get request node."""
        ...

    def modify(self, **kwargs: object) -> "_GmailApiNode":
        """Create a modify request node."""
        ...

    def execute(self) -> dict[str, Any]:
        """Execute the built request."""
        ...


def _gmail_service(credentials: object) -> _GmailApiNode:
    return cast(_GmailApiNode, build("gmail", "v1", credentials=credentials, cache_discovery=False))


SERVICE_ACCOUNT_CREDENTIALS = cast(Any, service_account.Credentials)
OAUTH_CREDENTIALS = cast(Any, Credentials)


class GmailClient(Protocol):
    """Represent GmailClient data and behavior."""

    def list_labels(self) -> list[dict[str, Any]]:
        """List labels."""
        ...

    def search_messages(self, query: str, limit: int = 100) -> list[str]:
        """Search messages."""
        ...

    def search_messages_page(self, query: str, page_token: str = DEFAULT_STR, page_size: int = 50) -> dict[str, Any]:
        """Search one page of messages."""
        ...

    def get_message_metadata(self, message_id: str) -> dict[str, Any]:
        """Get lightweight message metadata."""
        ...

    def list_history(self, start_history_id: str, label_id: str = DEFAULT_STR, limit: int = 500) -> dict[str, Any]:
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
        self, message_id: str, add_label_ids: list[str] = DEFAULT_LIST_STR, remove_label_ids: list[str] = DEFAULT_LIST_STR
    ) -> dict[str, Any]:
        """Modify message."""
        ...


class GoogleGmailClient:
    """Represent GoogleGmailClient data and behavior."""

    def __init__(self, credentials: _DelegatedCredentials, subject: str) -> None:
        """Initialize GoogleGmailClient."""
        if not subject:
            msg = "A delegated Workspace subject is required."
            raise ValueError(msg)
        self.service = _gmail_service(credentials.with_subject(subject))
        self.user_id = "me"

    @classmethod
    def from_service_account_info(cls, service_account_info: dict[str, Any], subject: str) -> Self:
        """Build a Gmail client from decrypted service-account JSON."""
        credentials = cast(
            _DelegatedCredentials,
            SERVICE_ACCOUNT_CREDENTIALS.from_service_account_info(service_account_info, scopes=[GMAIL_MODIFY_SCOPE]),
        )
        return cls(credentials, subject)

    @classmethod
    def from_service_account_file(cls, service_account_file: Path, subject: str) -> Self:
        """Build a Gmail client from a service-account file."""
        credentials = cast(
            _DelegatedCredentials,
            SERVICE_ACCOUNT_CREDENTIALS.from_service_account_file(service_account_file, scopes=[GMAIL_MODIFY_SCOPE]),
        )
        return cls(credentials, subject)

    def list_labels(self) -> list[dict[str, Any]]:
        """List labels."""
        response = self.service.users().labels().list(userId=self.user_id).execute()
        return list(response.get("labels") or [])

    def search_messages(self, query: str, limit: int = 100) -> list[str]:
        """Search messages."""
        ids: list[str] = []
        page_token: str = DEFAULT_STR
        while len(ids) < limit:
            response = self.search_messages_page(query, page_token=page_token, page_size=min(limit - len(ids), 500))
            ids.extend(item["id"] for item in response.get("messages", []))
            page_token = str(response.get("next_page_token") or "")
            if not page_token:
                break
        return ids[:limit]

    def search_messages_page(self, query: str, page_token: str = DEFAULT_STR, page_size: int = 50) -> dict[str, Any]:
        """Search one page of messages."""
        kwargs: dict[str, Any] = {"userId": self.user_id, "q": query, "maxResults": max(1, min(page_size, 500))}
        if page_token:
            kwargs["pageToken"] = page_token
        response = self.service.users().messages().list(**kwargs).execute()
        return {
            "messages": list(response.get("messages") or []),
            "next_page_token": response.get("nextPageToken") or "",
            "result_size_estimate": response.get("resultSizeEstimate"),
        }

    def get_message_metadata(self, message_id: str) -> dict[str, Any]:
        """Get lightweight message metadata."""
        return (
            self.service.users()
            .messages()
            .get(
                userId=self.user_id,
                id=message_id,
                format="metadata",
                metadataHeaders=["Date", "Subject", "From", "To", "Message-ID"],
            )
            .execute()
        )

    def list_history(self, start_history_id: str, label_id: str = DEFAULT_STR, limit: int = 500) -> dict[str, Any]:
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
        page_token: str = DEFAULT_STR
        history_id: str = DEFAULT_STR
        while len(records) < limit:
            request_kwargs = dict(kwargs)
            if page_token:
                request_kwargs["pageToken"] = page_token
            response = self.service.users().history().list(**request_kwargs).execute()
            records.extend(response.get("history") or [])
            history_id = str(response.get("historyId") or history_id)
            page_token = str(response.get("nextPageToken") or "")
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
        data = str(response.get("data") or "")
        padding = "=" * (-len(data) % 4)
        return base64.urlsafe_b64decode(data + padding)

    def modify_message(
        self, message_id: str, add_label_ids: list[str] = DEFAULT_LIST_STR, remove_label_ids: list[str] = DEFAULT_LIST_STR
    ) -> dict[str, Any]:
        """Modify message."""
        body = {
            "addLabelIds": add_label_ids or [],
            "removeLabelIds": remove_label_ids or [],
        }
        return self.service.users().messages().modify(userId=self.user_id, id=message_id, body=body).execute()


class UserOAuthGmailClient(GoogleGmailClient):
    """Represent UserOAuthGmailClient data and behavior."""

    def __init__(self, credentials: object) -> None:
        """Initialize UserOAuthGmailClient."""
        self.service = _gmail_service(credentials)
        self.user_id = "me"

    @classmethod
    def from_credentials_info(cls, credentials_info: dict[str, Any]) -> Self:
        """Build an OAuth Gmail client from decrypted user credentials JSON."""
        credentials = OAUTH_CREDENTIALS.from_authorized_user_info(credentials_info, scopes=[GMAIL_MODIFY_SCOPE])
        return cls(credentials)

    @classmethod
    def from_credentials_file(cls, credentials_file: Path) -> Self:
        """Build an OAuth Gmail client from a credentials file."""
        credentials = OAUTH_CREDENTIALS.from_authorized_user_file(credentials_file, scopes=[GMAIL_MODIFY_SCOPE])
        return cls(credentials)
