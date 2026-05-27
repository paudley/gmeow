# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""sklearn-backed categorization external analyzer for the ANALYSIS phase.

This module ports deterministic main-branch email category rules into the external analyzer contract.
It also exposes the learned-category discovery path used for operator review and backfill audits.
"""

import re
from collections import Counter
from email.utils import parseaddr
from typing import Any, Protocol, cast

from sklearn.cluster import DBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer

from gmeow_intel.contracts import Annotation, ExternalCommandRequest

DEFAULT_HIDDEN = {
    "camera_alert",
    "machine_notification",
    "bulk_status_noise",
    "call_notice",
    "dev_activity",
    "dev_review",
    "mailing_list",
}
MIN_CATEGORY_TOKEN_LENGTH = 3
MIN_CLUSTER_MESSAGES = 2


class _SelectedMatrixLike(Protocol):
    def mean(self, _axis: int) -> object:
        """Return the average vector for selected matrix rows."""
        ...


class _MatrixLike(Protocol):
    def __getitem__(self, _key: object) -> _SelectedMatrixLike:
        """Return a selected matrix view for row indexes."""
        ...


class _VectorizerLike(Protocol):
    def fit_transform(self, _documents: list[str]) -> _MatrixLike:
        """Fit the vectorizer and return the sparse document matrix."""
        ...

    def get_feature_names_out(self) -> object:
        """Return the learned feature names in matrix column order."""
        ...


class _ClusteringLike(Protocol):
    labels_: object


INITIAL_CATEGORY_RULES: list[tuple[str, dict[str, Any], str]] = [
    ("camera_alert", {"domain": "notifications.ui.com"}, "UniFi Protect camera notifications"),
    ("call_notice", {"domain": "ringcentral.com"}, "RingCentral call and voicemail notifications"),
    ("dev_activity", {"domain": "github.com"}, "GitHub repository notifications"),
    ("dev_activity", {"domain": "gitlab.com"}, "GitLab repository notifications"),
    ("dev_ci", {"domain": "github.com", "subject": ["ci", "workflow", "run failed"]}, "GitHub CI notifications"),
    ("dev_ci", {"domain": "gitlab.com", "subject": ["pipeline"]}, "GitLab pipeline notifications"),
    ("dev_review", {"domain": "github.com", "body": ["pull request", "review", "commented"]}, "GitHub code review notifications"),
    ("security_admin", {"domain": "google.com", "sender": ["workspace-alerts"]}, "Google Workspace admin alerts"),
    (
        "security_admin",
        {"domain": "gitlab.com", "subject": ["password", "passkey", "verify your identity"]},
        "GitLab security account alerts",
    ),
    ("financial_statement", {"domain": "interactivebrokers.com"}, "Interactive Brokers statements and reports"),
    ("financial_statement", {"subject": ["statement", "e-statement", "invoice", "receipt"]}, "Statements, invoices, and receipts"),
    ("mailing_list", {"domain": "tuhs.org"}, "TUHS mailing list"),
    ("newsletter", {"sender": ["newsletter", "daily"], "subject": ["daily", "weekly"]}, "Newsletter and digest mail"),
    ("promotion", {"labels": ["CATEGORY_PROMOTIONS"]}, "Gmail promotions"),
    ("social", {"labels": ["CATEGORY_SOCIAL"]}, "Gmail social category"),
    ("social", {"domain": "redditmail.com"}, "Reddit notifications"),
    ("social", {"domain": "linkedin.com"}, "LinkedIn notifications"),
    ("social", {"domain": "patreon.com"}, "Patreon/community notifications"),
    ("real_estate", {"subject": ["subject removal", "possession date", "completion date", "rolston"]}, "Real estate transaction mail"),
    ("corporate_filing", {"subject": ["corporation", "corporations canada", "french name"]}, "Corporate filing and registry mail"),
    ("legal", {"subject": ["strata plan", "owners", "v.", "eps1755"]}, "Legal and strata matter mail"),
    ("family", {"sender": ["erin@", "couple@"], "labels": ["1people/erin"]}, "Family/couple correspondence"),
    ("health_appointment", {"subject": ["vet appointment", "appointment", "seniors panel"]}, "Appointments and health-related scheduling"),
    ("home_services", {"subject": ["tree care", "payment received", "invoice"], "labels": ["1house"]}, "Household service/vendor mail"),
    ("work", {"labels": ["1work"]}, "Work-labelled mail"),
]


def analyzer_name() -> str:
    """Return the stable analyzer name."""
    return "categories.sklearn"


def analyze(request: ExternalCommandRequest) -> Annotation:
    """Return main-branch deterministic category assignments for one message."""
    message = message_from_request(request)
    assignments = deterministic_assignments(message)

    return Annotation(
        schema_version=request.schema_version,
        object_digest=request.job.object_digest,
        kind="analysis",
        analyzer_name=request.job.analyzer.name,
        analyzer_version=request.job.analyzer.version,
        data={
            "status": "complete",
            "categories": assignments,
            "category_ids": [assignment["category"] for assignment in assignments],
            "default_hidden": sorted(DEFAULT_HIDDEN),
            "implementation": "python.sklearn_mainbranch",
        },
    )


def message_from_request(request: ExternalCommandRequest) -> dict[str, Any]:
    """Build the message shape used by the old category engine from FILESTORE data."""
    metadata = _mail_metadata(request.manifest)
    labels = _string_list(metadata.get("labels")) + _string_list(metadata.get("label_ids"))
    headers = _headers_from_text(request.text)
    sender = _first_string(metadata, "sender", "from") or headers.get("from", "")
    subject = _first_string(metadata, "subject") or headers.get("subject", "")
    snippet = _first_string(metadata, "snippet")

    return {
        "id": _first_string(metadata, "message_id", "id") or request.job.object_digest,
        "sender": sender,
        "subject": subject,
        "snippet": snippet,
        "text_body": request.text,
        "labels": labels,
        "label_ids": labels,
    }


def deterministic_assignments(message: dict[str, Any]) -> list[dict[str, Any]]:
    """Return deterministic category assignments ported from the main branch."""
    categories = set(_base_categories(message))
    sender = (message.get("sender") or "").lower()
    subject = (message.get("subject") or "").lower()
    text = " ".join([subject, sender, (message.get("snippet") or "").lower(), (message.get("text_body") or "")[:2000].lower()])
    labels = {label.lower() for label in message.get("labels", [])}

    categories.update(_deterministic_rule_categories(sender, subject, text, labels))

    for category, rule, _description in INITIAL_CATEGORY_RULES:
        if _rule_matches(message, rule):
            categories.add(category)

    if categories - {"update", "primary"}:
        categories.discard("primary")

    document = message_document(message)
    return [
        {
            "category": category,
            "confidence": 1.0 if category not in {"update", "primary"} else 0.6,
            "source": "system",
            "reason": "deterministic sender/subject/label rule",
            "top_terms": _top_terms(document, 8),
            "enabled": True,
            "default_hidden": category in DEFAULT_HIDDEN,
        }
        for category in sorted(categories)
    ]


def discover(messages: list[dict[str, Any]]) -> dict[str, Any]:
    """Discover learned category clusters using the old main-branch TF-IDF/DBSCAN method."""
    if len(messages) < MIN_CLUSTER_MESSAGES:
        return {"messages": len(messages), "clusters": []}

    docs = [message_document(message) for message in messages]
    vectorizer = _tfidf_vectorizer()
    matrix = vectorizer.fit_transform(docs)
    clustering = _dbscan_fit(matrix)
    terms = vectorizer.get_feature_names_out()
    clusters: list[dict[str, Any]] = []
    labels = [int(value) for value in cast(Any, clustering.labels_)]
    for label in sorted(set(labels) - {-1}):
        indexes = [idx for idx, value in enumerate(labels) if value == label]
        top_terms = _matrix_top_terms(matrix[indexes].mean(0), terms, 12)
        senders = Counter(_sender_key(messages[idx]) for idx in indexes).most_common(5)
        category_id = "learned:" + _slug("_".join([senders[0][0] if senders else "cluster", *top_terms[:3]]))
        clusters.append(
            {
                "id": category_id,
                "source": "learned",
                "enabled": False,
                "messages": len(indexes),
                "message_ids": [messages[idx].get("id", "") for idx in indexes],
                "top_terms": top_terms,
                "top_senders": [{"sender": sender, "count": count} for sender, count in senders],
                "sample_subjects": [messages[idx].get("subject") for idx in indexes[:5]],
            }
        )

    return {"messages": len(messages), "clusters": clusters}


def _tfidf_vectorizer() -> _VectorizerLike:
    return cast(_VectorizerLike, TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_df=0.85, max_features=5000))


def _dbscan_fit(matrix: _MatrixLike) -> _ClusteringLike:
    return cast(_ClusteringLike, DBSCAN(eps=0.55, min_samples=2, metric="cosine").fit(matrix))


def message_document(message: dict[str, Any]) -> str:
    """Return the normalized document used for category scoring and discovery."""
    labels = " ".join(_string_list(message.get("labels")) + _string_list(message.get("label_ids")))
    sender_name, sender_addr = parseaddr(message.get("sender") or "")
    sender_domain = sender_addr.split("@", 1)[-1] if "@" in sender_addr else sender_addr
    parts = [
        f"sender_name:{sender_name}",
        f"sender:{sender_addr}",
        f"sender_domain:{sender_domain}",
        f"labels:{labels}",
        f"subject:{message.get('subject') or ''}",
        message.get("subject") or "",
        message.get("subject") or "",
        f"snippet:{message.get('snippet') or ''}",
        message.get("snippet") or "",
    ]
    if not message.get("snippet"):
        parts.append((message.get("text_body") or "")[:1200])
    return normalize_text(" ".join(parts))


def normalize_text(text: str) -> str:
    """Normalize message text for deterministic and learned categories."""
    value = text.lower()
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"[\w.+-]+@[\w.-]+", " ", value)
    value = re.sub(r"\b\d+(?:[.:/-]\d+)*\b", " ", value)
    value = re.sub(r"[^a-z0-9_:+#.-]+", " ", value)
    return " ".join(token for token in value.split() if _good_token(token))


def _base_categories(message: dict[str, Any]) -> list[str]:
    sender = (message.get("sender") or "").lower()
    snippet = (message.get("snippet") or "").lower()
    text = " ".join([(message.get("subject") or "").lower(), sender, snippet, (message.get("text_body") or "")[:2000].lower()])
    labels = {label.lower() for label in message.get("labels", [])}
    rules = [
        ("camera_alert", "notifications.ui.com" in sender or "unifi os" in sender or "unifi.ui.com" in text),
        ("camera_alert", "smart detection" in text and ("recorded an animal" in text or "protect/events" in text)),
        ("news_alert", "googlealerts-noreply@google.com" in sender),
        ("admin_alert", "google-workspace-alerts-noreply@google.com" in sender),
        ("promotion", "category_promotions" in labels),
        ("update", "category_updates" in labels),
    ]
    categories = [category for category, matched in rules if matched]
    if not categories:
        categories.append("primary")
    return sorted(set(categories))


def _deterministic_rule_categories(sender: str, subject: str, text: str, labels: set[str]) -> set[str]:
    categories: set[str] = set()
    rules = [
        ("call_notice", "notify@ringcentral.com" in sender or "service@ringcentral.com" in sender or subject.startswith("new call")),
        ("security_admin", _is_security_admin_message(sender, subject, text)),
        ("financial_statement", "interactivebrokers.com" in sender or "statement" in subject or "trade confirmation" in text),
        ("mailing_list", "tuhs.org" in sender or "category_forums" in labels),
        ("newsletter", "newsletter" in sender or "daily@" in sender or "weekly" in subject),
        ("social", "category_social" in labels or "redditmail.com" in sender or "linkedin.com" in sender or "patreon.com" in sender),
    ]
    categories.update(category for category, matched in rules if matched)
    categories.update(_dev_categories(sender, subject, text))
    return categories


def _dev_categories(sender: str, subject: str, text: str) -> set[str]:
    categories: set[str] = set()
    if "notifications@github.com" in sender or "noreply.github.com" in text:
        categories.add("dev_activity")
        if "commented on this pull request" in text or "review" in text or "requested" in text:
            categories.add("dev_review")
        if "workflow run" in text or "ci" in subject or "run failed" in subject:
            categories.add("dev_ci")
    if "gitlab@mg.gitlab.com" in sender:
        categories.add("dev_activity")
        if "pipeline" in text or "failed pipeline" in subject or "fixed pipeline" in subject:
            categories.add("dev_ci")
    return categories


def _is_security_admin_message(sender: str, subject: str, text: str) -> bool:
    return (
        "google-workspace-alerts-noreply@google.com" in sender
        or "verify your identity" in text
        or "password changed" in text
        or (
            "gitlab@mg.gitlab.com" in sender
            and ("password changed" in subject or "passkey" in subject or "verify your identity" in subject)
        )
    )


def _rule_matches(message: dict[str, Any], rule: dict[str, Any]) -> bool:
    haystacks = {
        "sender": (message.get("sender") or "").lower(),
        "subject": (message.get("subject") or "").lower(),
        "body": " ".join([(message.get("snippet") or ""), (message.get("text_body") or "")]).lower(),
        "labels": " ".join(_string_list(message.get("labels")) + _string_list(message.get("label_ids"))).lower(),
    }
    for key in ["sender", "subject", "body", "labels"]:
        values: Any = rule.get(key) or rule.get(f"{key}_contains") or []
        if isinstance(values, str):
            values = [values]
        if values and not any(str(value).lower() in haystacks[key] for value in values):
            return False
    domain = rule.get("domain")
    return not (domain and str(domain).lower() not in haystacks["sender"])


def _mail_metadata(manifest: dict[str, object]) -> dict[str, object]:
    for facet in cast(list[dict[str, object]], manifest.get("facets", [])):
        if facet.get("kind") == "mail_message" and isinstance(facet.get("metadata"), dict):
            return cast(dict[str, object], facet["metadata"])
    return {}


def _headers_from_text(text: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in text.splitlines()[:40]:
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        key = name.strip().lower()
        if key in {"from", "to", "subject", "date", "message-id"}:
            headers[key] = value.strip()
    return headers


def _first_string(values: dict[str, object], *keys: str) -> str:
    for key in keys:
        value = values.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        items = cast(list[object], value)
        return [text for item in items if (text := str(item))]
    if isinstance(value, str) and value:
        return [value]
    return []


def _good_token(token: str) -> bool:
    if len(token) < MIN_CATEGORY_TOKEN_LENGTH:
        return False
    if token.isdigit():
        return False
    return token not in {"the", "and", "for", "you", "your", "with", "this", "that", "from", "have", "has", "was", "are", "http", "https"}


def _top_terms(text: str, limit: int) -> list[str]:
    return [term for term, _ in Counter(text.split()).most_common(limit)]


def _matrix_top_terms(row: object, terms: object, limit: int) -> list[str]:
    matrix_row = cast(Any, row)
    term_values = cast(Any, terms)
    array = matrix_row.A1 if hasattr(matrix_row, "A1") else matrix_row.asarray().ravel()
    indexes = array.argsort()[::-1][:limit]
    return [str(term_values[index]) for index in indexes if array[index] > 0]


def _sender_key(message: dict[str, Any]) -> str:
    _, addr = parseaddr(message.get("sender") or "")
    return addr.lower() or (message.get("sender") or "").lower()


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug[:80] or "cluster"
