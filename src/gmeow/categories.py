# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Compute and persist categorization decisions for Gmeow messages.

This module owns the deterministic category vocabulary, learned-category discovery, and the
manual-profile and manual-rule overlays that combine into the assignments stored alongside each
message. The CategoryEngine is the entry point used by ingestion, MCP tools, and the maintenance
worker to keep category state in sync with the underlying mail corpus.
"""

import re
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from email.utils import parseaddr
from typing import Any, cast

from sklearn.cluster import DBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from .cache import categorize_message
from .kg import clean_text_for_kg

DEFAULT_HIDDEN = {"camera_alert", "machine_notification", "bulk_status_noise", "call_notice", "dev_activity", "dev_review", "mailing_list"}
MIN_CLUSTER_MESSAGES = 2
MIN_PROFILE_EXAMPLES = 2
PROFILE_MATCH_THRESHOLD = 0.35
MIN_CATEGORY_TOKEN_LENGTH = 3


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


def message_document(message: dict[str, Any]) -> str:
    """Message document."""
    labels = " ".join(message.get("labels", []) + message.get("label_ids", []))
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
        parts.append(clean_text_for_kg((message.get("text_body") or "")[:1200]))
    return normalize_text(" ".join(parts))


def normalize_text(text: str) -> str:
    """Normalize text."""
    value = clean_text_for_kg(text).lower()
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"[\w.+-]+@[\w.-]+", " ", value)
    value = re.sub(r"\b\d+(?:[.:/-]\d+)*\b", " ", value)
    value = re.sub(r"[^a-z0-9_:+#.-]+", " ", value)
    return " ".join(token for token in value.split() if _good_token(token))


def deterministic_assignments(message: dict[str, Any]) -> list[dict[str, Any]]:
    """Deterministic assignments."""
    categories = set(categorize_message(message))
    sender = (message.get("sender") or "").lower()
    subject = (message.get("subject") or "").lower()
    text = " ".join([subject, sender, (message.get("snippet") or "").lower(), (message.get("text_body") or "")[:2000].lower()])
    labels = {label.lower() for label in message.get("labels", [])}

    categories.update(_deterministic_rule_categories(sender, subject, text, labels))

    if categories - {"update", "primary"}:
        categories.discard("primary")
    return [
        {
            "category": category,
            "confidence": 1.0 if category not in {"update", "primary"} else 0.6,
            "source": "system",
            "reason": "deterministic sender/subject/label rule",
            "top_terms": _top_terms(message_document(message), 8),
            "enabled": True,
        }
        for category in sorted(categories)
    ]


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


class CategoryEngine:
    """Represent CategoryEngine data and behavior."""

    def __init__(self, cache: object) -> None:
        """Initialize CategoryEngine."""
        self.cache: Any = cache
        self._manual_profiles_cache: dict[str, list[str]] | None = None

    def categorize_message(self, message_id: str, manual_profiles: dict[str, list[str]] | None = None) -> list[dict[str, Any]]:
        """Categorize message."""
        message = self.cache.get_message(message_id)
        if message is None:
            raise KeyError(message_id)
        assignments = deterministic_assignments(message)
        assignments.extend(self._manual_rule_assignments(message))
        if manual_profiles is None:
            manual_profiles = self._cached_manual_profiles()
        assignments.extend(self._manual_profile_assignments(message, manual_profiles))
        deduped = _dedupe_assignments(assignments)
        self.cache.replace_message_categories(message_id, deduped)
        return deduped

    def recategorize(self, since_hours: int = 0, limit: int = 0) -> dict[str, Any]:
        """Recategorize."""
        messages = self._messages(since_hours=since_hours, limit=limit)
        manual_profiles = self._manual_profiles(messages)
        categorized = 0
        for message in messages:
            assignments = deterministic_assignments(message)
            assignments.extend(self._manual_profile_assignments(message, manual_profiles))
            assignments.extend(self._manual_rule_assignments(message))
            self.cache.replace_message_categories(message["id"], _dedupe_assignments(assignments))
            categorized += 1
        return {"messages": categorized, "manual_profiles": sorted(manual_profiles), "categories": self.cache.category_stats()}

    def seed_initial_categories(self) -> dict[str, Any]:
        """Seed initial categories."""
        inserted = 0
        for category, rule, description in INITIAL_CATEGORY_RULES:
            self.cache.upsert_category(category, source="manual", default_hidden=category in DEFAULT_HIDDEN, description=description)
            if not self._rule_exists(category, rule):
                self.cache.upsert_category_rule(category, rule)
                inserted += 1
        return {"rules_inserted": inserted, "categories": self.cache.list_categories()}

    def discover(self, since_hours: int = 48, limit: int = 0, *, store: bool = True) -> dict[str, Any]:
        """Discover."""
        messages = self._messages(since_hours=since_hours, limit=limit)
        if len(messages) < MIN_CLUSTER_MESSAGES:
            run: dict[str, Any] = {"messages": len(messages), "clusters": []}
            if store:
                run["id"] = self.cache.store_learned_category_run(run)
            return run
        docs = [message_document(message) for message in messages]
        vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_df=0.85, max_features=5000)
        matrix = vectorizer.fit_transform(docs)
        clustering = DBSCAN(eps=0.55, min_samples=2, metric="cosine").fit(matrix)
        terms = vectorizer.get_feature_names_out()
        clusters: list[dict[str, Any]] = []
        for label in sorted({int(value) for value in clustering.labels_} - {-1}):
            indexes = [idx for idx, value in enumerate(clustering.labels_) if int(value) == label]
            top_terms = _matrix_top_terms(matrix[indexes].mean(axis=0), terms, 12)
            senders = Counter(_sender_key(messages[idx]) for idx in indexes).most_common(5)
            category_id = "learned:" + _slug("_".join([senders[0][0] if senders else "cluster", *top_terms[:3]]))
            clusters.append(
                {
                    "id": category_id,
                    "source": "learned",
                    "enabled": False,
                    "messages": len(indexes),
                    "message_ids": [messages[idx]["id"] for idx in indexes],
                    "top_terms": top_terms,
                    "top_senders": [{"sender": sender, "count": count} for sender, count in senders],
                    "sample_subjects": [messages[idx].get("subject") for idx in indexes[:5]],
                }
            )
        run = {"messages": len(messages), "clusters": clusters}
        if store:
            run["id"] = self.cache.store_learned_category_run(run)
        return run

    def enable_learned_category(self, learned_id: str, category: str = "") -> dict[str, Any]:
        """Enable learned category."""
        for run in self.cache.learned_category_runs(limit=50):
            for cluster in run["run"].get("clusters", []):
                if cluster.get("id") != learned_id:
                    continue
                target = category or learned_id.replace("learned:", "learned_")
                self.cache.upsert_category(
                    target,
                    source="learned",
                    default_hidden=target in DEFAULT_HIDDEN,
                    enabled=True,
                    description="Accepted learned category",
                    profile={"top_terms": cluster.get("top_terms", []), "top_senders": cluster.get("top_senders", [])},
                )
                for message_id in cluster.get("message_ids", []):
                    self.cache.replace_message_categories(
                        message_id,
                        [
                            {
                                "category": target,
                                "confidence": 0.8,
                                "source": "learned",
                                "reason": f"accepted learned cluster {learned_id}",
                                "top_terms": cluster.get("top_terms", []),
                            }
                        ],
                        sources=("learned",),
                    )
                return {"enabled": target, "messages": len(cluster.get("message_ids", []))}
        raise KeyError(learned_id)

    def _messages(self, since_hours: int = 0, limit: int = 0) -> list[dict[str, Any]]:
        messages = cast(list[dict[str, Any]], self.cache.iter_messages())
        if since_hours:
            cutoff = datetime.now(UTC) - timedelta(hours=since_hours)
            messages = [
                message
                for message in messages
                if message.get("message_date_iso") and datetime.fromisoformat(message["message_date_iso"]) >= cutoff
            ]
        messages.sort(key=lambda message: cast(str, message.get("message_date_iso") or ""), reverse=True)
        return messages[:limit] if limit else messages

    def _manual_profiles(self, messages: list[dict[str, Any]]) -> dict[str, list[str]]:
        examples: dict[str, list[str]] = defaultdict(list)
        for message in messages:
            for assignment in self.cache.message_category_assignments(message["id"]):
                if assignment["source"] == "manual":
                    examples[assignment["category"]].append(message_document(message))
        return examples

    def _cached_manual_profiles(self) -> dict[str, list[str]]:
        if self._manual_profiles_cache is None:
            self._manual_profiles_cache = self._manual_profiles(self.cache.iter_messages())
        return self._manual_profiles_cache

    def _manual_profile_assignments(self, message: dict[str, Any], profiles: dict[str, list[str]]) -> list[dict[str, Any]]:
        if not profiles:
            return []
        assignments: list[dict[str, Any]] = []
        doc = message_document(message)
        for category, examples in profiles.items():
            corpus = [*examples, doc]
            if len(corpus) < MIN_PROFILE_EXAMPLES:
                continue
            matrix = TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_df=1.0, max_features=3000).fit_transform(corpus)
            similarity = float(cosine_similarity(matrix[-1], matrix[:-1]).max())
            if similarity >= PROFILE_MATCH_THRESHOLD:
                assignments.append(
                    {
                        "category": category,
                        "confidence": round(similarity, 3),
                        "source": "learned",
                        "reason": "similar to manually categorized examples",
                        "top_terms": _top_terms(doc, 8),
                    }
                )
        return assignments

    def _manual_rule_assignments(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        assignments: list[dict[str, Any]] = []
        for rule in self.cache.list_category_rules():
            if not rule["enabled"] or not _rule_matches(message, rule["rule"]):
                continue
            assignments.append(
                {
                    "category": rule["category"],
                    "confidence": float(rule["rule"].get("confidence", 0.95)),
                    "source": "manual_rule",
                    "reason": f"matched category rule {rule['id']}",
                    "top_terms": _top_terms(message_document(message), 8),
                }
            )
        return assignments

    def _rule_exists(self, category: str, rule: dict[str, Any]) -> bool:
        return any(existing["category"] == category and existing["rule"] == rule for existing in self.cache.list_category_rules())


def _rule_matches(message: dict[str, Any], rule: dict[str, Any]) -> bool:
    haystacks = {
        "sender": (message.get("sender") or "").lower(),
        "subject": (message.get("subject") or "").lower(),
        "body": " ".join([(message.get("snippet") or ""), (message.get("text_body") or "")]).lower(),
        "labels": " ".join(message.get("labels", []) + message.get("label_ids", [])).lower(),
    }
    for key in ["sender", "subject", "body", "labels"]:
        values: Any = rule.get(key) or rule.get(f"{key}_contains") or []
        if isinstance(values, str):
            values = [values]
        if values and not any(str(value).lower() in haystacks[key] for value in values):
            return False
    domain = rule.get("domain")
    return not (domain and str(domain).lower() not in haystacks["sender"])


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


def _dedupe_assignments(assignments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_category: dict[str, dict[str, Any]] = {}
    for assignment in assignments:
        category = assignment["category"]
        existing = by_category.get(category)
        if existing is None or assignment.get("confidence", 0) >= existing.get("confidence", 0):
            by_category[category] = assignment
    return sorted(by_category.values(), key=lambda item: (-float(item.get("confidence", 0)), item["category"]))
