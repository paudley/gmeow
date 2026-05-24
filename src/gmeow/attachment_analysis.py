# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Analyze attachment content for the Gmeow archive.

The analyzer extracts text, OCR, archive listings, and optional vision captions from cached
attachments so downstream search and intelligence can operate on derived signals. It defers to
external tools (tesseract, pandoc, exiftool) when configured and falls back to skip records when
those binaries are unavailable.
"""

import base64
import json
import mimetypes
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, cast

from .config import AttachmentAnalysisConfig
from .http_json import HttpJsonError, post_json


class _OpenAIContentPart(TypedDict, total=False):
    """Single content fragment in an OpenAI chat response."""

    type: str
    text: str


DEFAULT_ATTACHMENT_ANALYSIS_CONFIG = cast(AttachmentAnalysisConfig, None)
ANALYSIS_VERSION = 1
TEXT_TYPES = (
    "application/json",
    "application/xml",
    "application/yaml",
    "application/x-yaml",
    "message/",
    "text/",
)


def analyze_attachment(
    path: Path,
    content: bytes,
    metadata: dict[str, Any],
    config: AttachmentAnalysisConfig = DEFAULT_ATTACHMENT_ANALYSIS_CONFIG,
) -> dict[str, Any]:
    """Analyze attachment."""
    config = config or AttachmentAnalysisConfig()
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    analysis: dict[str, Any] = {
        "version": ANALYSIS_VERSION,
        "extracted_at": now,
        "tools": {},
        "errors": [],
        "skipped": [],
        "available_text": False,
    }
    if not config.enabled:
        analysis["skipped"].append({"stage": "analysis", "reason": "disabled"})
        return analysis

    source = cast(dict[str, Any], metadata.get("source", {}) if isinstance(metadata.get("source"), dict) else {})
    gmail = cast(dict[str, Any], source.get("gmail", {}) if isinstance(source.get("gmail"), dict) else {})
    declared_media_type = cast(str, metadata.get("media_type") or gmail.get("mime_type") or "application/octet-stream")
    filename = cast(str, gmail.get("filename") or metadata.get("filename") or path.name)
    detected = _detect_file(path)
    media_type = _best_media_type(declared_media_type, detected, filename)
    analysis["file"] = {
        "filename": filename,
        "declared_media_type": declared_media_type,
        "detected_media_type": detected.get("mime_type"),
        "media_type": media_type,
        "size": len(content),
        "extension": Path(str(filename)).suffix.lower().lstrip("."),
        "file_description": detected.get("description"),
    }

    _add_text_analysis(analysis, content, media_type, filename, config)
    _add_pdf_analysis(analysis, path, media_type, config)
    _add_image_analysis(analysis, path, content, media_type, config)
    if _is_calendar(media_type, filename):
        _merge_stage(analysis, "calendar", _calendar_analysis(content))
    _add_archive_analysis(analysis, path, media_type, filename, config)
    if "text" not in analysis and config.pandoc_enabled and _pandoc_candidate(media_type, filename):
        _merge_stage(analysis, "document", _pandoc_text(path, config.max_text_chars))

    _finalize_text(analysis, config.max_text_chars)
    return analysis


def _add_text_analysis(analysis: dict[str, Any], content: bytes, media_type: str, filename: str, config: AttachmentAnalysisConfig) -> None:
    if not _is_text_like(media_type, filename):
        return
    text = _decode_text(content, config.max_text_chars)
    if text:
        analysis["text"] = text


def _add_pdf_analysis(analysis: dict[str, Any], path: Path, media_type: str, config: AttachmentAnalysisConfig) -> None:
    if media_type != "application/pdf":
        return
    if config.pdf_text_enabled:
        _merge_stage(analysis, "document", _pdf_analysis(path, config.max_text_chars))
    else:
        analysis["skipped"].append({"stage": "pdf", "reason": "disabled"})


def _add_image_analysis(analysis: dict[str, Any], path: Path, content: bytes, media_type: str, config: AttachmentAnalysisConfig) -> None:
    if not media_type.startswith("image/"):
        return
    if config.ocr_enabled:
        _merge_stage(analysis, "image", _image_ocr(path, config.ocr_languages, config.max_text_chars))
    else:
        analysis["skipped"].append({"stage": "ocr", "reason": "disabled"})
    if config.vision_caption_enabled:
        _merge_stage(analysis, "vision", _vision_caption(content, media_type, config))
    else:
        analysis["skipped"].append({"stage": "vision_caption", "reason": "disabled"})


def _add_archive_analysis(analysis: dict[str, Any], path: Path, media_type: str, filename: str, config: AttachmentAnalysisConfig) -> None:
    if not _is_archive(media_type, filename):
        return
    if config.archive_listing_enabled:
        _merge_stage(analysis, "archive", _archive_listing(path, filename))
    else:
        analysis["skipped"].append({"stage": "archive", "reason": "disabled"})


def _merge_stage(analysis: dict[str, Any], key: str, result: dict[str, Any]) -> None:
    for tool, value in result.pop("tools", {}).items():
        analysis["tools"][tool] = value
    analysis["errors"].extend(result.pop("errors", []))
    analysis["skipped"].extend(result.pop("skipped", []))
    if not result:
        return
    existing = analysis.get(key)
    if isinstance(existing, dict):
        analysis[key] = _deep_merge(cast(dict[str, Any], existing), result)
    else:
        analysis[key] = result


def _detect_file(path: Path) -> dict[str, str]:
    result = _run(["file", "--brief", "--mime-type", str(path)], timeout=10)
    description = _run(["file", "--brief", str(path)], timeout=10)
    return {
        "mime_type": result.get("stdout", "").strip() if result.get("ok") else "",
        "description": description.get("stdout", "").strip() if description.get("ok") else "",
    }


def _best_media_type(declared: str, detected: dict[str, Any], filename: str) -> str:
    detected_type = detected.get("mime_type")
    guessed, _ = mimetypes.guess_type(filename or "")
    for value in [detected_type, declared, guessed]:
        if value and value != "application/octet-stream":
            return str(value).split(";", 1)[0].lower()
    return (declared or "application/octet-stream").split(";", 1)[0].lower()


def _is_text_like(media_type: str, filename: str) -> bool:
    lowered = media_type.lower()
    if any(lowered.startswith(prefix) for prefix in TEXT_TYPES):
        return True
    return Path(filename or "").suffix.lower() in {".txt", ".md", ".csv", ".tsv", ".json", ".xml", ".yaml", ".yml", ".ics", ".vcf"}


def _decode_text(content: bytes, limit: int) -> str:
    fragment = content[: limit * 4]
    failures: list[str] = []
    for encoding in ["utf-8", "utf-16", "latin-1"]:
        try:
            text = fragment.decode(encoding)
        except UnicodeDecodeError as exc:
            failures.append(f"{encoding}: {exc!s}")
            continue
        return _clean_text(text, limit)
    return ""


def _pdf_analysis(path: Path, limit: int) -> dict[str, Any]:
    result: dict[str, Any] = {"tools": {}, "errors": [], "skipped": []}
    info = _run(["pdfinfo", str(path)], timeout=30)
    result["tools"]["pdfinfo"] = _tool_status(info)
    if info.get("ok"):
        parsed: dict[str, Any] = {}
        for line in info["stdout"].splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                parsed[_snake(key)] = value.strip()
        result.update(parsed)
    else:
        result["errors"].append({"stage": "pdfinfo", "error": info.get("error")})
    text = _run(["pdftotext", "-layout", "-enc", "UTF-8", str(path), "-"], timeout=60)
    result["tools"]["pdftotext"] = _tool_status(text)
    if text.get("ok") and text.get("stdout", "").strip():
        result["text"] = _clean_text(text["stdout"], limit)
    elif not text.get("ok"):
        result["errors"].append({"stage": "pdftotext", "error": text.get("error")})
    return result


def _image_ocr(path: Path, languages: str, limit: int) -> dict[str, Any]:
    result: dict[str, Any] = {"tools": {}, "errors": [], "skipped": []}
    ocr = _run(["tesseract", str(path), "stdout", "-l", languages], timeout=60)
    result["tools"]["tesseract"] = _tool_status(ocr)
    if ocr.get("ok") and ocr.get("stdout", "").strip():
        result["ocr_text"] = _clean_text(ocr["stdout"], limit)
    elif ocr.get("ok"):
        result["skipped"].append({"stage": "ocr", "reason": "no text detected"})
    else:
        result["errors"].append({"stage": "ocr", "error": ocr.get("error")})
    return result


def _vision_caption(content: bytes, media_type: str, config: AttachmentAnalysisConfig) -> dict[str, Any]:
    result: dict[str, Any] = {"tools": {}, "errors": [], "skipped": []}
    if not config.vision_caption_endpoint or not config.vision_caption_model:
        result["skipped"].append({"stage": "vision_caption", "reason": "endpoint or model not configured"})
        return result
    payload = {
        "model": config.vision_caption_model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Describe this email attachment image concisely. Include visible text, objects, UI state, and notable context."
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{media_type};base64,{base64.b64encode(content).decode()}"},
                    },
                ],
            }
        ],
        "max_tokens": 256,
        "temperature": 0,
    }
    try:
        _, data = post_json(config.vision_caption_endpoint, payload, timeout=config.vision_caption_timeout_seconds)
    except (HttpJsonError, OSError, TimeoutError, ValueError, TypeError, json.JSONDecodeError) as exc:
        result["tools"]["vision_caption"] = {"available": False, "error": repr(exc)}
        result["errors"].append({"stage": "vision_caption", "error": repr(exc)})
        return result
    text = _extract_openai_text(data)
    result["tools"]["vision_caption"] = {"available": True, "model": config.vision_caption_model}
    if text:
        result["vision_caption"] = text
    return result


def _calendar_analysis(content: bytes) -> dict[str, Any]:
    text = _decode_text(content, 50_000)
    fields: dict[str, list[str]] = {}
    unfolded = re.sub(r"\r?\n[ \t]", "", text)
    for line in unfolded.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.split(";", 1)[0].lower()
        if key in {"summary", "description", "location", "dtstart", "dtend", "organizer", "attendee", "uid"}:
            fields.setdefault(key, []).append(value.strip())
    summary = "\n".join(f"{key}: {', '.join(values[:10])}" for key, values in sorted(fields.items()))
    return {"text": _clean_text(summary or text, 50_000), "fields": fields, "tools": {}, "errors": [], "skipped": []}


def _archive_listing(path: Path, filename: str) -> dict[str, Any]:
    suffix = Path(filename or "").suffix.lower()
    if suffix == ".zip":
        listed = _run(["unzip", "-l", str(path)], timeout=30)
        tool = "unzip"
    else:
        listed = _run(["7z", "l", "-slt", str(path)], timeout=30)
        tool = "7z"
    result: dict[str, Any] = {"tools": {tool: _tool_status(listed)}, "errors": [], "skipped": []}
    if not listed.get("ok"):
        result["errors"].append({"stage": "archive", "error": listed.get("error")})
        return result
    files = _archive_files(listed["stdout"])
    result["files"] = files[:1000]
    result["file_count"] = len(files)
    result["text"] = "\n".join(files[:1000])
    return result


def _pandoc_text(path: Path, limit: int) -> dict[str, Any]:
    result = _run(["pandoc", "-t", "plain", str(path)], timeout=60)
    output: dict[str, Any] = {"tools": {"pandoc": _tool_status(result)}, "errors": [], "skipped": []}
    if result.get("ok") and result.get("stdout", "").strip():
        output["text"] = _clean_text(result["stdout"], limit)
    elif not result.get("ok"):
        output["errors"].append({"stage": "pandoc", "error": result.get("error")})
    return output


def _run(args: list[str], timeout: int) -> dict[str, Any]:
    if shutil.which(args[0]) is None:
        return {"ok": False, "error": f"{args[0]} not found"}
    try:
        completed = subprocess.run(args, check=False, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"{args[0]} timed out"}
    error = completed.stderr.strip() or completed.stdout.strip()
    return {
        "ok": completed.returncode == 0,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "error": None if completed.returncode == 0 else error[:2000],
    }


def _tool_status(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("ok"):
        return {"available": True}
    return {"available": False, "error": result.get("error")}


def _is_calendar(media_type: str, filename: str) -> bool:
    return media_type in {"text/calendar", "application/ics"} or Path(filename or "").suffix.lower() == ".ics"


def _is_archive(media_type: str, filename: str) -> bool:
    suffix = Path(filename or "").suffix.lower()
    return media_type in {"application/zip", "application/x-7z-compressed"} or suffix in {".zip", ".7z"}


def _pandoc_candidate(media_type: str, filename: str) -> bool:
    suffix = Path(filename or "").suffix.lower()
    return media_type in {"text/html", "application/rtf"} or suffix in {".html", ".htm", ".rtf", ".docx", ".odt"}


def _archive_files(output: str) -> list[str]:
    files: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("Archive:", "Length", "Date", "----", "Path = ", "Size = ", "Packed Size = ")):
            continue
        if re.fullmatch(r"\d+\s+files?", line):
            continue
        if "  " in line and re.match(r"^\d+", line):
            parts = re.split(r"\s{2,}", line)
            if parts and not re.fullmatch(r"\d+\s+files?", parts[-1]):
                files.append(parts[-1])
        elif line.startswith("Path = "):
            files.append(line.removeprefix("Path = ").strip())
    return sorted({item for item in files if item and item not in {".", ".."}})


def _finalize_text(analysis: dict[str, Any], limit: int) -> None:
    keys = ["text", "ocr_text", "vision_caption"]
    texts: list[str] = [analysis[key] for key in keys if isinstance(analysis.get(key), str)]
    for section in ["document", "image", "calendar", "archive", "vision"]:
        value = analysis.get(section)
        if isinstance(value, dict):
            section_dict = cast(dict[str, Any], value)
            texts.extend(section_dict[key] for key in keys if isinstance(section_dict.get(key), str))
    content = _clean_text("\n\n".join(texts), limit)
    if content:
        analysis["content_text"] = content
        analysis["available_text"] = True


def _clean_text(text: str, limit: int) -> str:
    cleaned = re.sub(r"\r\n?", "\n", text or "")
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{4,}", "\n\n\n", cleaned)
    return cleaned.strip()[:limit]


def _snake(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _extract_openai_text(data: dict[str, Any]) -> str:
    try:
        raw_content: Any = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return ""
    if isinstance(raw_content, str):
        return raw_content.strip()
    if not isinstance(raw_content, list):
        return ""
    typed_parts: list[_OpenAIContentPart] = cast(list[_OpenAIContentPart], raw_content)
    return "\n".join(part.get("text", "").strip() for part in typed_parts).strip()


def _deep_merge(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(existing)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(cast(dict[str, Any], merged[key]), cast(dict[str, Any], value))
        else:
            merged[key] = value
    return merged
