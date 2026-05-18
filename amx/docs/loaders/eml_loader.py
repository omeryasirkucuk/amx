"""Email (.eml) loader used by the docs scanner.

Headers (From/To/Subject/Date) become a YAML-style frontmatter
block at the top of the returned text; the body falls back from
text/plain to text/html (the latter converted with markdownify).
Attachments are listed by filename only; full attachment ingestion
is a follow-up. Quoted-reply blocks are preserved so the LLM can
see the thread context.
"""

from __future__ import annotations

from email import message_from_bytes
from email.message import Message
from pathlib import Path

from markdownify import markdownify

HEADERS = ("From", "To", "Cc", "Subject", "Date")


def _decoded(part: Message) -> str:
    raw = part.get_payload(decode=True)
    payload: bytes = raw if isinstance(raw, bytes) else b""
    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")


def _pick_body(msg: Message) -> str:
    if msg.is_multipart():
        text_part: Message | None = None
        html_part: Message | None = None
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain" and text_part is None:
                text_part = part
            elif ctype == "text/html" and html_part is None:
                html_part = part
        if text_part is not None:
            return _decoded(text_part)
        if html_part is not None:
            return markdownify(_decoded(html_part))
        return ""
    text = _decoded(msg)
    if msg.get_content_type() == "text/html":
        text = markdownify(text)
    return text


def _list_attachments(msg: Message) -> list[str]:
    out: list[str] = []
    if not msg.is_multipart():
        return out
    for part in msg.walk():
        filename = part.get_filename()
        if filename:
            out.append(filename)
    return out


def load_eml(path: str | Path) -> str:
    raw = Path(path).read_bytes()
    msg = message_from_bytes(raw)
    headers = [f"{h}: {msg.get(h, '').strip()}" for h in HEADERS if msg.get(h)]
    body = _pick_body(msg).strip()
    attachments = _list_attachments(msg)
    parts = ["\n".join(headers), "", body]
    if attachments:
        parts.append("")
        parts.append("Attachments: " + ", ".join(attachments))
    return "\n".join(parts)
