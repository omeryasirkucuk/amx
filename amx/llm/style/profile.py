"""StyleProfile: a structured, content-safe summary of a user's
description writing style. Only this object — never the original
reference comments — is injected into run-time prompts."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass

PLACEHOLDERS: frozenset[str] = frozenset(
    {"<ENTITY>", "<METRIC>", "<DATE_FIELD>", "<STATUS>", "<IDENTIFIER>"}
)

_PLACEHOLDER_RE = re.compile(r"<[A-Z_]+>")


@dataclass(frozen=True)
class StyleProfile:
    language: str
    tone: str
    avg_length_words: int
    length_range: tuple[int, int]
    person: str
    capitalization: str
    ends_with_period: bool
    structural_patterns: list[str]
    vocabulary_register: str
    redacted_examples: list[str]

    def to_json(self) -> str:
        d = asdict(self)
        d["length_range"] = list(self.length_range)
        return json.dumps(d, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, payload: str) -> StyleProfile:
        d = json.loads(payload)
        for ex in d.get("redacted_examples", []):
            for tag in _PLACEHOLDER_RE.findall(ex):
                if tag not in PLACEHOLDERS:
                    raise ValueError(f"redacted example uses unknown placeholder {tag}")
        d["length_range"] = tuple(d["length_range"])
        return cls(**d)
