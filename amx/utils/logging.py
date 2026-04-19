"""Structured logging for AMX."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

LOG_DIR = Path.home() / ".amx" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Written when the profile agent cannot parse an LLM reply (debugging).
LAST_PROFILE_RESPONSE_FILE = LOG_DIR / "last_profile_agent_response.txt"

_fmt = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"amx.{name}")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(LOG_DIR / "amx.log")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(_fmt)
        logger.addHandler(fh)
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(logging.WARNING)
        sh.setFormatter(_fmt)
        logger.addHandler(sh)
    return logger
