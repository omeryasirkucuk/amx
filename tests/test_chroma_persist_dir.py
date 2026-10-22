"""The Chroma vector store must live under the resolved config dir.

``AMX_CONFIG_DIR`` is documented to keep two AMX instances from sharing
config, history.db, uploads and chroma_db. The vector store paths were
hardcoded to ``~/.amx/chroma_db``, so isolated instances still read and
wrote one shared docs/code/search index.
"""

from __future__ import annotations

import re
from pathlib import Path

import amx

AMX_ROOT = Path(amx.__file__).resolve().parent


def test_chroma_persist_dir_follows_amx_config_dir(tmp_path: Path, monkeypatch) -> None:
    from amx.config import chroma_persist_dir

    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path / "isolated"))
    assert Path(chroma_persist_dir()) == tmp_path / "isolated" / "chroma_db"


def test_chroma_persist_dir_defaults_to_home(monkeypatch) -> None:
    from amx.config import chroma_persist_dir

    monkeypatch.delenv("AMX_CONFIG_DIR", raising=False)
    assert Path(chroma_persist_dir()) == Path.home() / ".amx" / "chroma_db"


def test_no_module_hardcodes_the_home_chroma_path() -> None:
    pattern = re.compile(r"""Path\.home\(\)\s*/\s*["']\.amx["']\s*/\s*["']chroma_db["']""")
    offenders = [
        str(p.relative_to(AMX_ROOT))
        for p in AMX_ROOT.rglob("*.py")
        if pattern.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == []
