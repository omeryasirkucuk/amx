"""Analyze a codebase to find references to database tables, columns, and schemas."""

from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from amx.utils.logging import get_logger

log = get_logger("codebase.analyzer")

CODE_EXTENSIONS = {
    ".py", ".sql", ".java", ".scala", ".kt", ".js", ".ts",
    ".r", ".R", ".ipynb", ".sh", ".yaml", ".yml", ".json",
    ".cs", ".go", ".rb", ".php", ".pl", ".lua",
}


@dataclass
class CodeReference:
    file: str
    line_no: int
    line_text: str
    matched_asset: str  # table or column name that was found
    context: str = ""   # surrounding lines for richer understanding


@dataclass
class CodebaseReport:
    path: str
    total_files: int = 0
    scanned_files: int = 0
    references: dict[str, list[CodeReference]] = field(default_factory=dict)


def _clone_if_remote(path: str) -> str:
    if path.startswith("https://github.com") or path.startswith("git@"):
        import git as gitpython

        dest = tempfile.mkdtemp(prefix="amx_code_")
        log.info("Cloning %s → %s", path, dest)
        gitpython.Repo.clone_from(path, dest, depth=1)
        return dest
    return path


def test_codebase_path_reachable(path: str) -> None:
    """Verify GitHub/git remote or local directory without cloning. Raises RuntimeError on failure."""
    from amx.docs.scanner import test_git_remote_reachable

    p = path.strip()
    if not p:
        raise RuntimeError("Empty path")
    if p.startswith("https://github.com") or p.startswith("git@"):
        test_git_remote_reachable(p)
        return
    if p.startswith("http://") or p.startswith("https://"):
        raise RuntimeError(
            "Codebase URL must be a Git clone URL (https://github.com/... or git@github.com:...)."
        )
    root = Path(p).expanduser().resolve()
    if not root.exists():
        raise RuntimeError(f"Path does not exist: {root}")
    if not root.is_dir():
        raise RuntimeError(f"Codebase path must be a local directory: {root}")


def analyze_codebase(
    path: str,
    table_names: list[str],
    column_names: list[str] | None = None,
    context_lines: int = 3,
) -> CodebaseReport:
    local_path = _clone_if_remote(path)
    root = Path(local_path)
    report = CodebaseReport(path=path)

    assets = set(table_names)
    if column_names:
        assets.update(column_names)

    pattern = re.compile(
        r"\b(" + "|".join(re.escape(a) for a in sorted(assets, key=len, reverse=True)) + r")\b",
        re.IGNORECASE,
    )

    code_files = [
        f for f in root.rglob("*")
        if f.is_file() and f.suffix.lower() in CODE_EXTENSIONS
    ]
    report.total_files = len(code_files)

    for fpath in code_files:
        try:
            lines = fpath.read_text(errors="replace").splitlines()
        except Exception:
            continue
        report.scanned_files += 1

        for i, line in enumerate(lines):
            for match in pattern.finditer(line):
                asset = match.group(1).lower()
                start = max(0, i - context_lines)
                end = min(len(lines), i + context_lines + 1)
                ctx = "\n".join(lines[start:end])

                ref = CodeReference(
                    file=str(fpath.relative_to(root)),
                    line_no=i + 1,
                    line_text=line.strip(),
                    matched_asset=asset,
                    context=ctx,
                )
                report.references.setdefault(asset, []).append(ref)

    log.info(
        "Scanned %d/%d files in %s, found %d asset references",
        report.scanned_files,
        report.total_files,
        path,
        sum(len(v) for v in report.references.values()),
    )
    return report
