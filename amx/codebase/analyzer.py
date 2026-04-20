"""Analyze a codebase to find references to database tables, columns, and schemas."""

from __future__ import annotations

import ast
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from amx.utils.logging import get_logger

log = get_logger("codebase.analyzer")

MAX_REGEX_ASSETS = 450
MAX_COL_NAMES = 280

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
    # Mentions that look like catalog objects but are not in connected DB table list (LLM context only)
    external_mentions: dict[str, list[CodeReference]] = field(default_factory=dict)


def _clone_if_remote(path: str) -> str:
    if path.startswith("https://github.com") or path.startswith("git@"):
        import git as gitpython

        from amx.docs.scanner import normalize_github_url

        clone_url = normalize_github_url(path)
        dest = tempfile.mkdtemp(prefix="amx_code_")
        log.info("Cloning %s → %s", clone_url, dest)
        gitpython.Repo.clone_from(clone_url, dest, depth=1)
        return dest
    return path


_SPARK_READ_TABLE = re.compile(
    r"(?:spark|session)\s*\.\s*read\s*\.\s*table\s*\(\s*[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
_SPARK_TABLE_CALL = re.compile(
    r"\.table\s*\(\s*[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
_SQL_QUALIFIED = re.compile(
    r"[`\"'\[]\s*([A-Za-z_][\w]*)\s*\.\s*([A-Za-z_][\w]*)\s*[`\"'\]]",
)


def _catalog_match(token: str, assets: set[str]) -> bool:
    tl = token.strip().lower()
    if not tl:
        return False
    if tl in assets:
        return True
    if "." in tl:
        tail = tl.rsplit(".", 1)[-1]
        if tail in assets:
            return True
    return False


def _append_ref(
    bucket: dict[str, list[CodeReference]],
    key: str,
    ref: CodeReference,
) -> None:
    bucket.setdefault(key, []).append(ref)


def merge_codebase_reports(
    left: CodebaseReport | None,
    right: CodebaseReport,
) -> CodebaseReport:
    """Merge two scans (e.g. multiple roots in one run). Preserves order within each key."""
    if left is None:
        return right
    out = CodebaseReport(
        path=f"{left.path};{right.path}",
        total_files=left.total_files + right.total_files,
        scanned_files=left.scanned_files + right.scanned_files,
    )
    for k, v in left.references.items():
        out.references[k] = list(v)
    for k, v in left.external_mentions.items():
        out.external_mentions[k] = list(v)
    for k, v in right.references.items():
        out.references.setdefault(k, []).extend(v)
    for k, v in right.external_mentions.items():
        out.external_mentions.setdefault(k, []).extend(v)
    return out


def _sqlglot_ident_part(node: object | None) -> str | None:
    if node is None:
        return None
    if isinstance(node, str):
        return node
    name = getattr(node, "name", None)
    if isinstance(name, str) and name:
        return name
    this = getattr(node, "this", None)
    if this is not None and this is not node:
        return _sqlglot_ident_part(this)
    return str(node) if node else None


def _scan_sqlglot_sql_file(
    rel_file: str,
    lines: list[str],
    context_lines: int,
    assets: set[str],
    catalog_tables: frozenset[str],
    references: dict[str, list[CodeReference]],
    external_mentions: dict[str, list[CodeReference]],
) -> None:
    """Optional richer SQL table mentions when ``sqlglot`` is installed (``pip install 'amx[code-intel]'``)."""
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return

    text = "\n".join(lines)
    if len(text) > 300_000:
        return
    try:
        stmts = sqlglot.parse(text)
    except Exception:
        return

    seen: set[str] = set()
    for stmt in stmts:
        if stmt is None:
            continue
        for table in stmt.find_all(exp.Table):
            db = _sqlglot_ident_part(getattr(table, "db", None))
            nm = _sqlglot_ident_part(getattr(table, "name", None)) or _sqlglot_ident_part(
                getattr(table, "this", None)
            )
            if not nm:
                continue
            token = f"{db}.{nm}".lower() if db else nm.lower()
            if token in seen:
                continue
            seen.add(token)
            line_idx = max(0, (getattr(table, "meta", None) or {}).get("line", 1) - 1)
            if line_idx >= len(lines):
                line_idx = 0
            start = max(0, line_idx - context_lines)
            end = min(len(lines), line_idx + context_lines + 1)
            ctx = "\n".join(lines[start:end])
            ref = CodeReference(
                file=rel_file,
                line_no=line_idx + 1,
                line_text=lines[line_idx].strip() if lines else "",
                matched_asset=token,
                context=ctx,
            )
            tail = token.rsplit(".", 1)[-1]
            if _catalog_match(token, assets) or (catalog_tables and tail in catalog_tables):
                _append_ref(references, tail if tail in assets else token, ref)
            else:
                _append_ref(external_mentions, token, ref)


def _scan_spark_sql_literals_in_line(
    line: str,
    line_idx: int,
    rel_file: str,
    lines: list[str],
    context_lines: int,
    assets: set[str],
    catalog_tables: frozenset[str],
    references: dict[str, list[CodeReference]],
    external_mentions: dict[str, list[CodeReference]],
) -> None:
    start = max(0, line_idx - context_lines)
    end = min(len(lines), line_idx + context_lines + 1)
    ctx = "\n".join(lines[start:end])
    seen: set[str] = set()

    for rx in (_SPARK_READ_TABLE, _SPARK_TABLE_CALL):
        for m in rx.finditer(line):
            token = m.group(1).strip()
            if not token or token.lower() in seen:
                continue
            seen.add(token.lower())
            ref = CodeReference(
                file=rel_file,
                line_no=line_idx + 1,
                line_text=line.strip(),
                matched_asset=token.lower(),
                context=ctx,
            )
            key = token.lower()
            if _catalog_match(token, assets) or (
                catalog_tables and token.split(".")[-1].lower() in catalog_tables
            ):
                _append_ref(references, key, ref)
            else:
                _append_ref(external_mentions, key, ref)

    if rel_file.lower().endswith(".sql"):
        for m in _SQL_QUALIFIED.finditer(line):
            token = f"{m.group(1)}.{m.group(2)}"
            if token.lower() in seen:
                continue
            seen.add(token.lower())
            ref = CodeReference(
                file=rel_file,
                line_no=line_idx + 1,
                line_text=line.strip(),
                matched_asset=token.lower(),
                context=ctx,
            )
            if _catalog_match(token, assets):
                _append_ref(references, token.lower(), ref)
            else:
                _append_ref(external_mentions, token.lower(), ref)


def _scan_python_ast_strings(
    fpath: Path,
    rel_file: str,
    lines: list[str],
    context_lines: int,
    assets: set[str],
    catalog_tables: frozenset[str],
    references: dict[str, list[CodeReference]],
    external_mentions: dict[str, list[CodeReference]],
) -> None:
    src = "\n".join(lines)
    try:
        tree = ast.parse(src, filename=str(fpath))
    except SyntaxError:
        return

    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        s = node.value
        if len(s) < 3 or len(s) > 400 or "." not in s:
            continue
        line_idx = getattr(node, "lineno", 1) - 1
        if line_idx < 0 or line_idx >= len(lines):
            line_idx = 0
        start = max(0, line_idx - context_lines)
        end = min(len(lines), line_idx + context_lines + 1)
        ctx = "\n".join(lines[start:end])
        for m in re.finditer(r"\b([A-Za-z_][\w]*\.[A-Za-z_][\w]*)\b", s):
            token = m.group(1)
            tail = token.split(".")[-1].lower()
            ref = CodeReference(
                file=rel_file,
                line_no=line_idx + 1,
                line_text=lines[line_idx].strip() if lines else "",
                matched_asset=token.lower(),
                context=ctx,
            )
            if _catalog_match(token, assets) or tail in assets:
                _append_ref(references, tail if tail in assets else token.lower(), ref)
            elif catalog_tables and tail in catalog_tables:
                _append_ref(references, tail, ref)
            else:
                _append_ref(external_mentions, token.lower(), ref)


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
    *,
    known_catalog_tables: frozenset[str] | None = None,
    index_semantic: bool = False,
) -> CodebaseReport:
    local_path = _clone_if_remote(path)
    root = Path(local_path).expanduser().resolve()
    report = CodebaseReport(path=path)

    if not root.exists():
        raise RuntimeError(
            f"Codebase path does not exist: {root}. "
            "Use a full directory path, a https://github.com/... URL, or git@… — "
            "not a profile name (profile names are for /code /add-code-profile only)."
        )
    if not root.is_dir():
        raise RuntimeError(f"Codebase path must be a directory or Git URL, not a single file: {root}")

    catalog = known_catalog_tables or frozenset(t.lower() for t in table_names)

    ordered: list[str] = [t.lower() for t in sorted(table_names, key=len, reverse=True)]
    for c in sorted((column_names or []), key=lambda x: len(str(x)), reverse=True):
        cl = str(c).lower()
        if cl not in ordered:
            ordered.append(cl)
        if len(ordered) >= MAX_REGEX_ASSETS:
            break
    assets_list = ordered[:MAX_REGEX_ASSETS]
    assets: set[str] = set(assets_list)
    if not assets_list:
        pattern = re.compile(r"$^")
    else:
        pattern = re.compile(
            r"\b(" + "|".join(re.escape(a) for a in sorted(assets_list, key=len, reverse=True)) + r")\b",
            re.IGNORECASE,
        )

    code_files = [
        f for f in root.rglob("*")
        if f.is_file() and f.suffix.lower() in CODE_EXTENSIONS
    ]
    report.total_files = len(code_files)
    if report.total_files == 0:
        exts = ", ".join(sorted(CODE_EXTENSIONS))
        log.warning(
            "No scannable source files under %s (extensions: %s).",
            root,
            exts,
        )

    for fpath in code_files:
        try:
            lines = fpath.read_text(errors="replace").splitlines()
        except Exception:
            continue
        report.scanned_files += 1
        rel = str(fpath.relative_to(root))

        for i, line in enumerate(lines):
            for match in pattern.finditer(line):
                asset = match.group(1).lower()
                start = max(0, i - context_lines)
                end = min(len(lines), i + context_lines + 1)
                ctx = "\n".join(lines[start:end])

                ref = CodeReference(
                    file=rel,
                    line_no=i + 1,
                    line_text=line.strip(),
                    matched_asset=asset,
                    context=ctx,
                )
                report.references.setdefault(asset, []).append(ref)

            _scan_spark_sql_literals_in_line(
                line,
                i,
                rel,
                lines,
                context_lines,
                assets,
                catalog,
                report.references,
                report.external_mentions,
            )

        if fpath.suffix.lower() == ".py":
            _scan_python_ast_strings(
                fpath,
                rel,
                lines,
                context_lines,
                assets,
                catalog,
                report.references,
                report.external_mentions,
            )

        if fpath.suffix.lower() == ".sql":
            _scan_sqlglot_sql_file(
                rel,
                lines,
                context_lines,
                assets,
                catalog,
                report.references,
                report.external_mentions,
            )

    ext_n = sum(len(v) for v in report.external_mentions.values())
    log.info(
        "Scanned %d/%d files in %s, %d catalog hits, %d external-style mentions",
        report.scanned_files,
        report.total_files,
        path,
        sum(len(v) for v in report.references.values()),
        ext_n,
    )
    if index_semantic and report.total_files:
        try:
            from amx.codebase.code_rag import index_codebase_tree

            index_codebase_tree(root, report=report)
        except Exception as exc:
            log.warning("Semantic code index failed: %s", exc)
    return report
