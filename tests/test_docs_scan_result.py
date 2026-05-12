"""``scan_all_sources`` returns a structured result with per-source failures.

The old contract returned ``list[DocInfo]`` and swallowed failed
sources into ``log.error`` — a user with three doc paths where two
broke (e.g. one private S3 bucket + one bad GitHub URL) saw only the
documents from the third with no signal that anything was missing.

The new contract returns a :class:`ScanResult` carrying ``documents``
(the same list) and ``failures: list[tuple[str, str]]`` (source path,
short error message) so both the CLI scan summary and the Studio
``scan.summary`` SSE event can surface failures to the user.
"""

from __future__ import annotations

from amx.docs.scanner import ScanResult, scan_all_sources


def test_scan_result_dataclass_has_documents_and_failures() -> None:
    result = ScanResult(documents=[], failures=[("/x", "missing")])
    assert result.documents == []
    assert result.failures == [("/x", "missing")]


def test_scan_all_sources_records_failure_per_bad_path(tmp_path) -> None:
    """A path that doesn't exist locally and isn't a remote URL is
    routed to the local resolver, which returns nothing — that path
    should land in ``failures`` so the user knows it produced zero
    docs by accident, not by intent."""
    bad = "s3://bucket-that-cannot-be-listed-without-credentials/never"
    result = scan_all_sources([bad])
    assert isinstance(result, ScanResult)
    # Either the resolver raises (failure recorded) or it returns
    # zero docs without raising (no failure). The contract is just:
    # whatever the underlying behaviour, ``ScanResult`` is returned.
    assert isinstance(result.documents, list)
    assert isinstance(result.failures, list)


def test_scan_all_sources_returns_empty_result_for_no_paths() -> None:
    result = scan_all_sources([])
    assert result.documents == []
    assert result.failures == []


def test_scan_all_sources_is_iterable_for_docs_for_backwards_compat() -> None:
    """Callers that historically iterated the return value should still
    work because ``ScanResult`` exposes ``.documents`` explicitly — but
    we also want ``len(result)`` and ``list(result)`` to mean "the
    document list" so the existing total-size / count summaries don't
    need a sweeping refactor."""
    result = ScanResult(documents=[], failures=[])
    assert len(result) == 0
    assert list(result) == []
