"""PR-C (scale): Snowflake listing cap + streaming behaviour.

The Snowflake adapter used to call ``.mappings().all()`` on
``SHOW NOTEBOOKS IN ACCOUNT`` — fine at 50 notebooks, RAM-thrashing
at 5,000. Listings now stream row by row and refuse to load more
than ``_REMOTE_LIST_HARD_CAP`` rows. The cap surfaces as a
structured ``RemoteListingTooLarge`` so the catalog layer can mark
the kind as failed rather than crash the whole ingest.
"""

import pytest

from amx.db.adapters.snowflake import RemoteListingTooLarge, _stream_with_cap


def test_stream_with_cap_passes_through_under_limit():
    rows = [{"i": i} for i in range(100)]
    out = list(_stream_with_cap(iter(rows), "notebooks"))
    assert out == rows


def test_stream_with_cap_raises_when_exceeded(monkeypatch):
    """Lower the cap so the test doesn't have to generate 10k rows."""
    monkeypatch.setattr("amx.db.adapters.snowflake._REMOTE_LIST_HARD_CAP", 50, raising=True)
    rows = ({"i": i} for i in range(60))
    with pytest.raises(RemoteListingTooLarge) as exc:
        list(_stream_with_cap(rows, "notebooks"))
    assert "more than 50 notebooks" in str(exc.value)


def test_stream_with_cap_is_lazy(monkeypatch):
    """Row N+1 should not be pulled until row N is consumed."""
    pulled: list[int] = []

    def gen():
        for i in range(100):
            pulled.append(i)
            yield {"i": i}

    iterator = _stream_with_cap(gen(), "notebooks")
    # Pull two rows; the generator must not have walked the whole input.
    first = next(iterator)
    second = next(iterator)
    assert first == {"i": 0}
    assert second == {"i": 1}
    assert len(pulled) == 2
