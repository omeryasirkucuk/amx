"""Internal mixin modules for ``SearchCatalog`` (v0.9.1 refactor).

The historical ``amx/search/catalog.py`` was a 2033-line
``SearchCatalog`` class with 53 methods spanning entity CRUD, sync
orchestration, search/find/ranking, join discovery, usage tracking,
and settings. v0.9.1 splits those clusters into mixin modules so each
file is a manageable size and each cluster is testable in isolation.

Public API is preserved — the ``SearchCatalog`` class still exposes
all 50 methods via Python MRO; the only change is that 47 of them
now live in mixin classes here.
"""

from amx.search._catalog.entity_crud import EntityCrudMixin
from amx.search._catalog.join import JoinMixin
from amx.search._catalog.search import SearchMixin
from amx.search._catalog.settings import SettingsMixin
from amx.search._catalog.sync import SyncMixin
from amx.search._catalog.usage import UsageMixin

__all__ = [
    "EntityCrudMixin",
    "JoinMixin",
    "SearchMixin",
    "SettingsMixin",
    "SyncMixin",
    "UsageMixin",
]
