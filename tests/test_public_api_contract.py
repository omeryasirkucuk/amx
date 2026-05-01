"""Lock down the AMX public Python API contract.

Anything documented in ``docs/PUBLIC_API.md`` is a stable contract
once we ship 1.0. These tests fail loudly if a public name is
accidentally removed or renamed — turning a silent breakage into a
red CI run on the PR that introduced it.

What's covered:

* ``amx.__all__`` and the lazy ``amx.<name>`` surface.
* ``amx.core.__all__`` and every name re-exported through it.
* The ``CONFIG_SCHEMA_VERSION`` and ``ConfigSchemaTooNewError`` leak
  from ``amx.config`` because they're part of the on-disk schema
  contract (see ``docs/PUBLIC_API.md``).

What's NOT covered (deliberately): individual method signatures,
dataclass field shapes. Those evolve additively within a major
version; locking them down here would prevent legitimate growth.
"""

from __future__ import annotations

import unittest


class TopLevelAmxSurfaceTests(unittest.TestCase):
    def test_version_is_a_string(self) -> None:
        import amx

        self.assertIsInstance(amx.__version__, str)
        self.assertTrue(amx.__version__)

    def test_top_level_all_lists_expected_public_names(self) -> None:
        import amx

        self.assertEqual(
            set(amx.__all__),
            {"AMXApplication", "AbstractEntity", "UniversalMetadataAdapter", "__version__", "init"},
        )

    def test_lazy_attributes_resolve(self) -> None:
        import amx

        self.assertTrue(callable(amx.init))
        self.assertTrue(hasattr(amx.AMXApplication, "load"))
        self.assertTrue(amx.AbstractEntity.__name__ == "AbstractEntity")
        self.assertTrue(amx.UniversalMetadataAdapter.__name__ == "UniversalMetadataAdapter")

    def test_unknown_top_level_attribute_raises(self) -> None:
        import amx

        with self.assertRaises(AttributeError):
            _ = amx.NotAPublicSymbol  # type: ignore[attr-defined]


class CoreSurfaceTests(unittest.TestCase):
    """Every name in ``amx.core.__all__`` is part of the public API.

    This test fails if a public symbol is removed, renamed, or moved
    out of ``amx.core``. It does NOT fail when symbols are *added* to
    the surface — that's an additive (minor-version) change.
    """

    EXPECTED_PUBLIC_NAMES: frozenset[str] = frozenset(
        {
            "AMXApplication",
            "AbstractEntity",
            "AskToolbox",
            "LoopBasedAskAgent",
            "StateManager",
            "ToolAskResponse",
            "UniversalMetadataAdapter",
            "infer_table_metadata",
        }
    )

    def test_core_all_includes_every_documented_public_name(self) -> None:
        import amx.core as core

        missing = self.EXPECTED_PUBLIC_NAMES - set(core.__all__)
        self.assertFalse(
            missing,
            f"docs/PUBLIC_API.md lists names that are no longer in "
            f"amx.core.__all__: {sorted(missing)}",
        )

    def test_each_public_core_name_resolves(self) -> None:
        import amx.core as core

        for name in self.EXPECTED_PUBLIC_NAMES:
            with self.subTest(name=name):
                obj = getattr(core, name)
                self.assertIsNotNone(obj, f"{name} resolved to None")

    def test_core_unknown_attribute_raises(self) -> None:
        import amx.core as core

        with self.assertRaises(AttributeError):
            _ = core.NotAPublicSymbol  # type: ignore[attr-defined]


class ConfigSchemaContractTests(unittest.TestCase):
    """The on-disk config schema constants are part of the public contract.

    Although ``AMXConfig`` itself is internal (its dataclass shape
    isn't stable), :data:`CONFIG_SCHEMA_VERSION` and
    :exc:`ConfigSchemaTooNewError` leak through the on-disk schema
    contract documented in ``docs/PUBLIC_API.md`` and need to stay
    importable from ``amx.config`` across minor versions.
    """

    def test_schema_version_constant_is_an_int(self) -> None:
        from amx.config import CONFIG_SCHEMA_VERSION

        self.assertIsInstance(CONFIG_SCHEMA_VERSION, int)
        self.assertGreaterEqual(CONFIG_SCHEMA_VERSION, 1)

    def test_schema_error_is_a_runtime_error_subclass(self) -> None:
        from amx.config import ConfigSchemaTooNewError

        self.assertTrue(issubclass(ConfigSchemaTooNewError, RuntimeError))


class CoreModuleAllListsTests(unittest.TestCase):
    """Each module backing the public surface declares its own ``__all__``.

    A missing ``__all__`` means ``from amx.core.foo import *`` brings
    in helpers / private helpers, which then become *de facto* public
    on the next ``ruff format`` import sort. The explicit list pins
    what's intentionally public per module.
    """

    EXPECTED: dict[str, set[str]] = {
        "amx.core.application": {"AMXApplication"},
        "amx.core.ask_agent": {
            "AskToolbox",
            "LoopBasedAskAgent",
            "ReasoningTraceStep",
            "ToolAskResponse",
            "ToolResult",
        },
        "amx.core.inference": {"infer_table_metadata"},
        "amx.core.metadata": {
            "AbstractEntity",
            "LexicalSignal",
            "SemanticSignal",
            "StatisticalSignal",
            "StructuralSignal",
            "UniversalMetadataAdapter",
        },
        "amx.core.state": {"StateManager"},
    }

    def test_each_public_module_declares_all(self) -> None:
        import importlib

        for module_name, expected in self.EXPECTED.items():
            with self.subTest(module=module_name):
                mod = importlib.import_module(module_name)
                self.assertTrue(
                    hasattr(mod, "__all__"),
                    f"{module_name} should declare __all__ to pin its "
                    f"public surface; see docs/PUBLIC_API.md.",
                )
                missing = expected - set(mod.__all__)
                self.assertFalse(
                    missing,
                    f"{module_name}.__all__ is missing public names: {sorted(missing)}",
                )


if __name__ == "__main__":
    unittest.main()
