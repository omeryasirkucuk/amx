from amx.cli_support.slash_commands import _LLM_COMMANDS


def test_style_is_in_llm_namespace_and_cross_namespace():
    entry = next((c for c in _LLM_COMMANDS if c.head == "style"), None)
    assert entry is not None
    assert entry.namespace == "llm"
    assert entry.cross_namespace is True
