# tests/storage/test_shared_lineage_schema.py
from amx.storage.shared_schema import build_metadata


def test_lineage_artifacts_table_exists():
    md = build_metadata(schema="AMX")
    assert "AMX.lineage_artifacts" in md.tables


def test_lineage_artifacts_has_required_columns():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_artifacts"]
    expected = {
        "id", "name", "db_profile", "anchor_entity_ref",
        "depth_up", "depth_down", "format", "output_path",
        "edge_set_hash", "node_count", "edge_count",
        "generated_at", "extractors_used", "extractors_partial",
        "canvas_meta",
        "created_by", "hostname", "client_version",
        "created_at", "updated_at", "local_id",
    }
    actual = {c.name for c in table.columns}
    assert expected <= actual, f"missing: {expected - actual}"


def test_lineage_artifacts_indexes():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_artifacts"]
    idx_names = {idx.name for idx in table.indexes}
    assert "ix_lineage_artifacts_db_profile" in idx_names
    assert "ix_lineage_artifacts_local_lookup" in idx_names
    assert "ix_lineage_artifacts_name_profile" in idx_names


def test_lineage_artifact_nodes_table_exists():
    md = build_metadata(schema="AMX")
    assert "AMX.lineage_artifact_nodes" in md.tables


def test_lineage_artifact_nodes_has_required_columns():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_artifact_nodes"]
    expected = {
        "id", "artifact_id", "entity_ref", "entity_kind", "db_profile",
        "x", "y", "width", "height", "z_index",
        "display_label", "column_list_json", "logo_key", "custom_style_json",
        "created_by", "hostname", "client_version",
        "created_at", "updated_at", "local_id",
    }
    actual = {c.name for c in table.columns}
    assert expected <= actual, f"missing: {expected - actual}"


def test_lineage_artifact_edges_table_exists():
    md = build_metadata(schema="AMX")
    assert "AMX.lineage_artifact_edges" in md.tables


def test_lineage_artifact_edges_has_required_columns():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_artifact_edges"]
    expected = {
        "id", "artifact_id", "source_node_id", "target_node_id",
        "edge_kind", "join_type", "on_condition", "where_clause",
        "source_columns_json", "target_columns_json",
        "label", "style_json", "waypoints_json",
        "created_by", "hostname", "client_version",
        "created_at", "updated_at", "local_id",
    }
    actual = {c.name for c in table.columns}
    assert expected <= actual, f"missing: {expected - actual}"


def test_lineage_comments_table_exists():
    md = build_metadata(schema="AMX")
    assert "AMX.lineage_comments" in md.tables


def test_lineage_comments_has_attribution_columns():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_comments"]
    actual = {c.name for c in table.columns}
    for expected_col in ("created_by", "hostname", "client_version",
                         "created_at", "updated_at", "local_id"):
        assert expected_col in actual, f"missing attribution: {expected_col}"
