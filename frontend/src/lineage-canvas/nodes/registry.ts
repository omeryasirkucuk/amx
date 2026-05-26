/**
 * Single source of truth for ReactFlow's nodeTypes map.
 */

import { AssetNode } from "./AssetNode";
import { CommentNode } from "./CommentNode";
import { DataFrameNode } from "./DataFrameNode";
import { FilterNode } from "./FilterNode";
import { LogoNode } from "./LogoNode";
import { OperatorNode } from "./OperatorNode";

export const nodeTypes = {
  table: DataFrameNode,
  filter: FilterNode,
  operator: OperatorNode,
  comment: CommentNode,
  logo: LogoNode,
  notebook: AssetNode,
  query: AssetNode,
  stream: AssetNode,
  pipeline: AssetNode,
  streamlit_app: AssetNode,
  job: AssetNode,
  vector_search_index: AssetNode,
  dashboard: AssetNode,
  external: AssetNode,
};

/** Map AMX op_kind → ReactFlow node type. */
export function nodeTypeForOperator(opKind: string): "filter" | "operator" {
  return opKind === "filter" ? "filter" : "operator";
}
