/**
 * Single source of truth for ReactFlow's nodeTypes map.
 */

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
};

/** Map AMX op_kind → ReactFlow node type. */
export function nodeTypeForOperator(opKind: string): "filter" | "operator" {
  return opKind === "filter" ? "filter" : "operator";
}
