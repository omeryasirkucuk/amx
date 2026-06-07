// Vitest configuration for the pure-logic unit tests. Node
// environment only — modules under test never import vscode.
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/unit/**/*.test.ts"],
  },
});
