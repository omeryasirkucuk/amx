import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Vite config for the AMX visualizer SPA.
// `outDir` points at amx/web/static so the wheel ships the dist
// directly. `base: "/"` matches the FastAPI mount path. Dev server
// proxies /api/* into the local uvicorn so `npm run dev` works
// against `amx /visualize --no-open`.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
  build: {
    outDir: "../amx/web/static",
    emptyOutDir: true,
    sourcemap: false,
    target: "es2020",
    rollupOptions: {
      output: {
        manualChunks: {
          react: ["react", "react-dom", "react-router-dom"],
          query: ["@tanstack/react-query"],
        },
      },
    },
  },
  server: {
    port: 5173,
    host: "127.0.0.1",
    proxy: {
      "/api": {
        target: "http://127.0.0.1:47821",
        changeOrigin: true,
      },
    },
  },
});
