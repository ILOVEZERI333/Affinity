import { defineConfig } from "vitest/config";
export default defineConfig({
  // tsconfig keeps jsx: "preserve" for Next; page.test.tsx renders to a string.
  esbuild: { jsx: "automatic" },
  test: { include: ["lib/**/*.test.ts", "app/**/*.test.tsx"] },
});
