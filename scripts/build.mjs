import * as esbuild from "esbuild"
import { spawnSync } from "node:child_process"
import { existsSync } from "node:fs"
import { rm } from "node:fs/promises"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const dist = join(root, "dist")

async function removeDist(dir) {
  if (!existsSync(dir)) return
  const attempts = 8
  for (let i = 1; i <= attempts; i++) {
    try {
      await rm(dir, { recursive: true, force: true, maxRetries: 8, retryDelay: 100 })
      return
    } catch (e) {
      const code = e && typeof e === "object" && "code" in e ? e.code : ""
      const retry =
        code === "ENOTEMPTY" ||
        code === "EBUSY" ||
        code === "EPERM" ||
        code === "EACCES" ||
        code === "EMFILE"
      if (retry && i < attempts) {
        await new Promise((r) => setTimeout(r, 100 * i))
        continue
      }
      throw e
    }
  }
}

await removeDist(dist)

const tsc = spawnSync("tsc", ["--emitDeclarationOnly"], {
  cwd: root,
  stdio: "inherit",
  shell: true,
})

if (tsc.status !== 0) {
  process.exit(tsc.status ?? 1)
}

await esbuild.build({
  absWorkingDir: root,
  entryPoints: [join(root, "src/cli.ts"), join(root, "src/index.ts")],
  bundle: true,
  platform: "node",
  target: "node18",
  format: "esm",
  outdir: dist,
  sourcemap: true,
  external: [
    "better-sqlite3",
    "dotenv",
    "dotenv/config",
    "oracledb",
    "pg",
    "mysql2",
    "mysql2/promise",
    "mssql",
    "mongodb",
    "redis",
  ],
})
