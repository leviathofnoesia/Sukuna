# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains the Cloudflare Worker (TypeScript, ESM). Durable Objects live in `src/durable-objects/` and the main harness is `src/durable-objects/mahoraga-harness.ts`.
- `dashboard/` is the React + Vite UI for monitoring and control.
- `migrations/` holds D1 database migrations; `scripts/` contains setup helpers.
- `docs/` stores extended documentation. `wrangler.jsonc` is the Worker config; `.env.example` and `agent-config.example.json` are templates.

## Build, Test, and Development Commands
Run from the repo root unless noted:
- `npm run dev` — start the Worker locally with Wrangler.
- `npm run build` — run `tsc` for the Worker (type-checks).
- `npm run typecheck` — explicit no-emit type check.
- `npm run lint` — lint `src/` with ESLint.
- `npm run test` / `npm run test:run` — Vitest watch mode or one-shot.
- `npm run db:migrate` / `npm run db:migrate:remote` — apply D1 migrations locally or remotely.
- `npm run deploy` / `npm run deploy:production` — deploy the Worker.
- `npm run setup:access` — configure Cloudflare Access (see `README.md`).

Dashboard (from `dashboard/`):
- `npm run dev`, `npm run build`, `npm run preview`.

## Coding Style & Naming Conventions
- TypeScript (ESM) with strict compiler settings; keep files 2-space indented.
- Prefer existing naming patterns: `kebab-case` for files, `camelCase` for functions/vars, `PascalCase` for types/classes.
- Use the `@/*` path alias for imports from `src/` when helpful.

## Testing Guidelines
- Framework: Vitest. No coverage threshold is enforced.
- Recommended naming: `*.test.ts` or `*.spec.ts` near the code under test.
- Run locally with `npm run test` (watch) or `npm run test:run` (CI-style).

## Commit & Pull Request Guidelines
- Use Conventional Commits as seen in history: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `ci:`, `legal:`, and optional scopes like `fix(dashboard):`.
- PRs should include: a concise summary, testing performed, and screenshots for dashboard/UI changes. Call out any config or migration changes.

## Security & Configuration Tips
- Never commit secrets. Use `wrangler secret put` for API tokens and credentials.
- Keep new config options documented in `README.md` and mirrored in `.env.example` when applicable.
