# AGENTS

Repository-level guidance for AI coding agents working in this project.

## Scope

- Applies to the entire repository.
- Follow these rules unless a user request explicitly overrides them.

## 1. Safe Command Defaults

- Prefer non-destructive commands.
- Do not run destructive git commands (`git reset --hard`, `git checkout --`, history rewrites) unless explicitly requested.
- Prefer repository scripts over ad-hoc commands:
  - `npm run dev`
  - `npm run dashboard`
  - `npm run setup:bq`
  - `npm run publish:views`
  - `npm run sync:coordinates`
  - `npm run sync:weather`
  - `npm run sync:weather:latest`
- For scripted/agent terminal runs, prefer:
  - `DISABLE_AUTO_UPDATE=true <command>`

## 2. Documentation Expectations

When changing behavior, update docs in the same change set when relevant:

- Root overview and workflows: `README.md`
- Dashboard behavior: `dashboard/README.md`
- SQL/view behavior: `sql/bigquery/README.md`
- Operational runbook: `docs/repo-operations-reference.md`
- Coordinate caching details: `docs/event-coordinates-optimization.md`

## 3. BigQuery View Workflow

- SQL source of truth lives in `sql/bigquery/*.sql`.
- Publish updated SQL to views with:
  - `npm run publish:views`
- If visitor map coordinate logic changes, refresh coordinate table first:
  - `npm run sync:coordinates`
- Validate changed components after edits (lint/type/editor errors).

## 4. Secrets and Sensitive Data

- Never commit credentials, API tokens, or key material.
- Never paste secrets into docs, code comments, or commit messages.
- Treat `.env` and service-account files as local-only.
- Commit variable names only, never variable values.

## 5. SSR Result Caching

Dashboard components (RunReport.astro, TopLists.astro) use in-memory caching (6-hour TTL) to reduce BigQuery query volume:

- Cache is managed via `dashboard/src/lib/cache.ts` (getCached, setCached, clearCache, clearAllCache)
- RunReport caches per run*id: `runReport*${run_id}` (latest run data), `weather_${run_id}` (weather data)
- TopLists uses static key: `topLists_global` (global top-20 arrays)
- Cache TTL is 6 hours (360 minutes) since data only refreshes weekly on Monday
- Cache is cleared on application restart; no manual intervention required
- Expected impact: 80-90% reduction in query volume during traffic peaks

## Project Conventions

- Keep SQL logic in views and keep Astro components focused on rendering/filtering.
- For dashboard UI date display, use `dd-mm-yyyy` format (for example `toLocaleDateString('en-GB').split('/').join('-')`).
- Preserve existing behavior unless user requests functional change.
- Keep changes minimal and targeted.
