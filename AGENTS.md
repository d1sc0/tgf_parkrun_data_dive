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

## Project Conventions

- Keep SQL logic in views and keep Astro components focused on rendering/filtering.
- For dashboard UI date display, use `dd-mm-yyyy` format (for example `toLocaleDateString('en-GB').split('/').join('-')`).
- Preserve existing behavior unless user requests functional change.
- Keep changes minimal and targeted.
