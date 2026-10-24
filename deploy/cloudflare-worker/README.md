# Altira Atlas Cloudflare Deploy Profile

Cloudflare Workers deployment profile for Altira Atlas.

## Purpose
- Host API + static frontend on Cloudflare Workers/D1.
- Keep deployment-specific config isolated from desktop/local app runtime.

## Commands
- Install deps: `npm install`
- Local dev: `npm run dev`
- Deploy: `npm run deploy`
- Domain migration check: `./scripts/check-domain-migration.sh`
- Production smoke checks: `./scripts/smoke-release.sh`
- Top-20 backfill: `./scripts/backfill-top20.sh [start_year] [end_year] [chunk_size]`
- Bulk NASS backfill (official file download): `node ./scripts/backfill-nass-bulk.mjs --start-year 2005 --end-year 2026`
- Backfill orchestrator (retry + resume): `node ./scripts/backfill-orchestrator.mjs --start-year 2005 --end-year 2026 --resume true --max-retries 3`

## Production Backfill Setup
1. Configure an ingest admin secret on the Worker (never commit this in `wrangler.toml`):
   - `npx wrangler secret put INGEST_ADMIN_TOKEN`
2. Add the same value to GitHub repo secret `ATLAS_INGEST_ADMIN_TOKEN` (repo: `altiratech/ATLAS`).
3. If optional edge protection such as Cloudflare Access is enabled for Atlas routes, create a service token in the Access app and add GitHub secrets:
   - `ATLAS_CF_ACCESS_CLIENT_ID`
   - `ATLAS_CF_ACCESS_CLIENT_SECRET`
4. Trigger workflow: `Actions` -> `Backfill Top-20 Atlas Data` -> `Run workflow`.
5. Optional local operator run:
   - `ATLAS_INGEST_ADMIN_TOKEN=\"<token>\" ATLAS_CF_ACCESS_CLIENT_ID=\"<id>\" ATLAS_CF_ACCESS_CLIENT_SECRET=\"<secret>\" ATLAS_BACKFILL_STATES=\"IA,IL,IN\" ./scripts/backfill-top20.sh 2005 2026 1`
6. Recommended default is `chunk_size=1` plus state-by-state orchestration (script now calls one state per request) to avoid long-run request failures.
7. Backfill script now has automatic fallback: if `state+year` NASS ingest fails, it retries that same state/year one series at a time (`nass_series=...`).

## Bulk Baseline Backfill (Recommended First Pass)
Use the dedicated bulk workflow to populate historical baseline from official USDA files, then keep API ingest for incremental updates.

1. Trigger workflow: `Actions` -> `Backfill NASS Bulk Baseline` -> `Run workflow`.
2. Defaults auto-discover latest NASS bulk files from `https://www.nass.usda.gov/datasets/`:
   - `qs.crops_<stamp>.txt.gz`
   - `qs.economics_<stamp>.txt.gz`
3. Optional explicit URLs can be supplied via workflow inputs (`crops_url`, `economics_url`).
4. Bulk loader writes through `/api/v1/ingest/bulk` using existing auth headers (`ATLAS_INGEST_ADMIN_TOKEN` and optional edge-protection service token headers).
5. Keep `Backfill Top-20 Atlas Data` for targeted API-driven deltas and retries, not first-time historical baseline.

## Backfill Orchestrator (Retry/Resume + Progress Ledger)
Use the orchestrator workflow when you want resumable year/state execution with status tracking and workflow summaries.

1. Trigger workflow: `Actions` -> `Atlas Backfill Orchestrator` -> `Run workflow`.
2. The workflow runs each `year + state` as an independent unit using the bulk loader.
3. Progress is persisted to `ingest_progress` in D1 (fields include `source`, `year`, `state`, `status`, `rows_total`, `inserted`, `skipped`, `attempts`, `last_error`).
4. Resume mode (`resume=true`) skips units already marked `success`.
5. Retries (`max_retries`) are enforced per unit, not per full run.
6. Each run publishes:
   - Job `GITHUB_STEP_SUMMARY` (success/failure totals and failed units table)
   - JSON artifact: `atlas-backfill-summary`

### Progress API
- `GET /api/v1/ingest/progress?source=USDA-NASS-BULK&start_year=2005&end_year=2006&states=IA,IL`
- `POST /api/v1/ingest/progress` (upsert one `source+year+state` record)
- Same auth as ingest endpoints:
  - `Authorization: Bearer <atlas_session_token>` or
  - `X-Atlas-Ingest-Token: <INGEST_ADMIN_TOKEN>`

## Ingest Endpoint Auth Modes
- Session mode (existing): `Authorization: Bearer <atlas_session_token>`
- Admin mode (new): `X-Atlas-Ingest-Token: <INGEST_ADMIN_TOKEN>`
- If the admin secret is not configured, the endpoint stays session-only.
- If optional edge protection such as Cloudflare Access protects the hostname, include service token headers (`CF-Access-Client-Id`, `CF-Access-Client-Secret`) for automation.
- Treat that edge protection as an operator/deployment layer, not as Atlas's long-term canonical end-user identity model.
- Ingest supports optional query scoping for backfill orchestration:
  - `states=IA,IL`
  - `nass_series=cash_rent,corn_yield`
  - `include_nass=0|1`
  - `include_fred=0|1`
  - `include_ag_index=0|1`

## Notes
- Canonical project root remains `Code/active/altira-atlas`.
- Canonical web domain: `https://atlas.ryanjameson.me`
- Legacy domain compatibility: `https://atlas.altiratech.com` and `https://farmland.altiratech.com` (`/api/*` remains active; web routes redirect to `https://altiratech.com/` without carrying their path or query to the company homepage)
- Do not commit `node_modules` or `.wrangler` state.
