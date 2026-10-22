# Current Status

Product: Altira Atlas, a real-assets intelligence and underwriting product.

Live:
- Canonical application: `https://atlas.ryanjameson.me`.
- `atlas.altiratech.com` and `farmland.altiratech.com` redirect browser GET traffic to the canonical host with path and query preserved.
- Both AltiraTech hosts continue serving `/api/*` for compatibility.
- Worker `farmland-terminal`, D1 `farmland-terminal-db`, Secrets Store bindings, anonymous sessions, and the daily `0 6 * * *` ingestion cron are unchanged.
- The portfolio action at `https://ryanjameson.me/projects/` points to the personal hostname.

Verification:
- Canonical production run `29579800756` deployed Worker version `517d8f40-cbb3-426e-939b-bca72d4122f5` and passed the complete smoke suite.
- DNS/TLS, both legacy redirects, both legacy health endpoints, desktop Chrome click-through, and the portfolio desktop/mobile automated suites pass.
- Exact evidence and rollback IDs are recorded in `docs/ATLAS_RYANJAMESON_DOMAIN_VERIFICATION_2026-07-17.md`.

Next:
- Prepared22October, not deployed: company-host web traffic redirects to the company homepage; personal-host app and company `/api/*` consumers remain.35 routing tests, types, frontend build and Worker dry-run pass. Release must account for the existing automatic smoke script's anonymous-session bootstrap POST.
- Keep both legacy routes during an observation window; retire them only after an API-client and rollback review.
- Product UX follow-up remains the Research Workspace queue-mode versus active-memo-mode first-time-user check.
