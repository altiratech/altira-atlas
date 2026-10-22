# Altira Atlas

Altira Atlas is a real-assets intelligence and underwriting product for land research, scenario modeling, and investment diligence, starting with farmland.

The current live playbook is **Farmland Income**. Atlas is intentionally specialist: it focuses on land, infrastructure, location, evidence quality, and investment underwriting rather than becoming a generic private-equity research terminal.

## Status

Active product build.

Current local and deployed surfaces support:
- playbook-centered navigation
- county screening, comparison, and detail views
- assumption-set-aware underwriting and scenario modeling
- research notes, saved views, portfolio context, and backtesting
- data-source transparency for observed, proxy, state, and national inputs
- a Cloudflare deployment profile for the hosted web app

Canonical hosted app:
- `https://atlas.ryanjameson.me`

Compatibility hostnames:
- `https://atlas.altiratech.com` and `https://farmland.altiratech.com` keep `/api/*` active while web routes redirect to `https://altiratech.com/`. The personal application remains available directly at its canonical hostname.

## Quick Start

```bash
git clone https://github.com/altiratech/altira-atlas.git
cd altira-atlas

# Native desktop window when pywebview is available
./run.sh

# Browser mode
./run.sh --browser

# API server only
./run.sh --server
```

The launcher installs Python dependencies, initializes schema-only local storage, and serves the app at `http://127.0.0.1:3000`.

## Requirements

- Python 3.9+
- Python dependencies from `requirements.txt`
- Optional: `pywebview` for the native desktop window

Manual install:

```bash
pip install -r requirements.txt
```

## Product Areas

- **Atlas Home**: choose a playbook, resume recent work, reopen saved views, and jump back into research or underwriting.
- **Playbook Home**: explains the current live universe and separates observed context from modeled interpretation.
- **Screener**: playbook-aware filters, evidence presets, saved views, and export.
- **County Detail**: valuation, evidence, underwriting, credit stress, and decision context in one place.
- **Comparison**: side-by-side county trust context and modeled comparison.
- **Research Workspace**: memo drafting, scenario history, and county read carry-through.
- **Scenario Lab**: unlevered and levered underwriting, refinance roll-forward, and lender stress.
- **Portfolio**: holdings management plus aggregated risk, soil, hazard, and stress synthesis.
- **Backtest**: historical replay of reusable saved-view filters.

## Architecture

```text
backend/                 FastAPI app, SQLAlchemy models, metric services, and tests
frontend/                React single-page app served from one HTML entry point
deploy/cloudflare-worker Cloudflare Workers, Hono, and D1 deployment profile
docs/                    Data and metric extension notes
launcher.py              Optional desktop launcher
run.sh                   Local launcher for desktop, browser, or API-only mode
```

Tech stack:
- FastAPI, SQLAlchemy, SQLite
- React 18 with a no-build local frontend path
- Cloudflare Workers, Hono, and D1 for the deploy profile
- pywebview for optional native desktop use

## Data Posture

Atlas follows a no-synthetic-data policy:
- runtime paths do not auto-seed fabricated production records
- local SQLite starts schema-only
- production data should come from live or source-backed ingestion paths
- unavailable data should be represented as explicit placeholders, not generated samples

See `docs/ADDING_DATA_AND_METRICS.md` for instructions on adding custom data and metrics.

## Validation

```bash
cd backend
python -m pytest tests/ -v
```

Expected baseline is 61 tests. One portfolio test can fail when a persistent local database already contains prior state.

## License

No open-source license has been selected yet. Public source visibility does not grant reuse rights until a license file is added.
