#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -gt 0 ]]; then
  CANONICAL_BASE="$1"
  shift
else
  CANONICAL_BASE="https://atlas.ryanjameson.me"
fi

if [[ "$#" -gt 0 ]]; then
  LEGACY_BASES=("$@")
else
  LEGACY_BASES=(
    "https://atlas.altiratech.com"
    "https://farmland.altiratech.com"
  )
fi

COMPANY_BASE="https://altiratech.com"

echo "Checking company web separation and API compatibility"
echo "  canonical: ${CANONICAL_BASE}"

for legacy_base in "${LEGACY_BASES[@]}"; do
  echo "  legacy:    ${legacy_base}"

  # Use a browser-equivalent GET. Cloudflare Workers Assets can answer HEAD
  # requests directly even when run_worker_first is enabled.
  legacy_headers="$(curl -sS -D - -o /dev/null "${legacy_base}/")"
  legacy_status="$(printf '%s' "${legacy_headers}" | awk 'NR==1{print $2}')"
  legacy_location="$(printf '%s' "${legacy_headers}" | awk 'BEGIN{IGNORECASE=1}/^location:/{print $2}' | tr -d '\r')"

  if [[ "${legacy_status}" != "308" ]]; then
    echo "ERROR: expected ${legacy_base} root status 308, got ${legacy_status}"
    exit 1
  fi
  if [[ "${legacy_location}" != "${COMPANY_BASE}/" ]]; then
    echo "ERROR: expected ${legacy_base} redirect location ${COMPANY_BASE}/, got ${legacy_location}"
    exit 1
  fi

  legacy_api_status="$(curl -s -o /dev/null -w '%{http_code}' "${legacy_base}/api/v1/health")"
  if [[ "${legacy_api_status}" != "200" ]]; then
    echo "ERROR: expected ${legacy_base} API health status 200, got ${legacy_api_status}"
    exit 1
  fi
done

canonical_status="$(curl -s -o /dev/null -w '%{http_code}' "${CANONICAL_BASE}/")"
if [[ "${canonical_status}" != "200" ]]; then
  echo "ERROR: expected canonical root status 200, got ${canonical_status}"
  exit 1
fi

home_status="$(curl -s -o /dev/null -w '%{http_code}' "${CANONICAL_BASE}/altiratech-home")"
if [[ "${home_status}" != "200" ]]; then
  echo "ERROR: expected canonical /altiratech-home status 200, got ${home_status}"
  exit 1
fi

echo "OK: migration behavior looks correct."
