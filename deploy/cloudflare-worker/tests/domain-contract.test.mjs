import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

const workerRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function read(relativePath) {
  return readFile(path.join(workerRoot, relativePath), 'utf8');
}

test('canonical release binds the personal hostname and preserves both legacy domains', async () => {
  const [wrangler, worker] = await Promise.all([
    read('wrangler.toml'),
    read('src/index.ts'),
  ]);

  for (const hostname of [
    'atlas.ryanjameson.me',
    'atlas.altiratech.com',
    'farmland.altiratech.com',
  ]) {
    assert.match(
      wrangler,
      new RegExp(`\\{ pattern = "${hostname.replaceAll('.', '\\.')}", custom_domain = true \\}`),
      `missing custom domain route for ${hostname}`,
    );
  }

  assert.match(wrangler, /CANONICAL_HOST = "atlas\.ryanjameson\.me"/);
  assert.match(
    wrangler,
    /LEGACY_HOSTS = "atlas\.altiratech\.com,farmland\.altiratech\.com"/,
  );
  assert.doesNotMatch(wrangler, /^LEGACY_HOST =/m);

  assert.match(worker, /LEGACY_HOSTS\?: string;/);
  assert.match(worker, /\.split\(','\)/);
  assert.match(worker, /\.replace\(\/:\\d\+\$\/, ''\)/);
  assert.match(worker, /legacyHosts\.has\(reqHost\)/);
  assert.match(worker, /!c\.req\.path\.startsWith\('\/api\/'\)/);
});

test('operational defaults and visible canonical references use the personal hostname', async () => {
  const files = await Promise.all([
    read('scripts/check-domain-migration.sh'),
    read('scripts/smoke-release.sh'),
    read('scripts/backfill-top20.sh'),
    read('scripts/backfill-nass-bulk.mjs'),
    read('scripts/backfill-orchestrator.mjs'),
    read('scripts/ingest-industrial-eia.mjs'),
    read('src/services/ingest.ts'),
    read('README.md'),
    read('../../README.md'),
    read('../../frontend/src/app/shell.jsx'),
    read('../../frontend/src/shared/system.jsx'),
  ]);

  for (const contents of files) {
    assert.match(contents, /atlas\.ryanjameson\.me/);
  }

  const migrationCheck = files[0];
  assert.match(migrationCheck, /atlas\.altiratech\.com/);
  assert.match(migrationCheck, /farmland\.altiratech\.com/);
  assert.match(migrationCheck, /curl -sS -D - -o \/dev\/null/);
  assert.doesNotMatch(migrationCheck, /curl -sI/);
});

// Run the actual Worker locally with storage and assets stubbed; no provider calls.
const bundled = await build({
  absWorkingDir: workerRoot, entryPoints: ['src/index.ts'], bundle: true,
  write: false, format: 'esm', platform: 'browser', target: 'es2022',
});
const worker = (await import(`data:text/javascript;base64,${Buffer.from(bundled.outputFiles[0].text).toString('base64')}`)).default;
const personalHost = 'atlas.ryanjameson.me';
const legacyHosts = ['atlas.altiratech.com', 'farmland.altiratech.com'];
function fixture(overrides = {}) {
  const assets = [];
  return {
    assets,
    env: {
      CANONICAL_HOST: personalHost, LEGACY_HOSTS: legacyHosts.join(','),
      ENVIRONMENT: 'production',
      DB: { prepare() { assert.fail('Routing must not access storage'); } },
      ASSETS: { async fetch(request) {
        assets.push(request.url);
        return new Response('unchanged personal app', { headers: { 'Content-Type': 'text/html' } });
      } },
      ...overrides,
    },
  };
}
function request(host, path = '/', method = 'GET') {
  return new Request(`https://${host}${path}`, { method, headers: { Host: host } });
}

for (const host of legacyHosts) {
  for (const method of ['GET', 'HEAD', 'POST']) {
    for (const route of ['/', '/research?name=fixture', '/assets/app.js', '/api?legacy_redirect=1']) {
      test(`${method} ${host}${route} returns only the company homepage destination`, async () => {
        const { env, assets } = fixture();
        const response = await worker.fetch(request(host, route, method), env);
        assert.equal(response.status, 308);
        assert.equal(response.headers.get('Location'), 'https://altiratech.com/');
        assert.doesNotMatch(await response.text(), /ryanjameson|fixture|legacy_redirect/);
        assert.deepEqual(assets, []);
      });
    }
  }
  test(`${host} preserves CORS preflight and single-host legacy configuration`, async () => {
    const { env, assets } = fixture({ LEGACY_HOSTS: '', LEGACY_HOST: host });
    const preflight = await worker.fetch(request(host, '/', 'OPTIONS'), env);
    assert.equal(preflight.status, 204);
    assert.equal(preflight.headers.get('Location'), null);
    assert.equal((await worker.fetch(request(host.toUpperCase() + ':443'), env)).headers.get('Location'), 'https://altiratech.com/');
    assert.deepEqual(assets, []);
  });
  test(`${host} does not need a personal canonical destination to separate company web traffic`, async () => {
    const { env, assets } = fixture({ CANONICAL_HOST: undefined });
    const response = await worker.fetch(request(host, '/research?name=fixture'), env);
    assert.equal(response.status, 308);
    assert.equal(response.headers.get('Location'), 'https://altiratech.com/');
    assert.deepEqual(assets, []);
  });
}

for (const host of [...legacyHosts, personalHost]) {
  test(`${host} retains API aliases and never sends API requests to the homepage`, async () => {
    const { env, assets } = fixture();
    const health = await worker.fetch(request(host, '/api/v1/health?fixture=1'), env);
    assert.equal(health.status, 200);
    assert.equal(health.headers.get('Location'), null);
    assert.deepEqual(await health.json(), { status: 'ok', version: '0.3.0', runtime: 'cloudflare-workers' });
    for (const method of ['GET', 'POST']) {
      const unknown = await worker.fetch(request(host, '/api/unknown-fixture', method), env);
      assert.equal(unknown.status, 404);
      assert.equal(unknown.headers.get('Location'), null);
      assert.deepEqual(await unknown.json(), { error: 'Not found' });
    }
    assert.deepEqual(assets, []);
  });
}

for (const host of [personalHost, 'unrelated.example.test']) {
  test(`${host} keeps its existing non-API asset behavior`, async () => {
    for (const route of ['/', '/research?name=fixture', '/assets/app.js']) {
      const { env, assets } = fixture();
      const response = await worker.fetch(request(host, route), env);
      assert.equal(response.status, 200);
      assert.equal(response.headers.get('Location'), null);
      assert.equal(await response.text(), 'unchanged personal app');
      assert.deepEqual(assets, [`https://${host}${route}`]);
    }
  });
}
