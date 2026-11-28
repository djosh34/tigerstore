// Tests of scripts/codex-campaign.mjs that need no network:
//   node --test scripts/codex-campaign.test.mjs
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import test from 'node:test';

import {
  campaignCounts,
  checkOverlap,
  counts,
  isFull,
  kindOf,
  missing,
  parseReport,
  parseSpec,
  plan,
  searchPhrase,
  seedRanges,
  signature,
  vmPlan,
} from './codex-campaign.mjs';

test('the copy of campaignKind matches internal/sim', () => {
  // kindOf checks the cycle's SHA-256 against internal/sim's.
  assert.equal(kindOf('TestCampaign', 1), 'fresh');
  // TestCampaignLongSeeds and TestCampaignKindShares: seed 1052 is the
  // first long seed, seed 390 the first aged one, and 1 to 2,404 hold
  // 2,400 fresh, 3 aged and 1 long.
  assert.equal(kindOf('TestCampaign', 1052), 'long');
  assert.equal(kindOf('TestCampaign', 390), 'aged');
  assert.equal(kindOf('TestCampaign', 3546), 'long');
  assert.deepEqual(counts({ test: 'TestCampaign', first: 1, seeds: 2404 }), { fresh: 2400, aged: 12, long: 1 });
  assert.equal(kindOf('TestSeeds', 1052), 'fresh');
  assert.equal(kindOf('TestKubernetesLoadLong', 3), 'long');
});

test('specs by range and by counts', () => {
  assert.deepEqual(parseSpec('TestSeeds:1:400'), { test: 'TestSeeds', first: 1, seeds: 400 });
  assert.deepEqual(parseSpec('TestCampaign:1:long=1'), { test: 'TestCampaign', first: 1, seeds: 1052 });
  const c = parseSpec('TestCampaign:1:fresh=2400,aged=12,long=1');
  assert.deepEqual(counts(c), { fresh: 2400, aged: 12, long: 1 });
  assert.throws(() => parseSpec('TestSeeds:1:fresh=3'));
  assert.throws(() => parseSpec('seeds:1:3'));
  assert.throws(() => parseSpec('TestSeeds:1:0'));
});

test('the plan covers every seed once, long seeds alone', () => {
  const specs = [parseSpec('TestCampaign:1:4808'), parseSpec('TestSeeds:100:50')];
  for (const vms of [1, 2, 5, 11, 100]) {
    const seen = new Map();
    for (const vm of plan(specs, vms)) {
      assert.ok(vm.jobs.length > 0);
      for (const job of vm.jobs) {
        assert.ok(job.processes.length <= 4);
        if (job.long) assert.equal(job.processes.length, 1);
        for (const q of job.processes) {
          for (const [first, n, twice] of q) {
            assert.ok(twice <= n);
            for (let s = first; s < first + n; s++) {
              const key = `${job.test}/${s}`;
              assert.ok(!seen.has(key), `${key} twice in ${vms} VMs`);
              seen.set(key, true);
              assert.equal(kindOf(job.test, s) === 'long', Boolean(job.long), key);
            }
          }
        }
      }
    }
    assert.equal(seen.size, 4808 + 50, `${vms} VMs`);
  }
  // With VMs to spare, the long seeds get VMs of their own.
  const vms = plan([parseSpec('TestCampaign:1:4808')], 11);
  assert.deepEqual(vms.filter((vm) => vm.jobs.some((j) => j.long)).map((vm) => vm.jobs.length), [1, 1]);
  assert.throws(() => plan(specs, 101));
});

test('a queue runs 1% of its seeds twice, spread over its spans', () => {
  const [vm] = plan([parseSpec('TestCampaign:1000:1200')], 1);
  const job = vm.jobs.find((j) => !j.long);
  for (const q of job.processes) {
    const total = q.reduce((a, s) => a + s[1], 0);
    assert.equal(q.reduce((a, s) => a + s[2], 0), Math.floor(total / 100));
  }
  assert.equal(JSON.parse(vmPlan(vm))[0].test, 'TestCampaign');
});

test('a report of the VM script', () => {
  const r = parseReport(fs.readFileSync(new URL('./testdata/codex-campaign-report.txt', import.meta.url), 'utf8'));
  assert.equal(r.commit, '7c53d3b6114d0d86a674bf61db6cad25f607805f');
  assert.equal(r.cut, false);
  assert.equal(r.done, true);
  assert.equal(r.exit, 1);
  assert.equal(r.elapsed, 1234);
  assert.equal(r.cpu, 4321);
  assert.deepEqual(r.reported.TestCampaign, [[1, 244], [300, 300]]);
  assert.deepEqual(r.failures.map((f) => f.seed), [48, 50, 51, 52, 99]);
  assert.match(r.failures[3].error, /^seed 48: watch 1 was never created/);
  assert.equal(r.failures[0].replay, "SIM_SEED=48 SIM_SEEDS=1 scripts/sim.sh ./internal/sim '^TestCampaign$/^48$'");
  assert.equal(r.failures[4].replay, "SIM_TWICE=1 SIM_SEED=99 SIM_SEEDS=1 scripts/sim.sh ./internal/sim '^TestCampaign$/^99$'");
  assert.deepEqual(r.known, [{ test: 'TestCampaign', seed: 412, issue: 'https://github.com/djosh34/tigerstore/issues/785' }]);
  const c = campaignCounts(r.campaign.map((x) => x.counts));
  assert.equal(c['fresh: kubernetes'], 27);
  assert.equal(c['answered calls'], 5000);
  assert.equal(c['aged images with at least 3,000 leases'], 1);
  assert.equal(c['requested quorum loss, not lost'], 0);
  // A report missing its first lines was cut.
  const text = fs.readFileSync(new URL('./testdata/codex-campaign-report.txt', import.meta.url), 'utf8');
  assert.equal(parseReport(text.split('\n').slice(2).join('\n')).cut, true);
});

test('the VM script reports a real seed log', () => {
  // Seeds 47 (passed) and 48 (failed) of an Actions run of TestCampaign,
  // with the middle of seed 48's trace taken out.
  const dir = new URL('./testdata/codex-campaign-vm', import.meta.url).pathname;
  const out = execFileSync('bash', [new URL('./codex-campaign-vm.sh', import.meta.url).pathname, 'report'], {
    encoding: 'utf8',
    env: { ...process.env, CODEX_CAMPAIGN_DIR: dir },
  });
  const r = parseReport(out);
  assert.equal(r.cut, false);
  assert.equal(r.done, true);
  assert.equal(r.elapsed, 100);
  assert.equal(r.cpu, 400);
  assert.deepEqual(r.reported.TestCampaign, [[47, 48]]);
  assert.equal(r.failures.length, 1);
  assert.equal(r.failures[0].seed, 48);
  assert.match(r.failures[0].error, /^seed 48: watch 1 was never created; its last stream was on node 0/);
  assert.equal(campaignCounts(r.campaign.map((x) => x.counts))['answered calls'], 4321);
});

test('overlapping ranges and seed ranges', () => {
  assert.throws(() => checkOverlap([parseSpec('TestCampaign:1:100'), parseSpec('TestCampaign:50:10')]));
  checkOverlap([parseSpec('TestCampaign:1:100'), parseSpec('TestCampaign:101:10'), parseSpec('TestSeeds:1:100')]);
  assert.deepEqual(seedRanges([5, 1, 2, 3, 9]), [[1, 3], [5, 5], [9, 9]]);
});

test('missing seeds', () => {
  assert.deepEqual(missing(1, 10, [[1, 3], [5, 5], [9, 20]]), [[4, 4], [6, 8]]);
  assert.deepEqual(missing(1, 10, []), [[1, 10]]);
  assert.deepEqual(missing(5, 3, [[1, 100]]), []);
});

test('failures of one cause share a signature and a search phrase', () => {
  const a = { test: 'TestCampaign', error: 'seed 99: liveness: only 34 calls completed after healing' };
  const b = { test: 'TestCampaign', error: 'seed 7: liveness: only 2 calls completed after healing' };
  assert.equal(signature(a), signature(b));
  assert.equal(searchPhrase(a.error), 'calls completed after healing');
  assert.equal(searchPhrase('seed 1: test failed'), null);
});

test('only the counts of #497 are a full campaign', () => {
  assert.equal(isFull([parseSpec('TestCampaign:1:2404')]), false);
  assert.equal(isFull([parseSpec('TestCampaign:1:fresh=10000000,aged=50000,long=4000')]), true);
});
