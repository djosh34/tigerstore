#!/usr/bin/env node
// Runs a seed campaign of internal/sim on Codex Cloud VMs and reports it
// on GitHub (#501, docs/testing.md). Run on this host, not in CI:
//
//   node scripts/codex-campaign.mjs --commit <sha> --issue <n> --vms <n> <test>:<first>:<seeds> ...
//   node scripts/codex-campaign.mjs ... TestCampaign:<first>:fresh=<n>,aged=<n>,long=<n>
//   node scripts/codex-campaign.mjs --find-max-vms --issue <n>
//   node scripts/codex-campaign.mjs --dry-run --vms <n> <spec> ...
//
// Each VM is a thread in the owner's tigerstore environment. It gets two
// GPT-6 Luna turns at effort low with fixed prompts: one runs
// scripts/codex-campaign-vm.sh start with the VM's share of the plan,
// detached; the other, after a wait computed from that share and the
// seconds per seed measured on these VMs, runs its report. If the run is
// still going, one more report turn follows later. The script then posts
// one summary comment on the issue, files each new failure as a `bug`
// issue, sets the commit status `campaign` for a full campaign, and
// archives every thread it started, also after an error or an interrupt.
// No GitHub token goes into a VM.
//
// The endpoints are the ones the ChatGPT web app uses (#487, #476); they
// are not documented and can change.
import { execFileSync, spawn } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import readline from 'node:readline';
import { pathToFileURL } from 'node:url';

export const REPO = 'djosh34/tigerstore';
const ENVIRONMENT = 'tigerstore';
const MODEL = 'gpt-6-luna';
const EFFORT = 'low';
// Threads of this script carry this name, so a later run finds the ones
// an earlier run left.
const NAME = 'tigerstore-campaign';
// The owner's cap on VMs at once (plan review, round 14).
export const MAX_VMS = 100;
// Processes a VM runs at once: it has 4 CPUs (#476), and four processes
// give 3.1 times the seeds of one (#500).
export const PROCESSES = 4;
const AUTH = path.join(os.homedir(), '.codex', 'auth.json');
const STATE = path.join(os.homedir(), '.cache', 'tigerstore-campaign');

// --- Seed kinds: a copy of campaignKind in internal/sim/campaign_test.go.

// The costs of internal/sim, in fresh scenarios, and the shares by cost.
const COSTS = { fresh: 1, aged: 4 * 40, long: 150 };
const SHARES = { fresh: 80, aged: 15, long: 5 };
// TestCampaignKindCycleHash checks the Go side against the same hash.
export const CYCLE_SHA256 = 'b84598f8d62599e2f2b3331b667910dea986eb2aeeab2a38635def15acaea592';
// An aged seed is an image and this many forks (forksPerImage).
export const FORKS_PER_IMAGE = 4;
// Tests whose every seed is long; their seeds run alone, as on Actions.
const ALL_LONG = new Set(['TestCampaignShapeSeeds', 'TestKubernetesLoadLong']);

// campaignCycle walks the running-cost allocation until the deficits are
// zero again, as campaignCycle in Go does, and returns one letter a seed.
export function campaignCycle() {
  const kinds = ['fresh', 'aged', 'long'];
  const deficit = [0, 0, 0];
  let cycle = '';
  for (;;) {
    let kind = 0;
    for (let k = 1; k < kinds.length; k++) {
      const score = (i) => deficit[i] - (100 - SHARES[kinds[i]]) * COSTS[kinds[i]];
      if (score(k) > score(kind)) kind = k;
    }
    cycle += kinds[kind][0];
    for (let k = 0; k < kinds.length; k++) deficit[k] += SHARES[kinds[k]] * COSTS[kinds[kind]];
    deficit[kind] -= 100 * COSTS[kinds[kind]];
    if (deficit.every((d) => d === 0)) return cycle;
  }
}

let cycle;
// kindOf gives a seed's kind: fresh, aged or long. Seeds of tests other
// than TestCampaign are fresh unless the whole test is long.
export function kindOf(test, seed) {
  if (ALL_LONG.has(test)) return 'long';
  if (test !== 'TestCampaign' || seed === 0) return 'fresh';
  if (!cycle) {
    cycle = campaignCycle();
    const sum = crypto.createHash('sha256').update(cycle).digest('hex');
    if (sum !== CYCLE_SHA256) {
      throw new Error(`the copy of campaignKind has SHA-256 ${sum}, not internal/sim's ${CYCLE_SHA256}: update it`);
    }
  }
  return { f: 'fresh', a: 'aged', l: 'long' }[cycle[(seed - 1) % cycle.length]];
}

// Seconds a seed takes in one of a VM's four processes, measured on
// these VMs (docs/testing.md, Codex Cloud campaigns). An aged seed is
// its image and its forks. A long seed runs alone; it varies most.
const SECONDS = { fresh: 7, aged: 1120, long: 3600 };
const TEST_SECONDS = { TestSeeds: 3, TestMembershipFaults: 4 };
// The VM checks out the commit and builds the gosim binaries first.
const BUILD_SECONDS = 300;

export function secondsOf(test, kind) {
  return kind === 'fresh' && TEST_SECONDS[test] ? TEST_SECONDS[test] : SECONDS[kind];
}

// --- Plans.

// parseSpec reads <test>:<first>:<seeds>, or for TestCampaign
// <test>:<first>:fresh=<n>,aged=<n>,long=<n>, which takes the seeds from
// first on until each kind has its count (aged counts forks).
export function parseSpec(spec) {
  const [test, firstText, size, extra] = spec.split(':');
  if (!/^Test[A-Za-z0-9_]+$/.test(test ?? '') || !/^\d+$/.test(firstText ?? '') || !size || extra !== undefined) {
    throw new Error(`${spec}: want <test>:<first>:<seeds> or TestCampaign:<first>:fresh=<n>,aged=<n>,long=<n>`);
  }
  const first = Number(firstText);
  if (/^\d+$/.test(size)) return checkRange({ test, first, seeds: Number(size) });
  if (test !== 'TestCampaign') throw new Error(`${spec}: counts by kind are for TestCampaign`);
  const want = { fresh: 0, aged: 0, long: 0 };
  for (const part of size.split(',')) {
    const m = /^(fresh|aged|long)=(\d+)$/.exec(part);
    if (!m) throw new Error(`${spec}: ${part} is not fresh=<n>, aged=<n> or long=<n>`);
    want[m[1]] = Number(m[2]);
  }
  const have = { fresh: 0, aged: 0, long: 0 };
  let seed = first;
  while (have.fresh < want.fresh || have.aged < want.aged || have.long < want.long) {
    const kind = kindOf(test, seed++);
    have[kind] += kind === 'aged' ? FORKS_PER_IMAGE : 1;
  }
  return checkRange({ test, first, seeds: seed - first });
}

function checkRange(r) {
  if (r.seeds <= 0 || !Number.isSafeInteger(r.first + r.seeds)) {
    throw new Error(`${r.test}: ${r.seeds} seeds from ${r.first} is not a range of seeds`);
  }
  return r;
}

// counts gives how many fresh scenarios, aged forks and long seeds a
// range holds.
export function counts({ test, first, seeds }) {
  const c = { fresh: 0, aged: 0, long: 0 };
  for (let seed = first; seed < first + seeds; seed++) {
    const kind = kindOf(test, seed);
    c[kind] += kind === 'aged' ? FORKS_PER_IMAGE : 1;
  }
  return c;
}

// spans turns a list of seeds into [first, seeds, twice] spans of
// consecutive seeds, and spreads a run twice over 1% of them, as
// sim-campaign.py does for a process queue.
function spans(seeds) {
  const out = [];
  for (const seed of seeds) {
    const last = out.at(-1);
    if (last && last[0] + last[1] === seed) last[1]++;
    else out.push([seed, 1, 0]);
  }
  const total = seeds.length;
  const twice = Math.floor(total / 100);
  let before = 0;
  for (const span of out) {
    const after = before + span[1];
    span[2] = Math.ceil((after * twice) / total) - Math.ceil((before * twice) / total);
    before = after;
  }
  return out;
}

// plan splits the specs over vms VMs. Ordinary seeds go in contiguous
// pieces of about equal time to PROCESSES queues a VM; long seeds each
// run alone, one after the other, on VMs of their own, as many as their
// share of the time asks for. A VM runs its jobs one after the other.
export function plan(specs, vms) {
  if (!Number.isInteger(vms) || vms < 1 || vms > MAX_VMS) throw new Error(`--vms must be 1 to ${MAX_VMS}`);
  const tests = specs.map((s) => {
    const ordinary = [];
    const long = [];
    let seconds = 0;
    for (let seed = s.first; seed < s.first + s.seeds; seed++) {
      const kind = kindOf(s.test, seed);
      if (kind === 'long') long.push(seed);
      else {
        ordinary.push(seed);
        seconds += secondsOf(s.test, kind);
      }
    }
    return { ...s, ordinary, long, seconds };
  });
  const ordinarySeconds = tests.reduce((a, t) => a + t.seconds, 0);
  const longSeconds = tests.reduce((a, t) => a + t.long.length * SECONDS.long, 0);
  const longSeeds = tests.reduce((a, t) => a + t.long.length, 0);
  let longVMs = 0;
  if (longSeeds && !ordinarySeconds) longVMs = Math.min(vms, longSeeds);
  else if (longSeeds && vms > 1) {
    const share = Math.round((vms * longSeconds) / (longSeconds + ordinarySeconds / PROCESSES));
    longVMs = Math.max(1, Math.min(share, longSeeds, vms - 1));
  }
  const ordinaryVMs = ordinarySeconds ? vms - longVMs : 0;
  const out = Array.from({ length: ordinaryVMs + longVMs }, () => ({ jobs: [], seeds: 0, estimate: BUILD_SECONDS }));
  const queues = ordinaryVMs * PROCESSES;
  for (const t of tests) {
    if (!t.ordinary.length) continue;
    // Cut the test's ordinary seeds where the running time crosses each
    // queue's share.
    const pieces = Array.from({ length: queues }, () => []);
    let spent = 0;
    for (const seed of t.ordinary) {
      const s = secondsOf(t.test, kindOf(t.test, seed));
      const q = Math.min(queues - 1, Math.floor(((spent + s / 2) * queues) / t.seconds));
      pieces[q].push(seed);
      spent += s;
    }
    for (let v = 0; v < ordinaryVMs; v++) {
      const mine = pieces.slice(v * PROCESSES, (v + 1) * PROCESSES).filter((p) => p.length);
      if (!mine.length) continue;
      out[v].jobs.push({ test: t.test, processes: mine.map(spans) });
      out[v].seeds += mine.reduce((a, p) => a + p.length, 0);
      out[v].estimate += Math.max(...mine.map((p) => p.reduce((a, seed) => a + secondsOf(t.test, kindOf(t.test, seed)), 0)));
    }
  }
  // Long seeds go to the least loaded of their VMs, or with no VM of
  // their own, to the ordinary VMs.
  const longTargets = longVMs ? out.slice(ordinaryVMs) : out;
  for (const t of tests) {
    for (const seed of t.long) {
      const vm = longTargets.reduce((a, b) => (b.estimate < a.estimate ? b : a));
      let job = vm.jobs.find((j) => j.test === t.test && j.long);
      if (!job) vm.jobs.push((job = { test: t.test, long: true, processes: [[]] }));
      job.processes[0].push([seed, 1, 0]);
      vm.seeds++;
      vm.estimate += SECONDS.long;
    }
  }
  return out.filter((vm) => vm.jobs.length);
}

// vmPlan is the JSON the VM script runs: jobs of process queues of spans.
export function vmPlan(vm) {
  return JSON.stringify(vm.jobs.map((j) => ({ test: j.test, processes: j.processes })));
}

// --- Reports.

// parseReport reads the lines codex-campaign-vm.sh report prints.
export function parseReport(text) {
  const r = { done: false, exit: null, reported: {}, failures: [], known: [], campaign: [], build: [], moreFailures: 0 };
  const replays = {};
  for (const line of text.split('\n')) {
    const [word, ...rest] = line.split(' ');
    const arg = rest.join(' ');
    if (word === 'commit') r.commit = arg;
    else if (word === 'state') {
      r.done = rest[0] === 'done';
      if (r.done) r.exit = Number(rest[1]);
    } else if (word === 'elapsed') r.elapsed = Number(arg);
    else if (word === 'cpu') r.cpu = Number(arg);
    else if (word === 'reported') {
      const ranges = (rest[1] ?? '').split(',').filter(Boolean).map((x) => {
        const [a, b] = x.split('-').map(Number);
        return [a, b ?? a];
      });
      (r.reported[rest[0]] ??= []).push(...ranges);
    } else if (word === 'fail') r.failures.push({ test: rest[0], seed: Number(rest[1]), error: rest.slice(2).join(' ') });
    else if (word === 'replay') replays[`${rest[0]}/${rest[1]}`] = rest.slice(2).join(' ');
    else if (word === 'known') r.known.push({ test: rest[0], seed: Number(rest[1]), issue: rest[2] });
    else if (word === 'campaign') r.campaign.push({ test: rest[0], counts: rest.slice(1).join(' ') });
    else if (word === 'more-failures') r.moreFailures += Number(arg);
    else if (word === 'build') r.build.push(arg);
  }
  for (const f of r.failures) {
    f.replay = replays[`${f.test}/${f.seed}`] ?? `SIM_SEED=${f.seed} SIM_SEEDS=1 scripts/sim.sh ./internal/sim '^${f.test}$'`;
  }
  return r;
}

// campaignCounts sums the "key: n" counts of TestCampaign's count lines.
export function campaignCounts(lines) {
  const sum = {};
  for (const line of lines) {
    for (const m of line.split(';')[0].matchAll(/([a-z][a-z ,:-]*?): (\d+)(?=,|$)/g)) {
      const key = m[1].replace(/^, /, '');
      sum[key] = (sum[key] ?? 0) + Number(m[2]);
    }
  }
  return sum;
}

// missing gives the seeds of first..first+seeds-1 that no range covers,
// as ranges.
export function missing(first, seeds, ranges) {
  const out = [];
  let next = first;
  const end = first + seeds - 1;
  for (const [a, b] of [...ranges].sort((x, y) => x[0] - y[0])) {
    if (b < next) continue;
    if (a > end) break;
    if (a > next) out.push([next, Math.min(a - 1, end)]);
    next = Math.max(next, b + 1);
  }
  if (next <= end) out.push([next, end]);
  return out;
}

function covered(first, seeds, ranges) {
  return seeds - missing(first, seeds, ranges).reduce((a, [x, y]) => a + y - x + 1, 0);
}

export function rangeText(ranges) {
  return ranges.map(([a, b]) => (a === b ? `${a}` : `${a}–${b}`)).join(', ');
}

// signature is what failures of one cause share: the error without its
// numbers.
export function signature(f) {
  return `${f.test}: ${f.error.replace(/^seed \d+: /, '').replace(/\d+(\.\d+)?/g, 'N')}`;
}

// searchPhrase is the longest run of four or more words of an error with
// no digit in them, which an open issue about the same failure quotes.
export function searchPhrase(error) {
  const words = error.replace(/^seed \d+: /, '').split(/\s+/);
  let best = [];
  let run = [];
  for (const w of [...words, '0']) {
    if (/\d/.test(w) || /[^A-Za-z,.;:'()-]/.test(w)) {
      if (run.length > best.length) best = run;
      run = [];
    } else run.push(w.replace(/[,.;:()]+$/, '').replace(/^[(]+/, ''));
  }
  const phrase = best.join(' ');
  return best.length >= 4 && phrase.length >= 25 ? phrase : null;
}

// --- Codex Cloud.

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// refreshAuth has the codex CLI refresh ~/.codex/auth.json: its app
// server's account/read with refreshToken runs the normal refresh-token
// flow. A failure stops the campaign.
async function refreshAuth() {
  const server = spawn('codex', ['app-server'], { stdio: ['pipe', 'pipe', 'ignore'] });
  const lines = readline.createInterface({ input: server.stdout });
  const waiting = new Map();
  lines.on('line', (line) => {
    const m = JSON.parse(line);
    waiting.get(m.id)?.(m);
  });
  const exited = new Promise((_, reject) => server.on('exit', (code) => reject(new Error(`codex app-server exited with ${code}`))));
  exited.catch(() => {});
  let id = 0;
  const call = (method, params) =>
    Promise.race([
      exited,
      new Promise((resolve) => {
        waiting.set(++id, resolve);
        server.stdin.write(`${JSON.stringify({ id, method, params })}\n`);
      }),
    ]);
  try {
    await call('initialize', { clientInfo: { name: NAME, version: '1' } });
    server.stdin.write(`${JSON.stringify({ method: 'initialized' })}\n`);
    const r = await call('account/read', { refreshToken: true });
    if (r.error || r.result?.account?.type !== 'chatgpt') {
      throw new Error(`refreshing ~/.codex/auth.json through the codex CLI failed: ${JSON.stringify(r.error ?? r.result)}`);
    }
  } finally {
    server.kill();
  }
}

function auth() {
  const tokens = JSON.parse(fs.readFileSync(AUTH, 'utf8')).tokens;
  const payload = JSON.parse(Buffer.from(tokens.access_token.split('.')[1], 'base64url'));
  return { token: tokens.access_token, account: tokens.account_id, expires: payload.exp * 1000 };
}

let refreshing;
// token refreshes the access token when it expires within a day, so a
// campaign of days keeps working.
async function token() {
  if (auth().expires - Date.now() < 24 * 3600 * 1000) {
    refreshing ??= refreshAuth().finally(() => (refreshing = undefined));
    await refreshing;
  }
  return auth();
}

async function environmentConfig() {
  const { token: t, account } = await token();
  const r = await fetch('https://chatgpt.com/api/codex-cloud/v1/environment-configs?scope=user&limit=100&omitDraft=true', {
    headers: { Authorization: `Bearer ${t}`, 'ChatGPT-Account-Id': account },
  });
  if (!r.ok) throw new Error(`environment-configs answered ${r.status}`);
  const env = (await r.json()).data?.find((e) => e.name === ENVIRONMENT && e.is_owner);
  if (!env) throw new Error(`no environment named ${ENVIRONMENT} of the owner`);
  return env.id;
}

class CloudError extends Error {}

// Cloud is one WebSocket to the cloud's app server. A connection may drop
// at any time (#476), so every step that waits opens its own.
class Cloud {
  static async open() {
    const { token: t } = await token();
    const c = new Cloud();
    c.ws = new WebSocket('wss://codex-cloud-backend.chatgpt.com/', ['codex-app-server', 'codex-client.browser', `openai-bearer.${t}`]);
    c.waiting = new Map();
    c.listeners = new Set();
    c.id = 0;
    c.closed = new Promise((_, reject) => {
      c.ws.onclose = () => reject(new CloudError('the connection closed'));
    });
    c.closed.catch(() => {});
    c.ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m.id !== undefined && c.waiting.has(m.id)) c.waiting.get(m.id)(m);
      else for (const l of c.listeners) l(m);
    };
    await Promise.race([
      new Promise((resolve, reject) => {
        c.ws.onopen = resolve;
        c.ws.onerror = () => reject(new CloudError('could not connect'));
      }),
      sleep(60_000).then(() => {
        throw new CloudError('connecting timed out');
      }),
    ]);
    await c.call('initialize', { clientInfo: { name: NAME, version: '1' }, capabilities: { experimentalApi: true } });
    c.ws.send(JSON.stringify({ method: 'initialized' }));
    return c;
  }

  async call(method, params) {
    const id = ++this.id;
    const reply = new Promise((resolve) => this.waiting.set(id, resolve));
    this.ws.send(JSON.stringify({ id, method, params }));
    const m = await Promise.race([reply, this.closed, sleep(120_000).then(() => ({ error: { message: 'no answer in 2 minutes' } }))]);
    this.waiting.delete(id);
    if (m.error) throw new CloudError(`${method}: ${m.error.message ?? JSON.stringify(m.error)}`);
    return m.result;
  }

  close() {
    this.ws.close();
  }
}

async function withCloud(f) {
  const c = await Cloud.open();
  try {
    return await f(c);
  } finally {
    c.close();
  }
}

// Threads this run started and has not archived yet.
const live = new Set();

async function startThread(envConfig, name) {
  return withCloud(async (c) => {
    const r = await c.call('thread/start', {
      model: MODEL,
      sandbox: 'danger-full-access',
      approvalPolicy: 'never',
      environments: [{ environmentConfigId: envConfig, cwd: '/workspace' }],
      deferredEnvironment: true,
      config: { 'features.mcp_app_ui': true },
      pluginsMcp: { productSku: 'codex' },
    });
    const thread = { id: r.thread.id, environment: r.thread.environments[0].environmentId, tokens: 0, turns: 0, name };
    live.add(thread.id);
    await c.call('thread/name/set', { threadId: thread.id, name });
    return thread;
  });
}

// waitConnected waits until the thread's VM is up; a turn sent before
// finds no environment.
async function waitConnected(thread, limitMs) {
  const start = Date.now();
  while (Date.now() - start < limitMs) {
    try {
      const r = await withCloud((c) => c.call('environment/status', { environmentIds: [thread.environment] }));
      if (r.data?.[0]?.status === 'connected') return (Date.now() - start) / 1000;
    } catch (e) {
      if (!(e instanceof CloudError)) throw e;
    }
    await sleep(5_000);
  }
  throw new CloudError(`the VM was not up after ${limitMs / 1000} s`);
}

export function prompt(command) {
  return `Run this one shell command exactly as written, once, and do nothing else. Then reply with only its exit code.\n\n${command}`;
}

// turn sends one prompt and returns the turn's command runs once it
// completed.
async function turn(thread, text) {
  let turnId;
  const done = await withCloud(async (c) => {
    let finished;
    const completed = new Promise((resolve) => (finished = resolve));
    c.listeners.add((m) => {
      if (m.method === 'thread/tokenUsage/updated' && m.params.threadId === thread.id) {
        thread.tokens = Math.max(thread.tokens, m.params.tokenUsage.total.totalTokens);
      }
      if (m.method === 'turn/completed' && m.params.turn.id === turnId) finished(true);
    });
    await c.call('thread/resume', { threadId: thread.id });
    const r = await c.call('turn/start', {
      threadId: thread.id,
      effort: EFFORT,
      clientUserMessageId: crypto.randomUUID(),
      input: [{ type: 'text', text, text_elements: [] }],
    });
    turnId = r.turn.id;
    thread.turns++;
    return Promise.race([completed, c.closed.catch(() => false), sleep(10 * 60_000).then(() => false)]);
  });
  // Without the notification, read the turn until it ends.
  const start = Date.now();
  for (;;) {
    try {
      const r = await withCloud((c) => c.call('thread/turns/list', { threadId: thread.id, itemsView: 'full' }));
      const t = r.data.find((x) => x.id === turnId);
      if (t && t.status !== 'inProgress') {
        if (t.status !== 'completed') throw new CloudError(`turn ${turnId} ended ${t.status}: ${JSON.stringify(t.error)}`);
        return t.items.filter((i) => i.type === 'commandExecution');
      }
    } catch (e) {
      if (!(e instanceof CloudError)) throw e;
    }
    if (done === true && Date.now() - start > 60_000) throw new CloudError(`turn ${turnId} completed but cannot be read`);
    if (Date.now() - start > 20 * 60_000) throw new CloudError(`turn ${turnId} did not end in 20 minutes`);
    await sleep(15_000);
  }
}

// ran gives the output of the turn's run of command, which must have
// ended with exit code 0.
function ran(items, command) {
  const item = items.findLast((i) => i.command?.includes(command) && i.exitCode !== null);
  if (!item) throw new CloudError(`the turn did not run ${command}`);
  if (item.exitCode !== 0) throw new CloudError(`${command} exited ${item.exitCode}: ${item.aggregatedOutput?.slice(-2000)}`);
  return item.aggregatedOutput ?? '';
}

async function archive(id) {
  await withCloud((c) => c.call('thread/archive', { threadId: id }));
  live.delete(id);
}

async function archiveAll(ids) {
  const failed = [];
  for (const id of ids) {
    try {
      await archive(id);
    } catch (e) {
      failed.push(`${id}: ${e.message}`);
    }
  }
  if (failed.length) console.error(`could not archive:\n${failed.join('\n')}`);
}

// leftovers archives the threads of this script an earlier run left.
async function leftovers() {
  const ids = [];
  let cursor;
  do {
    const r = await withCloud((c) => c.call('thread/list', { limit: 100, cursor }));
    for (const t of r.data) if (t.name?.startsWith(NAME)) ids.push(t.id);
    cursor = r.nextCursor;
  } while (cursor);
  if (ids.length) console.log(`archiving ${ids.length} threads an earlier run left`);
  await archiveAll(ids);
}

async function usage() {
  const r = await withCloud((c) => c.call('account/rateLimits/read', {}));
  return { percent: r.rateLimits.primary?.usedPercent, credits: r.rateLimits.credits?.balance };
}

// lock keeps one campaign of this host at a time, so a run's start never
// archives the threads of a campaign still running.
function lock() {
  fs.mkdirSync(STATE, { recursive: true });
  const file = path.join(STATE, 'lock');
  try {
    const pid = Number(fs.readFileSync(file, 'utf8'));
    process.kill(pid, 0);
    throw new Error(`campaign ${pid} still runs on this host`);
  } catch (e) {
    if (e.code !== 'ENOENT' && e.code !== 'ESRCH') throw e;
  }
  fs.writeFileSync(file, `${process.pid}`);
  return () => fs.rmSync(file, { force: true });
}

// --- One VM.

async function runVM(vm, ctx) {
  const command = `cd /workspace/tigerstore && git fetch -q origin ${ctx.commit} && git checkout -q --detach ${ctx.commit} && scripts/codex-campaign-vm.sh start '${vmPlan(vm)}'`;
  const reportCommand = '/workspace/tigerstore/scripts/codex-campaign-vm.sh report';
  const log = (msg) => console.log(`vm ${vm.index}: ${msg}`);
  try {
    vm.thread = await startThread(ctx.env, `${NAME} ${ctx.run} vm ${vm.index}`);
    vm.boot = await waitConnected(vm.thread, 10 * 60_000);
    const started = ran(await turn(vm.thread, prompt(command)), 'codex-campaign-vm.sh start');
    const sum = crypto.createHash('sha256').update(vmPlan(vm)).digest('hex');
    if (!started.includes(sum)) throw new CloudError(`the VM got a different plan: ${started.trim()}`);
    vm.startedAt = Date.now();
    let wait = vm.estimate * 1.25 + 120;
    for (let report = 0; report < 2; report++) {
      log(`started; reports in ${Math.round(wait / 60)} minutes`);
      await sleep(wait * 1000);
      vm.report = parseReport(ran(await turn(vm.thread, prompt(reportCommand)), 'codex-campaign-vm.sh report'));
      if (vm.report.commit !== ctx.commit) throw new CloudError(`the VM runs ${vm.report.commit}, not ${ctx.commit}`);
      if (vm.report.done) break;
      // Still going: wait for what is left at the pace so far, and at
      // least for a running seed's real-time backstop.
      const elapsed = vm.report.elapsed ?? 0;
      const total = vm.estimate - BUILD_SECONDS;
      const did = Math.min(total, estimated(vm, vm.report));
      const left = did > 0 ? (elapsed * (total - did)) / did : total;
      wait = Math.max(left * 1.25, vm.jobs.some((j) => j.long) ? 180 * 60 : 65 * 60);
    }
    log(vm.report.done ? `done, exit ${vm.report.exit}` : 'still running at its last report');
  } catch (e) {
    vm.error = e.message;
    log(`failed: ${e.message}`);
  } finally {
    if (vm.thread) {
      try {
        await archive(vm.thread.id);
      } catch (e) {
        log(`archive failed: ${e.message}`);
      }
    }
  }
}

// estimated gives the seconds the seeds a report has would take by the
// planner's estimate.
function estimated(vm, report) {
  let s = 0;
  for (const job of vm.jobs) {
    const ranges = report.reported[job.test] ?? [];
    for (const q of job.processes) {
      for (const [first, n] of q) {
        for (let seed = first; seed < first + n; seed++) {
          if (ranges.some(([a, b]) => a <= seed && seed <= b)) s += secondsOf(job.test, kindOf(job.test, seed)) / (job.long ? 1 : PROCESSES);
        }
      }
    }
  }
  return s;
}

// --- GitHub.

function gh(args, input) {
  return execFileSync('gh', args, { encoding: 'utf8', input, stdio: ['pipe', 'pipe', 'inherit'] }).trim();
}

// openIssueFor finds an open issue that quotes the failure's error.
function openIssueFor(error) {
  const phrase = searchPhrase(error);
  if (!phrase) return null;
  const q = `repo:${REPO} is:issue is:open "${phrase.replaceAll('"', '')}"`;
  const found = JSON.parse(gh(['api', '-X', 'GET', 'search/issues', '-f', `q=${q}`, '-f', 'per_page=5']));
  return found.items?.[0]?.html_url ?? null;
}

function knownFailuresEmpty(commit) {
  const file = JSON.parse(gh(['api', `repos/${REPO}/contents/internal/sim/known-failures.txt?ref=${commit}`]));
  const text = Buffer.from(file.content, 'base64').toString('utf8');
  return !text.split('\n').some((l) => l.trim() && !l.startsWith('#'));
}

// The counts of a full campaign (#497).
export const FULL = { fresh: 10_000_000, aged: 50_000, long: 4_000 };

export function isFull(specs) {
  const c = { fresh: 0, aged: 0, long: 0 };
  for (const s of specs.filter((x) => x.test === 'TestCampaign')) {
    const k = counts(s);
    for (const key of Object.keys(c)) c[key] += k[key];
  }
  return c.fresh >= FULL.fresh && c.aged >= FULL.aged && c.long >= FULL.long;
}

// --- The campaign.

function duration(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.round((seconds % 3600) / 60);
  return h ? `${h} h ${m} min` : `${m} min`;
}

async function campaign(opts) {
  const specs = opts.specs;
  const vms = plan(specs, opts.vms).map((vm, index) => ({ ...vm, index }));
  const ctx = { commit: opts.commit, run: new Date().toISOString().replace(/[-:]/g, '').slice(0, 15), env: await environmentConfig() };
  await leftovers();
  const before = await usage();
  const start = Date.now();
  console.log(`campaign ${ctx.run} on ${ctx.commit}: ${vms.length} VMs`);
  await Promise.all(vms.map((vm) => runVM(vm, ctx)));
  const wall = (Date.now() - start) / 1000;
  const after = await usage();

  // Every requested seed must have a verdict.
  const reported = {};
  const failures = [];
  const known = [];
  const campaignLines = [];
  let cpu = 0;
  let more = 0;
  for (const vm of vms) {
    const r = vm.report;
    if (!r) continue;
    for (const [test, ranges] of Object.entries(r.reported)) (reported[test] ??= []).push(...ranges);
    failures.push(...r.failures.map((f) => ({ ...f, vm: vm.index })));
    known.push(...r.known);
    campaignLines.push(...r.campaign.map((c) => c.counts));
    cpu += r.cpu ?? 0;
    more += r.moreFailures;
  }
  let requested = 0;
  let got = 0;
  const gaps = [];
  for (const s of specs) {
    requested += s.seeds;
    got += covered(s.first, s.seeds, reported[s.test] ?? []);
    const m = missing(s.first, s.seeds, reported[s.test] ?? []);
    if (m.length) gaps.push(`${s.test} ${rangeText(m.slice(0, 20))}${m.length > 20 ? ', …' : ''}`);
  }
  const knownSeeds = new Set(known.map((k) => `${k.test}/${k.seed}`));
  const failed = failures.length + more;
  const passed = got - failed - knownSeeds.size;
  const counted = campaignCounts(campaignLines);
  const answered = counted['answered calls'];

  // File each failure: one issue for each cause, unless an open issue
  // already has it.
  const causes = new Map();
  for (const f of failures) {
    const key = signature(f);
    if (!causes.has(key)) causes.set(key, []);
    causes.get(key).push(f);
  }
  const filed = [];
  for (const [key, fs_] of causes) {
    let url = null;
    let found = false;
    if (!opts.noFile) {
      url = openIssueFor(fs_[0].error);
      found = Boolean(url);
      if (!url) {
        const body = [
          `A Codex Cloud campaign (\`scripts/codex-campaign.mjs\`, run ${ctx.run}) found this failure on ${ctx.commit}.`,
          '',
          `**Error:** ${fs_[0].error}`,
          '',
          `**Seeds of ${fs_[0].test}:** ${fs_.map((f) => f.seed).join(', ')}`,
          '',
          'Each replays alone, anywhere, at that commit:',
          '',
          ...fs_.slice(0, 10).map((f) => `    git checkout ${ctx.commit} && ${f.replay}`),
          '',
          `Signature: \`${key}\``,
        ].join('\n');
        url = gh(['issue', 'create', '--repo', REPO, '--label', 'bug', '--title', key.slice(0, 120), '--body-file', '-'], body);
      }
    }
    filed.push({ key, failures: fs_, url, found });
  }

  const full = isFull(specs);
  const clean = full && got === requested && failed === 0 && knownSeeds.size === 0 && knownFailuresEmpty(ctx.commit);
  const tokens = vms.reduce((a, vm) => a + (vm.thread?.tokens ?? 0), 0);
  const turns = vms.reduce((a, vm) => a + (vm.thread?.turns ?? 0), 0);
  const lines = [
    `## Codex Cloud campaign ${ctx.run} on ${ctx.commit}`,
    '',
    `\`node scripts/codex-campaign.mjs ${opts.argv.join(' ')}\``,
    '',
    `- **Seeds:** ${requested} requested (${specs.map((s) => `${s.test} ${s.first}–${s.first + s.seeds - 1}`).join('; ')}), ${got} reported, **${passed} passed, ${failed} failed**, ${knownSeeds.size} skipped as known failures, ${requested - got} missing.`,
    `- **Wall time:** ${duration(wall)} on ${vms.length} VMs.`,
    `- **Answered calls per core-second:** ${answered !== undefined && cpu ? (answered / cpu).toFixed(1) : 'not logged by these tests'} (${answered ?? '–'} answered calls, ${cpu} core-seconds of seeds).`,
    `- **Cost:** ${turns} GPT-6 Luna turns at effort ${EFFORT}, ${tokens} tokens; weekly usage ${before.percent}% before, ${after.percent}% after; credits ${before.credits} before, ${after.credits} after.`,
    `- **Full campaign:** ${full ? (clean ? 'yes, and clean: the commit status `campaign` is set to success' : 'yes, not clean: the commit status `campaign` is set to failure') : 'no, a short run: no commit status'}.`,
  ];
  if (Object.keys(counted).length) lines.push(`- **TestCampaign's counts:** ${Object.entries(counted).map(([k, v]) => `${k} ${v}`).join(', ')}.`);
  if (gaps.length) lines.push(`- **Missing seeds:** ${gaps.join('; ')}.`);
  lines.push('', '| VM | Thread | Seeds | Estimate | Seeds ran for | Core-seconds | Result |', '|---|---|---|---|---|---|---|');
  for (const vm of vms) {
    const r = vm.report;
    const result = vm.error ?? (r?.done ? `exit ${r.exit}, ${r.failures.length + r.moreFailures} failed` : 'still running at its last report');
    lines.push(`| ${vm.index} | \`${vm.thread?.id ?? '–'}\` | ${vm.seeds} | ${duration(vm.estimate)} | ${r?.elapsed !== undefined ? duration(r.elapsed) : '–'} | ${r?.cpu ?? '–'} | ${result.replaceAll('|', '\\|').slice(0, 300)} |`);
  }
  if (filed.length) {
    lines.push('', '| Failure | Seeds | Issue |', '|---|---|---|');
    for (const f of filed) {
      const seeds = f.failures.map((x) => x.seed);
      lines.push(`| ${f.failures[0].error.replaceAll('|', '\\|').slice(0, 200)} | ${seeds.slice(0, 20).join(', ')}${seeds.length > 20 ? ', …' : ''} | ${f.url ? `${f.url}${f.found ? ' (open)' : ' (new)'}` : 'not filed'} |`);
    }
    lines.push('', `Replay one: \`git checkout ${ctx.commit} && ${filed[0].failures[0].replay}\``);
  }
  if (known.length) lines.push('', `Skipped as known failures: ${known.map((k) => `${k.test} ${k.seed} (${k.issue})`).join(', ')}.`);
  const body = lines.join('\n');
  console.log(body);
  const url = gh(['issue', 'comment', String(opts.issue), '--repo', REPO, '--body-file', '-'], body);
  console.log(url);
  if (full) {
    gh([
      'api', `repos/${REPO}/statuses/${ctx.commit}`,
      '-f', `state=${clean ? 'success' : 'failure'}`,
      '-f', 'context=campaign',
      '-f', `target_url=${url}`,
      '-f', `description=${passed} of ${requested} seeds passed`,
    ]);
  }
  return got === requested && failed === 0 ? 0 : 1;
}

// findMaxVMs raises the number of VMs at once until VMs queue or fail,
// up to the cap: a VM that is not up within 5 minutes counts as queued.
// It runs no turn, so it costs no tokens.
async function findMaxVMs(opts) {
  const env = await environmentConfig();
  await leftovers();
  const run = new Date().toISOString().replace(/[-:]/g, '').slice(0, 15);
  const rows = [];
  let most = 0;
  for (const n of [10, 20, 40, 60, 80, MAX_VMS]) {
    const results = await Promise.all(
      Array.from({ length: n }, async (_, i) => {
        let thread;
        try {
          thread = await startThread(env, `${NAME} ${run} probe ${n}/${i}`);
          return { up: await waitConnected(thread, 5 * 60_000) };
        } catch (e) {
          return { error: e.message };
        }
      }),
    );
    // Every VM of the step was up at once before any is archived.
    await archiveAll([...live]);
    const up = results.filter((r) => r.up !== undefined).map((r) => r.up).sort((a, b) => a - b);
    const errors = [...new Set(results.filter((r) => r.error).map((r) => r.error))];
    rows.push(`| ${n} | ${up.length} | ${up.length ? `${up[0].toFixed(0)} / ${up[Math.floor(up.length / 2)].toFixed(0)} / ${up.at(-1).toFixed(0)}` : '–'} | ${errors.join('; ').replaceAll('|', '\\|') || '–'} |`);
    console.log(rows.at(-1));
    if (up.length < n) break;
    most = n;
  }
  const body = [
    `## Most Codex Cloud VMs at once: ${most}${most === MAX_VMS ? ' (the cap)' : ''}`,
    '',
    `\`node scripts/codex-campaign.mjs ${opts.argv.join(' ')}\`, run ${run}. Each step starts that many threads in the tigerstore environment at once and waits up to 5 minutes for every VM to be up; a VM not up by then counts as queued. Every thread was archived after its step.`,
    '',
    '| VMs | Up | Seconds to up, min / median / max | Errors |',
    '|---|---|---|---|',
    ...rows,
  ].join('\n');
  console.log(body);
  if (opts.issue) console.log(gh(['issue', 'comment', String(opts.issue), '--repo', REPO, '--body-file', '-'], body));
  return 0;
}

function parseArgs(argv) {
  const opts = { argv, specs: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const value = () => {
      if (i + 1 >= argv.length) throw new Error(`${a} needs a value`);
      return argv[++i];
    };
    if (a === '--commit') opts.commit = value();
    else if (a === '--issue') opts.issue = Number(value());
    else if (a === '--vms') opts.vms = Number(value());
    else if (a === '--dry-run') opts.dryRun = true;
    else if (a === '--no-file') opts.noFile = true;
    else if (a === '--find-max-vms') opts.findMax = true;
    else if (a.startsWith('-')) throw new Error(`unknown flag ${a}`);
    else opts.specs.push(parseSpec(a));
  }
  return opts;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.dryRun) {
    for (const [i, vm] of plan(opts.specs, opts.vms).entries()) {
      console.log(`vm ${i}: ${vm.seeds} seeds, about ${duration(vm.estimate)}: ${vmPlan(vm)}`);
    }
    return 0;
  }
  if (!opts.findMax && (!/^[0-9a-f]{40}$/.test(opts.commit ?? '') || !opts.issue || !opts.vms || !opts.specs.length)) {
    throw new Error('want --commit <full sha> --issue <n> --vms <n> and at least one <test>:<first>:<seeds>');
  }
  const unlock = lock();
  // Archive every thread this run started, also on an interrupt.
  const stop = async (signal) => {
    console.error(`${signal}: archiving ${live.size} threads`);
    await archiveAll([...live]);
    unlock();
    process.exit(130);
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  try {
    await refreshAuth();
    return opts.findMax ? await findMaxVMs(opts) : await campaign(opts);
  } finally {
    await archiveAll([...live]);
    unlock();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().then(
    (code) => process.exit(code),
    (e) => {
      console.error(e.message);
      process.exit(1);
    },
  );
}
