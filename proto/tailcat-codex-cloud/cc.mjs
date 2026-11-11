#!/usr/bin/env node
// Throwaway driver for #630: start real Codex Cloud VMs (new Cloud, the #476
// way) and run commands in them with command/exec. No model turn, no tokens.
//
//   node cc.mjs start N            start N threads at once, wait until connected
//   node cc.mjs exec I 'cmd' [s]   run cmd in VM I (0-based) via bash -lc
//   node cc.mjs put I local remote copy a small local file into VM I (base64)
//   node cc.mjs pair S C 'cmd' [s] run cmd in VM C with VM S's tailcat address
//                                  in $TC_ADDR (never printed)
//   node cc.mjs status             environment/status of every VM
//   node cc.mjs archive            thread/archive every thread (deletes VMs)
//
// State (thread and environment ids) lives in $STATE, default
// /tmp/tc630/threads.json, outside the repo. Never commit it.
import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';

const ENV_CONFIG_ID =
  process.env.ENV_CONFIG_ID ||
  'c53221b4-ece0-42c4-bcfe-3ad86086a86e~asenvcfg_fcfb89d41c3c81919a8e9d9e21205d6a';
const STATE = process.env.STATE || '/tmp/tc630/threads.json';
const URL = 'wss://codex-cloud-backend.chatgpt.com/';

function token() {
  return JSON.parse(fs.readFileSync(`${process.env.HOME}/.codex/auth.json`)).tokens.access_token;
}

async function connect() {
  const ws = new WebSocket(URL, ['codex-app-server', 'codex-client.browser', `openai-bearer.${token()}`]);
  const pending = new Map();
  const notes = [];
  let next = 1;
  await new Promise((res, rej) => {
    ws.onopen = res;
    ws.onerror = (e) => rej(new Error('ws error ' + (e.message || '')));
  });
  ws.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (m.id != null && pending.has(m.id)) {
      const { res } = pending.get(m.id);
      pending.delete(m.id);
      res(m);
    } else if (m.method) {
      notes.push(m);
    }
  };
  ws.onclose = (ev) => {
    for (const { rej } of pending.values()) rej(new Error(`ws closed ${ev.code}`));
    pending.clear();
  };
  const send = (method, params) =>
    new Promise((res, rej) => {
      const id = next++;
      pending.set(id, { res, rej });
      ws.send(JSON.stringify({ id, method, params }));
    });
  const init = await send('initialize', {
    clientInfo: { name: 'tigerstore-630', version: '0.0.1' },
    capabilities: { experimentalApi: true },
  });
  if (init.error) throw new Error(JSON.stringify(init.error));
  ws.send(JSON.stringify({ method: 'initialized' }));
  return { ws, send, notes, close: () => ws.close() };
}

const load = () => (fs.existsSync(STATE) ? JSON.parse(fs.readFileSync(STATE)) : []);
const save = (s) => {
  fs.mkdirSync(path.dirname(STATE), { recursive: true });
  fs.writeFileSync(STATE, JSON.stringify(s, null, 1));
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function envStatus(c, ids) {
  const r = await c.send('environment/status', { environmentIds: ids });
  return r.result ?? r;
}

async function start(n) {
  const c = await connect();
  const t0 = Date.now();
  const started = await Promise.all(
    Array.from({ length: n }, () =>
      c.send('thread/start', {
        environments: [{ environmentConfigId: ENV_CONFIG_ID, cwd: '/workspace' }],
        deferredEnvironment: true,
        config: { 'features.mcp_app_ui': true },
        pluginsMcp: { productSku: 'codex' },
        sandbox: 'danger-full-access',
        approvalPolicy: 'never',
      }),
    ),
  );
  const s = load();
  const mine = [];
  for (const r of started) {
    if (r.error) throw new Error(JSON.stringify(r.error));
    const threadId = r.result.thread.id;
    const environmentId =
      r.result.environmentId ??
      r.result.environments?.[0]?.environmentId ??
      r.result.thread.environments?.[0]?.environmentId ??
      JSON.stringify(r.result).match(/ccarenv_[A-Za-z0-9_\-]+/)?.[0];
    const e = { threadId, environmentId, startedAt: new Date().toISOString() };
    s.push(e);
    mine.push(e);
    console.log(`started thread ${threadId} env ${environmentId?.slice(0, 24)}…`);
  }
  save(s);
  for (;;) {
    const st = await envStatus(c, mine.map((e) => e.environmentId));
    const txt = JSON.stringify(st);
    const conn = (txt.match(/"connected"/g) || []).length;
    console.log(`${((Date.now() - t0) / 1000).toFixed(1)} s: ${conn}/${n} connected`);
    if (conn >= n) break;
    if (Date.now() - t0 > 300e3) {
      console.log(txt);
      throw new Error('not connected after 300 s');
    }
    await sleep(3000);
  }
  c.close();
}

async function exec(i, cmd, timeoutS = 600) {
  const s = load();
  const e = s[i];
  if (!e) throw new Error(`no VM ${i}`);
  const c = await connect();
  const r = await c.send('command/exec', {
    environmentId: e.environmentId,
    command: ['bash', '-lc', cmd],
    cwd: '/workspace',
    timeoutMs: timeoutS * 1000,
  });
  c.close();
  if (r.error) {
    console.error(JSON.stringify(r.error));
    process.exit(2);
  }
  process.stdout.write(r.result.stdout ?? '');
  process.stderr.write(r.result.stderr ?? '');
  process.exitCode = r.result.exitCode ?? 0;
}

// pair: read VM S's tailcat address (/tmp/tc/addr) and run cmd in VM C with
// it in $TC_ADDR. The address only passes through this process's memory.
async function pair(s, ci, cmd, timeoutS = 600) {
  const st = load();
  const c = await connect();
  const r = await c.send('command/exec', {
    environmentId: st[s].environmentId,
    command: ['bash', '-lc', 'cat /tmp/tc/addr'],
    cwd: '/workspace',
    timeoutMs: 30000,
  });
  c.close();
  const addr = (r.result?.stdout ?? '').trim();
  if (!/^tc[0-9a-z_-]+$/i.test(addr)) throw new Error(`VM ${s}: no tailcat address in /tmp/tc/addr`);
  await exec(ci, `export TC_ADDR='${addr}'\n${cmd}`, timeoutS);
}

async function put(i, local, remote) {
  const b64 = fs.readFileSync(local).toString('base64');
  if (b64.length > 500_000) throw new Error('file too big for put');
  await exec(i, `mkdir -p "$(dirname ${remote})" && echo '${b64}' | base64 -d > ${remote} && chmod +x ${remote} && echo put ${remote}`);
}

async function status() {
  const s = load();
  const c = await connect();
  console.log(JSON.stringify(await envStatus(c, s.map((e) => e.environmentId)), null, 1));
  c.close();
}

async function archive() {
  const s = load();
  const c = await connect();
  for (const e of s) {
    const r = await c.send('thread/archive', { threadId: e.threadId });
    console.log(`archive ${e.threadId}: ${JSON.stringify(r.result ?? r.error)}`);
  }
  c.close();
  save([]);
}

const [cmd, ...args] = process.argv.slice(2);
const run = { start: () => start(+args[0] || 2), exec: () => exec(+args[0], args[1], +args[2] || 600), put: () => put(+args[0], args[1], args[2]), pair: () => pair(+args[0], +args[1], args[2], +args[3] || 600), status, archive }[cmd];
if (!run) {
  console.error('usage: cc.mjs start N | exec I cmd [s] | put I local remote | status | archive');
  process.exit(1);
}
run().catch((err) => {
  console.error(err.stack || String(err));
  process.exit(1);
});
