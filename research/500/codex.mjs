// A Codex Cloud VM as a runner, by the method of tigerstore #487 and #476:
//   node codex.mjs start            -> prints the thread's JSON
//   node codex.mjs status <envId>
//   node codex.mjs exec <envId> <bash command> [timeoutMs]
//   node codex.mjs archive <threadId>
import fs from 'node:fs';
import crypto from 'node:crypto';

const ENV_CONFIG = 'c53221b4-ece0-42c4-bcfe-3ad86086a86e~asenvcfg_fcfb89d41c3c81919a8e9d9e21205d6a';
const token = JSON.parse(fs.readFileSync(`${process.env.HOME}/.codex/auth.json`)).tokens.access_token;

function connect() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket('wss://codex-cloud-backend.chatgpt.com/',
      ['codex-app-server', 'codex-client.browser', `openai-bearer.${token}`]);
    let id = 0;
    const pending = new Map();
    ws.onmessage = (ev) => {
      const m = JSON.parse(ev.data);
      if (m.id !== undefined && pending.has(m.id)) {
        const p = pending.get(m.id);
        pending.delete(m.id);
        if (m.error) p.reject(new Error(JSON.stringify(m.error)));
        else p.resolve(m.result);
      }
    };
    ws.onerror = (e) => reject(new Error('websocket error ' + (e.message || '')));
    ws.onclose = (e) => {
      for (const p of pending.values()) p.reject(new Error(`websocket closed ${e.code}`));
    };
    const send = (method, params) => new Promise((res, rej) => {
      const i = ++id;
      pending.set(i, { resolve: res, reject: rej });
      ws.send(JSON.stringify({ id: i, method, params }));
    });
    ws.onopen = async () => {
      try {
        await send('initialize', { clientInfo: { name: 'tigerstore', version: '0.0.1' }, capabilities: { experimentalApi: true } });
        ws.send(JSON.stringify({ method: 'initialized' }));
        resolve({ send, close: () => ws.close() });
      } catch (e) { reject(e); }
    };
  });
}

const [cmd, ...args] = process.argv.slice(2);
const c = await connect();
try {
  if (cmd === 'start') {
    const r = await c.send('thread/start', {
      environments: [{ environmentConfigId: ENV_CONFIG, cwd: '/workspace' }],
      deferredEnvironment: true, config: { 'features.mcp_app_ui': true }, pluginsMcp: { productSku: 'codex' },
    });
    console.log(JSON.stringify(r));
  } else if (cmd === 'status') {
    console.log(JSON.stringify(await c.send('environment/status', { environmentIds: [args[0]] })));
  } else if (cmd === 'exec') {
    const r = await c.send('command/exec', {
      environmentId: args[0], command: ['bash', '-lc', args[1]], cwd: '/workspace/tigerstore',
      timeoutMs: Number(args[2] || 600000),
    });
    process.stdout.write(r.stdout || '');
    process.stderr.write(r.stderr || '');
    process.exitCode = r.exitCode;
  } else if (cmd === 'archive') {
    console.log(JSON.stringify(await c.send('thread/archive', { threadId: args[0] })));
  } else {
    throw new Error('unknown command ' + cmd);
  }
} finally {
  c.close();
}
