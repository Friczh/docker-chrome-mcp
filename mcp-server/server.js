// Authenticated Streamable HTTP bridge exposing chrome-devtools-mcp remotely.
// Spawns a fresh chrome-devtools-mcp child per MCP session (attached to the
// same running Chrome via --browserUrl) rather than sharing one process
// across sessions — avoids the "already connected to a transport" crash
// that a single shared child hits under concurrent/reconnecting clients.
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import express from 'express';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

const AUTH_TOKEN = process.env.MCP_AUTH_TOKEN; // required before exposing publicly
const PORT = Number(process.env.PORT || 3001);
const CDP_URL = process.env.CDP_URL || 'http://localhost:9222';
const OUTPUT_DIR = process.env.OUTPUT_DIR || '/data/outputs';
const FILES_TOKEN = process.env.FILES_TOKEN;
const PUBLIC_BASE = process.env.PUBLIC_BASE || '';

// Custom tool, handled entirely by this server — never forwarded to the
// chrome-devtools-mcp child. For any URL directly reachable as plain
// content (a .js file, JSON, raw text), this fetches and saves it
// server-side. The file's bytes never pass through Claude's context —
// Claude gets back a short confirmation and a download URL instead of
// having to evaluate()/read the content and relay it itself.
const SAVE_URL_TOOL = {
  name: 'save_url_to_file',
  description: 'Fetch a URL directly on the server (bypassing the browser and Claude\'s context) and save its raw response body to disk. Use this instead of evaluate() + relaying content when you just need to capture a script, JSON, or text file that is reachable by its own URL — e.g. a .js file open in a tab. Returns a download link, not the file content.',
  inputSchema: {
    type: 'object',
    properties: {
      url: { type: 'string', description: 'URL to fetch' },
      filename: { type: 'string', description: 'Filename to save as (defaults to a name derived from the URL)' },
    },
    required: ['url'],
  },
};

async function handleSaveUrlToFile(args) {
  const { url, filename } = args;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Fetch failed: ${resp.status} ${resp.statusText}`);
  const buf = Buffer.from(await resp.arrayBuffer());
  const safeName = (filename || path.basename(new URL(url).pathname) || 'download').replace(/[^a-zA-Z0-9._-]/g, '_');
  const finalName = `${Date.now()}-${safeName}`;
  await writeFile(path.join(OUTPUT_DIR, finalName), buf);
  const tokenQs = FILES_TOKEN ? `?token=${FILES_TOKEN}` : '';
  return {
    content: [{
      type: 'text',
      text: `Saved ${buf.length} bytes to ${finalName}. Download: ${PUBLIC_BASE}/files/${finalName}${tokenQs}`,
    }],
  };
}

const sessions = new Map(); // sessionId -> { transport, child }

const app = express();

app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    console.log(`${req.method} ${req.path} -> ${res.statusCode} (${Date.now() - start}ms)`);
  });
  next();
});

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Mcp-Session-Id');
  res.header('Access-Control-Expose-Headers', 'Mcp-Session-Id');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.get('/', (req, res) => res.status(200).send('ok'));

// /files uses its own token, separate from MCP_AUTH_TOKEN — that one
// controls the entire browser and shouldn't be pasted into casual
// download links. This one only unlocks saved files.
app.use('/files', (req, res, next) => {
  if (!FILES_TOKEN) return next(); // no auth configured — do not expose publicly like this
  if (req.query.token === FILES_TOKEN) return next();
  res.status(401).send('unauthorized');
}, express.static(OUTPUT_DIR));

app.use((req, res, next) => {
  if (!AUTH_TOKEN) return next(); // no auth configured — do not expose publicly like this
  const headerOk = req.headers.authorization === `Bearer ${AUTH_TOKEN}`;
  const queryOk = req.query.token === AUTH_TOKEN;
  if (headerOk || queryOk) return next();
  res.status(401).json({ error: 'unauthorized' });
});

app.use(express.json({ limit: '50mb' }));

async function createSession() {
  const child = new StdioClientTransport({
    command: 'node_modules/.bin/chrome-devtools-mcp',
    args: [
      '--browserUrl', CDP_URL,
      '--no-usage-statistics', '--no-performance-crux',
      '--screenshotFormat', 'webp',
      '--screenshotQuality', '60',
      '--screenshotMaxWidth', '1280',
      '--screenshotMaxHeight', '800',
    ],
    env: { ...process.env, TMPDIR: OUTPUT_DIR },
  });

  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: () => randomUUID(),
    onsessioninitialized: (sessionId) => {
      sessions.set(sessionId, { transport, child });
      console.log(`New session ${sessionId}, ${sessions.size} active`);
    },
  });

  const TOOL_TIMEOUT_MS = 45_000;
  const pending = new Map(); // request id -> timeout handle
  const listRequests = new Set(); // request ids that were tools/list

  const cleanup = () => {
    if (transport.sessionId) sessions.delete(transport.sessionId);
    for (const t of pending.values()) clearTimeout(t);
    pending.clear();
    child.close().catch(() => {});
  };

  transport.onmessage = (msg) => {
    // Handle our own tool locally — never forwarded to the child.
    if (msg?.method === 'tools/call' && msg.params?.name === SAVE_URL_TOOL.name) {
      handleSaveUrlToFile(msg.params.arguments || {})
        .then((result) => transport.send({ jsonrpc: '2.0', id: msg.id, result }))
        .catch((err) => transport.send({
          jsonrpc: '2.0', id: msg.id,
          error: { code: -32002, message: `save_url_to_file failed: ${err.message}` },
        }))
        .catch((err) => console.error('transport.send error:', err));
      return;
    }

    if (msg?.method === 'tools/list' && msg.id !== undefined) {
      listRequests.add(msg.id);
    }

    if (msg?.id !== undefined && msg.method) {
      const timer = setTimeout(() => {
        pending.delete(msg.id);
        transport.send({
          jsonrpc: '2.0', id: msg.id,
          error: { code: -32001, message: `Tool call timed out after ${TOOL_TIMEOUT_MS / 1000}s; browser session was reset.` },
        }).catch(() => {});
        console.error(`Session ${transport.sessionId}: request ${msg.id} timed out, killing stuck child`);
        child.close().catch(() => {}); // triggers cleanup via child.onclose
      }, TOOL_TIMEOUT_MS);
      pending.set(msg.id, timer);
    }
    child.send(msg).catch((err) => console.error('child.send error:', err));
  };

  child.onmessage = (msg) => {
    if (msg?.id !== undefined && pending.has(msg.id)) {
      clearTimeout(pending.get(msg.id));
      pending.delete(msg.id);
    }
    if (msg?.id !== undefined && listRequests.has(msg.id) && msg.result?.tools) {
      listRequests.delete(msg.id);
      msg.result.tools = [...msg.result.tools, SAVE_URL_TOOL];
    }
    transport.send(msg).catch((err) => console.error('transport.send error:', err));
  };
  transport.onclose = cleanup;
  child.onclose = cleanup;
  transport.onerror = (err) => console.error('transport error:', err);
  child.onerror = (err) => console.error('child error:', err);

  await child.start();
  await transport.start();
  return transport;
}

app.post('/mcp', async (req, res) => {
  const sessionId = req.headers['mcp-session-id'];
  let transport;
  if (sessionId && sessions.has(sessionId)) {
    transport = sessions.get(sessionId).transport;
  } else if (!sessionId && isInitializeRequest(req.body)) {
    transport = await createSession();
  } else {
    res.status(400).json({ jsonrpc: '2.0', error: { code: -32000, message: 'Bad Request: No valid session ID provided' }, id: null });
    return;
  }
  await transport.handleRequest(req, res, req.body);
});

app.get('/mcp', async (req, res) => {
  const sessionId = req.headers['mcp-session-id'];
  const session = sessionId && sessions.get(sessionId);
  if (!session) return res.status(400).send('Invalid or missing session ID');
  await session.transport.handleRequest(req, res);
});

app.delete('/mcp', async (req, res) => {
  const sessionId = req.headers['mcp-session-id'];
  const session = sessionId && sessions.get(sessionId);
  if (!session) return res.status(400).send('Invalid or missing session ID');
  await session.transport.handleRequest(req, res);
});

app.listen(PORT, () => console.log(`MCP Streamable HTTP server listening on :${PORT}`));
