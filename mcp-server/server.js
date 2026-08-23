// Authenticated Streamable HTTP bridge exposing chrome-devtools-mcp remotely.
// Spawns a fresh chrome-devtools-mcp child per MCP session (attached to the
// same running Chrome via --browserUrl) rather than sharing one process
// across sessions — avoids the "already connected to a transport" crash
// that a single shared child hits under concurrent/reconnecting clients.
import { randomUUID } from 'node:crypto';
import express from 'express';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

const AUTH_TOKEN = process.env.MCP_AUTH_TOKEN; // required before exposing publicly
const PORT = Number(process.env.PORT || 3001);
const CDP_URL = process.env.CDP_URL || 'http://localhost:9222';

const sessions = new Map(); // sessionId -> { transport, child }

const app = express();

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Mcp-Session-Id');
  res.header('Access-Control-Expose-Headers', 'Mcp-Session-Id');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.get('/', (req, res) => res.status(200).send('ok'));

app.use((req, res, next) => {
  if (!AUTH_TOKEN) return next(); // no auth configured — do not expose publicly like this
  const headerOk = req.headers.authorization === `Bearer ${AUTH_TOKEN}`;
  const queryOk = req.query.token === AUTH_TOKEN;
  if (headerOk || queryOk) return next();
  res.status(401).json({ error: 'unauthorized' });
});

app.use(express.json());

async function createSession() {
  const child = new StdioClientTransport({
    command: 'node_modules/.bin/chrome-devtools-mcp',
    args: ['--browserUrl', CDP_URL, '--no-usage-statistics', '--no-performance-crux'],
  });

  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: () => randomUUID(),
    onsessioninitialized: (sessionId) => {
      sessions.set(sessionId, { transport, child });
      console.log(`New session ${sessionId}, ${sessions.size} active`);
    },
  });

  const cleanup = () => {
    if (transport.sessionId) sessions.delete(transport.sessionId);
    child.close().catch(() => {});
  };

  transport.onmessage = (msg) => child.send(msg).catch((err) => console.error('child.send error:', err));
  child.onmessage = (msg) => transport.send(msg).catch((err) => console.error('transport.send error:', err));
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