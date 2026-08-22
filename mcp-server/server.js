// Thin authenticated reverse proxy in front of supergateway (loopback-only),
// which itself bridges stdio <-> HTTP for the official chrome-devtools-mcp server.
import express from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';

const AUTH_TOKEN = process.env.MCP_AUTH_TOKEN; // required before exposing publicly
const BRIDGE_TARGET = process.env.BRIDGE_TARGET || 'http://127.0.0.1:3002';
const PORT = Number(process.env.PORT || 3001);

const app = express();

app.use((req, res, next) => {
  if (!AUTH_TOKEN) return next(); // no auth configured — do not expose publicly like this
  if (req.headers.authorization === `Bearer ${AUTH_TOKEN}`) return next();
  res.status(401).json({ error: 'unauthorized' });
});

app.use('/', createProxyMiddleware({
  target: BRIDGE_TARGET,
  changeOrigin: true,
  ws: true, // supergateway may use SSE/streaming; keep upgrade support
}));

app.listen(PORT, () => console.log(`Auth proxy listening on :${PORT} -> ${BRIDGE_TARGET}`));
