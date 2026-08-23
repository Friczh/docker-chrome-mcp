# chrome-mcp

Chrome (latest, headful via Xvfb) + full DevTools access + VNC, exposed as a remote MCP server for claude.ai web.

## Setup

```bash
cd chrome-mcp
echo "MCP_AUTH_TOKEN=$(openssl rand -hex 32)" > .env
docker compose build
docker compose up -d
```

Ports (host network): `3001` MCP endpoint, `5900` VNC (no password — SSH-tunnel only), `9222` raw CDP (keep off the public internet).

## Expose public link using Tailscale Funnel

```bash
tailscale funnel --bg 3001
```

## VNC access (for you, not Claude)

`x11vnc` runs with no password and is not exposed via Tailscale. SSH tunnel only:
```bash
ssh -L 5900:localhost:5900 user@your-debian-server
```
Then point any VNC client at `localhost:5900`. Never funnel/serve port 5900.

## Tool backend

Google's official `chrome-devtools-mcp` — full CDP surface: performance tracing, network inspection, console, DOM, screenshots, Lighthouse audits, memory/heap snapshots. Connects to the Chrome already running in the container via `--browserUrl`, sharing the same profile/cookies you see over VNC. Screenshots are capped (webp, quality 60, max 1280x800) to keep tool-result payloads small.

**Why no supergateway:** it wraps chrome-devtools-mcp in a single shared MCP `Server` object across all connections, which throws `Already connected to a transport` and crashes the moment a second session opens (claude.ai reliably does this — one health-check connection, then the real one). `server.js` replaces it with a hand-rolled Streamable HTTP bridge that spawns an isolated `chrome-devtools-mcp` child per session instead.

**Per-request timeout:** if a tool call (e.g. `evaluate` hung mid-script) doesn't respond within 45s, the bridge returns an error and kills that session's child, instead of the request hanging forever.

**`save_url_to_file` custom tool:** for content reachable at its own URL (e.g. a `.js` file open in a tab), this fetches it server-side and saves to `/data/outputs`, returning a download link — the actual bytes never pass through Claude's context/tokens. Files are served at `https://<host>/files/<name>?token=...`. Handled entirely in `server.js`, not forwarded to the `chrome-devtools-mcp` child.

## Notes / caveats
- `chrome-devtools-mcp` exposes far more surface than a curated tool set — it can read/modify essentially anything DevTools can, including cookies, storage, and network traffic on whatever's logged into the shared Chrome profile.
- Chrome runs `--no-sandbox` (required in most containers). Don't run this as an unprivileged multi-tenant service without more hardening.
- VNC has no password — access is gated entirely by SSH access to the host.
- Query-param auth tokens can show up in logs/proxy history — acceptable trade-off for a personal box behind your own Funnel, not for a shared/multi-tenant setup.
- The `save_url_to_file` tool integration (intercepting `tools/call`/`tools/list` mid-stream) is implemented against the documented MCP message shape but hasn't been exhaustively tested against every client behavior — if the tool doesn't appear tool list or errors on call, check `docker compose logs` first.
