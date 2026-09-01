// Authenticated Streamable HTTP bridge exposing chrome-devtools-mcp remotely.
// Spawns a fresh chrome-devtools-mcp child per MCP session (attached to the
// same running Chrome via --browserUrl) rather than sharing one process
// across sessions — avoids the "already connected to a transport" crash
// that a single shared child hits under concurrent/reconnecting clients.
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { writeFile, readFile, mkdir, readdir, stat, open } from 'node:fs/promises';
import { exec as execCb, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import express from 'express';
import beautifyPkg from 'js-beautify';
const { js: beautifyJs } = beautifyPkg;
import { diffLines } from 'diff';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

const AUTH_TOKEN = process.env.MCP_AUTH_TOKEN;
const PORT = Number(process.env.PORT || 3001);
const CDP_URL = process.env.CDP_URL || 'http://localhost:9222';
const OUTPUT_DIR = process.env.OUTPUT_DIR || '/data/outputs';
const SNAPSHOTS_DIR = process.env.SNAPSHOTS_DIR || '/data/snapshots';
const FILES_TOKEN = process.env.FILES_TOKEN;
const PUBLIC_BASE = process.env.PUBLIC_BASE || '';
const WORKSPACE_DIR = process.env.WORKSPACE_DIR || '/data/workspace';
const exec = promisify(execCb);

await mkdir(SNAPSHOTS_DIR, { recursive: true }).catch(() => {});
await mkdir(WORKSPACE_DIR, { recursive: true }).catch(() => {});

function safeEqual(a, b) {
  const bufA = Buffer.from(a), bufB = Buffer.from(b);
  return bufA.length === bufB.length && timingSafeEqual(bufA, bufB);
}

async function isValidMcpToken(token) {
  if (!token || !AUTH_TOKEN) return false;
  return safeEqual(token, AUTH_TOKEN);
}

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

// Same idea as save_url_to_file, but the source is a JS expression run in
// the active page rather than a URL fetch. Talks to CDP's HTTP endpoint
// directly (not through the chrome-devtools-mcp child) so a large result
// (a decoded token, a big object dump, etc.) never travels through the
// MCP message pipe as raw content — only a short preview + link does.
const EVALUATE_TO_FILE_TOOL = {
  name: 'evaluate_to_file',
  description: 'Run a JavaScript expression in the active tab and save the result directly to disk instead of returning it inline. Use this instead of the regular evaluate/evaluate_script tool whenever the result could be large (a decoded token, a big JSON blob, long text) — returns a short preview and a download link, not the full value.',
  inputSchema: {
    type: 'object',
    properties: {
      expression: { type: 'string', description: 'JavaScript expression to evaluate in the page' },
      filename: { type: 'string', description: 'Filename to save as (default: a generated name)' },
    },
    required: ['expression'],
  },
};

async function getCdpPageTarget() {
  let versionResp;
  try {
    versionResp = await fetch(`${CDP_URL}/json/version`);
  } catch (e) {
    throw new Error(`Could not reach CDP at ${CDP_URL} (${e.message}) — is Chrome running?`);
  }
  if (!versionResp.ok) throw new Error(`CDP at ${CDP_URL} returned HTTP ${versionResp.status}`);
  const targetsResp = await fetch(`${CDP_URL}/json/list`);
  const targets = await targetsResp.json();
  const page = targets.find((t) => t.type === 'page') || targets[0];
  if (!page?.webSocketDebuggerUrl) throw new Error('No page target with a debugger URL found — open a page first');
  return page;
}

async function cdpEvaluate(expression) {
  const page = await getCdpPageTarget();
  return new Promise((resolve, reject) => {
    import('ws').then(({ default: WebSocket }) => {
      const ws = new WebSocket(page.webSocketDebuggerUrl);
      const id = 1;
      ws.on('open', () => {
        ws.send(JSON.stringify({ id, method: 'Runtime.evaluate', params: { expression, returnByValue: true, awaitPromise: true } }));
      });
      ws.on('message', (data) => {
        const msg = JSON.parse(data.toString());
        if (msg.id === id) {
          ws.close();
          if (msg.result?.exceptionDetails) return reject(new Error(msg.result.exceptionDetails.text));
          resolve(msg.result?.result?.value);
        }
      });
      ws.on('error', reject);
    }).catch(reject);
  });
}

// Opens a CDP websocket good for several request/response round trips plus
// event listening (unlike cdpEvaluate, which is fire-and-forget-one-value).
// Caller must call close() when done.
async function openCdpSession() {
  const page = await getCdpPageTarget();
  const { default: WebSocket } = await import('ws');
  const ws = new WebSocket(page.webSocketDebuggerUrl);
  let msgId = 1;
  const pending = new Map();
  const eventHandlers = [];
  await new Promise((resolve, reject) => {
    ws.once('open', resolve);
    ws.once('error', reject);
  });
  ws.on('message', (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) reject(new Error(msg.error.message));
      else resolve(msg.result);
    } else if (msg.method) {
      for (const h of eventHandlers) h(msg.method, msg.params);
    }
  });
  return {
    send(method, params = {}) {
      return new Promise((resolve, reject) => {
        const id = msgId++;
        pending.set(id, { resolve, reject });
        ws.send(JSON.stringify({ id, method, params }));
      });
    },
    onEvent(handler) { eventHandlers.push(handler); },
    close() { ws.close(); },
  };
}

async function handleEvaluateToFile(args) {
  const { expression, filename } = args;
  const value = await cdpEvaluate(expression);
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  const safeName = (filename || 'evaluate-result.txt').replace(/[^a-zA-Z0-9._-]/g, '_');
  const finalName = `${Date.now()}-${safeName}`;
  await writeFile(path.join(OUTPUT_DIR, finalName), text);
  const tokenQs = FILES_TOKEN ? `?token=${FILES_TOKEN}` : '';
  const preview = text.length > 200 ? `${text.slice(0, 200)}…` : text;
  return {
    content: [{
      type: 'text',
      text: `Saved ${text.length} chars to ${finalName}. Preview: ${preview}\nDownload: ${PUBLIC_BASE}/files/${finalName}${tokenQs}`,
    }],
  };
}

const CUSTOM_TOOLS = {
  [SAVE_URL_TOOL.name]: { def: SAVE_URL_TOOL, handler: handleSaveUrlToFile },
  [EVALUATE_TO_FILE_TOOL.name]: { def: EVALUATE_TO_FILE_TOOL, handler: handleEvaluateToFile },
};

// ---------- RE toolset: script snapshotting, diffing, search, hooking ----------
// Built for reverse-engineering workflows (e.g. BotGuard/youtube.js-style
// challenge scripts) where the same script gets re-fetched repeatedly and
// only occasionally actually changes. Snapshotting + diffing means Claude
// only pays token cost for what changed, not the whole file every time.

function safeSnapshotName(name) {
  return name.replace(/[^a-zA-Z0-9._-]/g, '_');
}

async function listSnapshotVersions(name) {
  const dir = path.join(SNAPSHOTS_DIR, safeSnapshotName(name));
  try {
    const files = await readdir(dir);
    return files.filter((f) => f.endsWith('.js') || f.endsWith('.txt')).sort();
  } catch {
    return [];
  }
}

const DIFF_SCRIPT_TOOL = {
  name: 'diff_script',
  description: 'Fetch a URL (typically a JS file that rotates/changes over time, like a bot-detection challenge script) and compare it against the last saved snapshot under the same name. Returns only the diff, not the full script — use this instead of save_url_to_file when tracking a script for changes over time, e.g. re-checking a challenge script periodically. First call for a given name just saves the baseline and reports "no prior snapshot".',
  inputSchema: {
    type: 'object',
    properties: {
      url: { type: 'string', description: 'URL to fetch' },
      name: { type: 'string', description: 'Snapshot name to track this script under (e.g. "botguard-challenge") — reused across calls to diff against the previous version' },
      beautify: { type: 'boolean', description: 'Pretty-print the JS before diffing/saving (default true) — makes diffs and saved files far more readable for minified sources' },
    },
    required: ['url', 'name'],
  },
};

async function handleDiffScript(args) {
  const { url, name, beautify = true } = args;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Fetch failed: ${resp.status} ${resp.statusText}`);
  let text = await resp.text();
  if (beautify) {
    try { text = beautifyJs(text, { indent_size: 2 }); } catch { /* fall back to raw text */ }
  }

  const versions = await listSnapshotVersions(name);
  const dir = path.join(SNAPSHOTS_DIR, safeSnapshotName(name));
  await mkdir(dir, { recursive: true });
  const stamp = Date.now();
  await writeFile(path.join(dir, `${stamp}.js`), text);

  const tokenQs = FILES_TOKEN ? `?token=${FILES_TOKEN}` : '';
  if (versions.length === 0) {
    return { content: [{ type: 'text', text: `No prior snapshot for "${name}" — saved baseline (${text.length} chars). Nothing to diff against yet.` }] };
  }

  const prevText = await readFile(path.join(dir, versions[versions.length - 1]), 'utf8');
  if (prevText === text) {
    return { content: [{ type: 'text', text: `No changes since last snapshot of "${name}" (${versions.length} prior version${versions.length > 1 ? 's' : ''}).` }] };
  }

  const changes = diffLines(prevText, text);
  const diffLinesOut = [];
  for (const part of changes) {
    if (!part.added && !part.removed) continue;
    const marker = part.added ? '+' : '-';
    for (const line of part.value.split('\n')) {
      if (line) diffLinesOut.push(`${marker} ${line}`);
    }
  }
  const diffText = diffLinesOut.join('\n');
  const diffFileName = `${stamp}-diff-${safeSnapshotName(name)}.diff`;
  await writeFile(path.join(OUTPUT_DIR, diffFileName), diffText);

  const preview = diffText.length > 1500 ? `${diffText.slice(0, 1500)}\n…(truncated, see full diff file)` : diffText;
  return {
    content: [{
      type: 'text',
      text: `Changed since last snapshot of "${name}" (v${versions.length} -> v${versions.length + 1}, ${diffLinesOut.length} changed lines):\n\n${preview}\n\nFull diff: ${PUBLIC_BASE}/files/${diffFileName}${tokenQs}\nFull new version: ${PUBLIC_BASE}/files/${diffFileName.replace('-diff-', '-full-')}${tokenQs}`,
    }],
  };
}

const DIFF_VERSIONS_TOOL = {
  name: 'diff_script_versions',
  description: 'Compare two specific saved snapshot versions of a tracked script by their version index (from diff_script), instead of only "latest vs previous". Use this to pinpoint exactly when a specific change was introduced across many saved versions.',
  inputSchema: {
    type: 'object',
    properties: {
      name: { type: 'string', description: 'Snapshot name' },
      fromVersion: { type: 'number', description: '1-indexed version number to diff from' },
      toVersion: { type: 'number', description: '1-indexed version number to diff to' },
    },
    required: ['name', 'fromVersion', 'toVersion'],
  },
};

async function handleDiffScriptVersions(args) {
  const { name, fromVersion, toVersion } = args;
  const versions = await listSnapshotVersions(name);
  if (!versions.length) throw new Error(`No snapshots found for "${name}"`);
  if (fromVersion < 1 || fromVersion > versions.length || toVersion < 1 || toVersion > versions.length) {
    throw new Error(`Version out of range — "${name}" has ${versions.length} saved version(s)`);
  }
  const dir = path.join(SNAPSHOTS_DIR, safeSnapshotName(name));
  const fromText = await readFile(path.join(dir, versions[fromVersion - 1]), 'utf8');
  const toText = await readFile(path.join(dir, versions[toVersion - 1]), 'utf8');
  const changes = diffLines(fromText, toText);
  const out = [];
  for (const part of changes) {
    if (!part.added && !part.removed) continue;
    const marker = part.added ? '+' : '-';
    for (const line of part.value.split('\n')) if (line) out.push(`${marker} ${line}`);
  }
  const diffText = out.join('\n') || '(no differences)';
  const preview = diffText.length > 1500 ? `${diffText.slice(0, 1500)}\n…(truncated)` : diffText;
  return { content: [{ type: 'text', text: `v${fromVersion} -> v${toVersion} of "${name}" (${out.length} changed lines):\n\n${preview}` }] };
}

const LIST_PAGE_SCRIPTS_TOOL = {
  name: 'list_page_scripts',
  description: 'List every <script> on the current page (src URL or inline, with size) without fetching any content — a table of contents to decide what\'s worth pulling with diff_script/save_url_to_file, instead of guessing or fetching everything.',
  inputSchema: { type: 'object', properties: {} },
};

async function handleListPageScripts() {
  const result = await cdpEvaluate(`
    Array.from(document.scripts).map((s, i) => ({
      index: i,
      src: s.src || '(inline)',
      inlineLength: s.src ? null : s.textContent.length,
    }))
  `);
  const lines = (result || []).map((s) =>
    `[${s.index}] ${s.src}${s.inlineLength != null ? ` (inline, ${s.inlineLength} chars)` : ''}`
  );
  return { content: [{ type: 'text', text: lines.join('\n') || 'No scripts found on page.' }] };
}

const SEARCH_IN_FILE_TOOL = {
  name: 'search_in_saved_file',
  description: 'Search a file previously saved by save_url_to_file/evaluate_to_file/diff_script (grep-style, server-side) and return only matching line numbers + short context — instead of downloading/reading the whole file to find one function or string in a large script.',
  inputSchema: {
    type: 'object',
    properties: {
      filename: { type: 'string', description: 'Filename as returned in a previous save (just the name, not the full URL)' },
      pattern: { type: 'string', description: 'Substring or regex pattern to search for' },
      isRegex: { type: 'boolean', description: 'Treat pattern as a regex (default false — plain substring match)' },
      contextLines: { type: 'number', description: 'Lines of context around each match (default 2)' },
    },
    required: ['filename', 'pattern'],
  },
};

async function handleSearchInFile(args) {
  const { filename, pattern, isRegex = false, contextLines = 2 } = args;
  const text = await readFile(path.join(OUTPUT_DIR, safeSnapshotName(filename)), 'utf8');
  const lines = text.split('\n');
  const matcher = isRegex ? new RegExp(pattern) : null;
  const hits = [];
  lines.forEach((line, i) => {
    const matched = isRegex ? matcher.test(line) : line.includes(pattern);
    if (matched) hits.push(i);
  });
  if (!hits.length) return { content: [{ type: 'text', text: `No matches for "${pattern}" in ${filename}.` }] };

  const blocks = hits.slice(0, 50).map((i) => {
    const start = Math.max(0, i - contextLines);
    const end = Math.min(lines.length, i + contextLines + 1);
    const block = lines.slice(start, end).map((l, j) => `${start + j + 1}: ${l}`).join('\n');
    return block;
  });
  const more = hits.length > 50 ? `\n…and ${hits.length - 50} more matches (showing first 50)` : '';
  return { content: [{ type: 'text', text: `${hits.length} match(es) for "${pattern}" in ${filename}:\n\n${blocks.join('\n---\n')}${more}` }] };
}

// Injects instrumentation before page load that wraps a named function
// (found anywhere in window's own properties, or a dotted path like
// "window.foo.bar") to log every call's arguments + return value into a
// buffer, flushed to a file on demand. Lets Claude observe what a function
// actually does at runtime instead of re-running evaluate() calls trying
// to catch a value mid-execution — directly aimed at cases like capturing
// what a token-generating function is called with/returns.
const HOOK_FUNCTION_TOOL = {
  name: 'hook_function',
  description: 'Instrument a global function (by dotted path, e.g. "window.foo.generateToken") so every call to it — before the page even finishes loading — has its arguments and return value logged. Reload the page after calling this for the hook to take effect on load, then use dump_hook_log to retrieve captured calls. Useful for observing what a specific function does at runtime without manually re-triggering/relaying values.',
  inputSchema: {
    type: 'object',
    properties: {
      path: { type: 'string', description: 'Dotted path to the function on window, e.g. "window.bgutils.generatePoToken"' },
    },
    required: ['path'],
  },
};

async function handleHookFunction(args) {
  const { path: fnPath } = args;
  const script = `
    (function() {
      window.__hookLog = window.__hookLog || [];
      const parts = '${fnPath}'.replace('window.', '').split('.');
      let obj = window;
      for (let i = 0; i < parts.length - 1; i++) obj = obj[parts[i]];
      const key = parts[parts.length - 1];
      const original = obj[key];
      if (typeof original !== 'function') return 'not a function yet — page may not have loaded it';
      obj[key] = function(...args) {
        let result, error;
        try { result = original.apply(this, args); }
        catch (e) { error = String(e); throw e; }
        finally {
          window.__hookLog.push({
            t: Date.now(), path: '${fnPath}',
            args: args.map(a => { try { return JSON.stringify(a); } catch { return String(a); } }),
            result: error ? undefined : (() => { try { return JSON.stringify(result); } catch { return String(result); } })(),
            error,
          });
        }
        return result;
      };
      return 'hooked';
    })()
  `;
  const outcome = await cdpEvaluate(script);
  return { content: [{ type: 'text', text: `Hook result for ${fnPath}: ${outcome}. Note: if the page reloads, re-call hook_function to reinstall it before the target function is defined.` }] };
}

const DUMP_HOOK_LOG_TOOL = {
  name: 'dump_hook_log',
  description: 'Retrieve everything captured by hook_function so far (all calls, arguments, return values) and save it to a file — instead of the log passing through Claude\'s context directly, which could be large after many calls.',
  inputSchema: { type: 'object', properties: {} },
};

async function handleDumpHookLog() {
  const log = await cdpEvaluate('JSON.stringify(window.__hookLog || [], null, 2)');
  const text = log || '[]';
  const finalName = `${Date.now()}-hook-log.json`;
  await writeFile(path.join(OUTPUT_DIR, finalName), text);
  const tokenQs = FILES_TOKEN ? `?token=${FILES_TOKEN}` : '';
  const count = (JSON.parse(text) || []).length;
  return { content: [{ type: 'text', text: `Captured ${count} call(s). Log: ${PUBLIC_BASE}/files/${finalName}${tokenQs}` }] };
}

// ---------- Generic HTTP proxy — requests go out through this container's ----------
// network identity rather than through the browser or Claude's own sandbox.
// Useful for direct API calls (e.g. hitting YouTube's internal endpoints)
// without spinning up a full page, and for consistent egress IP/behavior.
const HTTP_REQUEST_TOOL = {
  name: 'http_request',
  description: 'Make an arbitrary HTTP request from this server (not from Claude\'s own sandbox, not through the browser) — the request goes out using this container\'s network identity. Useful for direct API calls where you don\'t need a full browser page. Response is saved to a file and a short preview + link is returned, since responses can be large.',
  inputSchema: {
    type: 'object',
    properties: {
      url: { type: 'string' },
      method: { type: 'string', description: 'HTTP method (default GET)' },
      headers: { type: 'object', description: 'Request headers as key-value pairs' },
      body: { type: 'string', description: 'Request body (for POST/PUT/etc)' },
    },
    required: ['url'],
  },
};

async function handleHttpRequest(args) {
  const { url, method = 'GET', headers = {}, body } = args;
  const resp = await fetch(url, { method, headers, body });
  const buf = Buffer.from(await resp.arrayBuffer());
  const finalName = `${Date.now()}-http-response`;
  await writeFile(path.join(OUTPUT_DIR, finalName), buf);
  const tokenQs = FILES_TOKEN ? `?token=${FILES_TOKEN}` : '';
  const text = buf.toString('utf8');
  const preview = text.length > 500 ? `${text.slice(0, 500)}…` : text;
  return {
    content: [{
      type: 'text',
      text: `${resp.status} ${resp.statusText}, ${buf.length} bytes.\nPreview: ${preview}\nFull response: ${PUBLIC_BASE}/files/${finalName}${tokenQs}`,
    }],
  };
}

const CUSTOM_TOOLS_RE = {
  [DIFF_SCRIPT_TOOL.name]: { def: DIFF_SCRIPT_TOOL, handler: handleDiffScript },
  [DIFF_VERSIONS_TOOL.name]: { def: DIFF_VERSIONS_TOOL, handler: handleDiffScriptVersions },
  [LIST_PAGE_SCRIPTS_TOOL.name]: { def: LIST_PAGE_SCRIPTS_TOOL, handler: handleListPageScripts },
  [SEARCH_IN_FILE_TOOL.name]: { def: SEARCH_IN_FILE_TOOL, handler: handleSearchInFile },
  [HOOK_FUNCTION_TOOL.name]: { def: HOOK_FUNCTION_TOOL, handler: handleHookFunction },
  [DUMP_HOOK_LOG_TOOL.name]: { def: DUMP_HOOK_LOG_TOOL, handler: handleDumpHookLog },
  [HTTP_REQUEST_TOOL.name]: { def: HTTP_REQUEST_TOOL, handler: handleHttpRequest },
};
const GREP_WORKSPACE_TOOL = {
  name: 'grep_workspace',
  description: 'Search workspace files for a pattern, returning only matching lines (file:line:text) — not whole files. Use before reading a file when hunting for something specific (a function name, endpoint, magic constant). Far cheaper than run_shell_command+cat on large/obfuscated scripts.',
  inputSchema: {
    type: 'object',
    properties: {
      pattern: { type: 'string', description: 'Extended regex (grep -E) to search for' },
      subdir: { type: 'string', description: 'Subdirectory relative to workspace root (default: whole workspace)' },
      filePattern: { type: 'string', description: 'Glob restricting which files are searched, e.g. "*.js" (default: all)' },
      maxMatches: { type: 'number', description: 'Cap on returned matches (default 100)' },
      ignoreCase: { type: 'boolean' },
    },
    required: ['pattern'],
  },
};

async function handleGrepWorkspace(args) {
  const { pattern, subdir, filePattern, maxMatches = 100, ignoreCase } = args;
  const dir = resolveInWorkspace(subdir);
  const includeArg = filePattern ? `--include=${JSON.stringify(filePattern)}` : '';
  const iArg = ignoreCase ? '-i' : '';
  const cmd = `grep -rn ${iArg} -E ${includeArg} --exclude-dir=node_modules --exclude-dir=.git --exclude-dir=.mcp-logs -- ${JSON.stringify(pattern)} . | head -n ${maxMatches}`;
  let stdout = '';
  try {
    const r = await exec(cmd, { cwd: dir, maxBuffer: 20 * 1024 * 1024, shell: '/bin/bash' });
    stdout = r.stdout;
  } catch (e) {
    if (e.code === 1 && !e.stderr) stdout = ''; // grep: exit 1 just means "no matches"
    else throw new Error(e.stderr || e.message);
  }
  return { content: [{ type: 'text', text: stdout.trim() || '(no matches)' }] };
}

const READ_WORKSPACE_FILE_TOOL = {
  name: 'read_workspace_file',
  description: 'Read a line range from a workspace file (default: first 200 lines). Prefer this over run_shell_command+cat when inspecting a large file — avoids pulling the whole thing into context when only a section is needed.',
  inputSchema: {
    type: 'object',
    properties: {
      filename: { type: 'string' },
      startLine: { type: 'number', description: '1-indexed (default 1)' },
      endLine: { type: 'number', description: '1-indexed, inclusive (default startLine+199)' },
    },
    required: ['filename'],
  },
};

async function handleReadWorkspaceFile(args) {
  const { filename, startLine = 1, endLine } = args;
  const dest = resolveInWorkspace(filename);
  const content = await readFile(dest, 'utf8');
  const lines = content.split('\n');
  const end = Math.min(endLine || startLine + 199, lines.length);
  const slice = lines.slice(startLine - 1, end).join('\n');
  const header = `[${filename}: lines ${startLine}-${end} of ${lines.length}]`;
  return { content: [{ type: 'text', text: `${header}\n${slice}` }] };
}

// ---------- BgUtils/youtube.js-focused tools ----------
// BotGuard/poToken work revolves around two things a plain shell can't get:
// (1) full session state including httpOnly cookies CDP can see but page JS
// can't, and (2) the network traffic around the attestation/minting calls,
// which is expensive to reconstruct one get_network_request call at a time.

const DUMP_STORAGE_STATE_TOOL = {
  name: 'dump_storage_state',
  description: 'Snapshot the active page\'s full storage state — cookies (including httpOnly, read via CDP, not document.cookie), localStorage, sessionStorage, and IndexedDB database names — into one JSON file in the workspace. Use to capture a session (e.g. right after a BotGuard challenge resolves) so it can be reused/replayed instead of re-derived.',
  inputSchema: {
    type: 'object',
    properties: { filename: { type: 'string', description: 'Destination filename in workspace (default: auto-generated)' } },
  },
};

async function handleDumpStorageState(args) {
  const session = await openCdpSession();
  try {
    const cookiesResult = await session.send('Network.getAllCookies');
    const evalResult = await session.send('Runtime.evaluate', {
      expression: `(async () => {
        const ls = {}; for (let i = 0; i < localStorage.length; i++) { const k = localStorage.key(i); ls[k] = localStorage.getItem(k); }
        const ss = {}; for (let i = 0; i < sessionStorage.length; i++) { const k = sessionStorage.key(i); ss[k] = sessionStorage.getItem(k); }
        let idbNames = null;
        try { idbNames = (await indexedDB.databases()).map((d) => d.name); } catch (e) {}
        return JSON.stringify({ localStorage: ls, sessionStorage: ss, indexedDBNames: idbNames, origin: location.origin, userAgent: navigator.userAgent });
      })()`,
      returnByValue: true,
      awaitPromise: true,
    });
    if (evalResult.exceptionDetails) throw new Error(evalResult.exceptionDetails.text);
    const pageState = JSON.parse(evalResult.result.value);
    const combined = { capturedAt: new Date().toISOString(), cookies: cookiesResult.cookies, ...pageState };
    const finalName = args?.filename || `storage-state-${Date.now()}.json`;
    const dest = resolveInWorkspace(finalName);
    await mkdir(path.dirname(dest), { recursive: true });
    await writeFile(dest, JSON.stringify(combined, null, 2));
    const idbNote = pageState.indexedDBNames ? `, IndexedDB: ${pageState.indexedDBNames.join(', ') || '(none)'}` : '';
    return {
      content: [{
        type: 'text',
        text: `Saved to workspace:${finalName} — ${combined.cookies.length} cookies, ${Object.keys(pageState.localStorage).length} localStorage keys, ${Object.keys(pageState.sessionStorage).length} sessionStorage keys${idbNote}.`,
      }],
    };
  } finally {
    session.close();
  }
}

const CAPTURE_NETWORK_TRAFFIC_TOOL = {
  name: 'capture_network_traffic',
  description: 'Passively record network traffic on the active page for a duration and save it as one JSON file in the workspace — much cheaper than polling list_network_requests/get_network_request per request. Optionally fetch response bodies (e.g. for the youtubei attestation/poToken endpoint) only for URLs matching a regex, so you don\'t pull down every image/script response.',
  inputSchema: {
    type: 'object',
    properties: {
      durationMs: { type: 'number', description: 'Capture duration in ms (default 10000, max 60000)' },
      urlFilter: { type: 'string', description: 'Regex; only requests whose URL matches this get their response body fetched and included' },
      maxBodies: { type: 'number', description: 'Max number of matched response bodies to fetch (default 20)' },
      filename: { type: 'string', description: 'Destination filename in workspace (default: auto-generated)' },
    },
  },
};

async function handleCaptureNetworkTraffic(args) {
  const { durationMs = 10_000, urlFilter, maxBodies = 20, filename } = args || {};
  const duration = Math.min(durationMs, 60_000);
  const filterRe = urlFilter ? new RegExp(urlFilter) : null;
  const requests = new Map();
  let bodiesCaptured = 0;

  const session = await openCdpSession();
  try {
    session.onEvent((method, params) => {
      if (method === 'Network.requestWillBeSent') {
        requests.set(params.requestId, {
          url: params.request.url,
          method: params.request.method,
          resourceType: params.type,
          requestHeaders: params.request.headers,
          postData: params.request.postData || null,
          timestamp: params.timestamp,
        });
      } else if (method === 'Network.responseReceived') {
        const entry = requests.get(params.requestId);
        if (entry) {
          entry.status = params.response.status;
          entry.mimeType = params.response.mimeType;
          entry.responseHeaders = params.response.headers;
        }
      } else if (method === 'Network.loadingFinished') {
        const entry = requests.get(params.requestId);
        if (entry && filterRe && filterRe.test(entry.url) && bodiesCaptured < maxBodies) {
          bodiesCaptured++;
          session.send('Network.getResponseBody', { requestId: params.requestId })
            .then((body) => { entry.body = body.base64Encoded ? Buffer.from(body.body, 'base64').toString('utf8') : body.body; })
            .catch(() => { entry.bodyError = true; });
        }
      }
    });

    await session.send('Network.enable');
    await new Promise((r) => setTimeout(r, duration));
    await session.send('Network.disable').catch(() => {});
    await new Promise((r) => setTimeout(r, 300)); // let in-flight getResponseBody calls above settle
  } finally {
    session.close();
  }

  const all = [...requests.values()];
  const matched = filterRe ? all.filter((r) => filterRe.test(r.url)) : all;
  const finalName = filename || `network-capture-${Date.now()}.json`;
  const dest = resolveInWorkspace(finalName);
  await mkdir(path.dirname(dest), { recursive: true });
  await writeFile(dest, JSON.stringify({ capturedAt: new Date().toISOString(), durationMs: duration, urlFilter: urlFilter || null, totalRequests: all.length, requests: all }, null, 2));

  const preview = matched.slice(0, 10).map((r) => `${r.method} ${r.status ?? '?'} ${r.url}`).join('\n');
  const summary = `Captured ${all.length} requests over ${duration}ms (${matched.length} matched filter, ${bodiesCaptured} bodies saved). Full capture: workspace:${finalName}`;
  return { content: [{ type: 'text', text: matched.length ? `${summary}\n\nMatched (first 10):\n${preview}` : summary }] };
}

Object.assign(CUSTOM_TOOLS, CUSTOM_TOOLS_RE);

// ---------- Exec toolset: arbitrary shell/code execution, package installs, ----------
// file upload/download in a persistent workspace. Gives agents a real dev
// environment alongside the browser — for writing/running scripts against
// BotGuard/youtube.js-style targets, installing test deps, etc. All paths
// are confined to WORKSPACE_DIR; commands run with a hard timeout.

function resolveInWorkspace(p) {
  const target = path.resolve(WORKSPACE_DIR, p || '.');
  if (target !== WORKSPACE_DIR && !target.startsWith(WORKSPACE_DIR + path.sep)) {
    throw new Error('Path escapes workspace directory');
  }
  return target;
}

const RUN_SHELL_TOOL = {
  name: 'run_shell_command',
  description: 'Run an arbitrary shell command (bash -lc) inside the container\'s persistent workspace directory. Use for running scripts, node/python code, git, npm/pip/pnpm/yarn, curl, etc. Output is truncated in the reply past maxOutputChars (default 2000) and saved in full to a downloadable file — use grep_workspace/read_workspace_file/download_workspace_file to inspect it in full without re-running the command.',
  inputSchema: {
    type: 'object',
    properties: {
      command: { type: 'string', description: 'Shell command to run' },
      cwd: { type: 'string', description: 'Working directory relative to the workspace root (default: workspace root)' },
      timeoutMs: { type: 'number', description: 'Timeout in ms (default 120000, max 600000)' },
      maxOutputChars: { type: 'number', description: 'Inline output cap before truncation (default 2000). Raise it only if you actually need more shown inline — the full output is always saved to a file regardless.' },
    },
    required: ['command'],
  },
};

async function handleRunShellCommand(args) {
  const { command, cwd, timeoutMs, maxOutputChars = 2000 } = args;
  const timeout = Math.min(timeoutMs || 120_000, 600_000);
  const workDir = resolveInWorkspace(cwd);
  await mkdir(workDir, { recursive: true });
  let stdout = '', stderr = '', code = 0;
  try {
    const result = await exec(command, { cwd: workDir, timeout, maxBuffer: 100 * 1024 * 1024, shell: '/bin/bash' });
    stdout = result.stdout; stderr = result.stderr;
  } catch (err) {
    stdout = err.stdout || ''; stderr = err.stderr || String(err.message || err);
    code = typeof err.code === 'number' ? err.code : 1;
  }
  const combined = `$ ${command}\n[exit ${code}]\n\n--- stdout ---\n${stdout || '(empty)'}${stderr ? `\n--- stderr ---\n${stderr}` : ''}`;
  const tokenQs = FILES_TOKEN ? `?token=${FILES_TOKEN}` : '';
  let text = combined;
  if (combined.length > maxOutputChars) {
    const finalName = `${Date.now()}-shell-output.txt`;
    await writeFile(path.join(OUTPUT_DIR, finalName), combined);
    text = `${combined.slice(0, maxOutputChars)}\n…(truncated, ${combined.length} chars total)\nFull output: ${PUBLIC_BASE}/files/${finalName}${tokenQs}`;
  }
  return { content: [{ type: 'text', text }] };
}

const INSTALL_PACKAGE_TOOL = {
  name: 'install_package',
  description: 'Install one or more packages into the workspace environment using npm, pip, or apt (apt runs as root inside the container). Use before running code that depends on a library not already present.',
  inputSchema: {
    type: 'object',
    properties: {
      manager: { type: 'string', enum: ['npm', 'pip', 'apt'], description: 'Package manager to use' },
      packages: { type: 'array', items: { type: 'string' }, description: 'Package names (with optional version specifiers, e.g. "lodash@4" or "requests==2.31.0")' },
      cwd: { type: 'string', description: 'For npm: working directory relative to workspace root (default: workspace root, requires a package.json — run "npm init -y" first if needed)' },
    },
    required: ['manager', 'packages'],
  },
};

async function handleInstallPackage(args) {
  const { manager, packages, cwd } = args;
  if (!packages?.length) throw new Error('No packages specified');
  const safePkgs = packages.map((p) => {
    if (!/^[a-zA-Z0-9@._\-/=<>~^!]+$/.test(p)) throw new Error(`Rejected suspicious package spec: ${p}`);
    return p;
  });
  let command;
  let workDir = WORKSPACE_DIR;
  if (manager === 'npm') {
    workDir = resolveInWorkspace(cwd);
    await mkdir(workDir, { recursive: true });
    command = `npm install ${safePkgs.join(' ')}`;
  } else if (manager === 'pip') {
    command = `pip install ${safePkgs.join(' ')}`; // /data/venv is on PATH — no --break-system-packages needed
  } else if (manager === 'apt') {
    command = `apt-get update -qq && apt-get install -y --no-install-recommends ${safePkgs.join(' ')}`;
  } else {
    throw new Error(`Unknown manager: ${manager}`);
  }
  return handleRunShellCommand({ command, cwd: manager === 'npm' ? cwd : undefined, timeoutMs: 300_000 });
}

const UPLOAD_FILE_TOOL = {
  name: 'upload_file_to_workspace',
  description: 'Write a file into the persistent workspace from base64-encoded content (e.g. a script Claude generated, or binary data). Use this to place code/data into the workspace before running it with run_shell_command.',
  inputSchema: {
    type: 'object',
    properties: {
      filename: { type: 'string', description: 'Destination path relative to the workspace root, e.g. "scripts/decode.py"' },
      contentBase64: { type: 'string', description: 'Base64-encoded file content' },
    },
    required: ['filename', 'contentBase64'],
  },
};

async function handleUploadFileToWorkspace(args) {
  const { filename, contentBase64 } = args;
  const dest = resolveInWorkspace(filename);
  await mkdir(path.dirname(dest), { recursive: true });
  const buf = Buffer.from(contentBase64, 'base64');
  await writeFile(dest, buf);
  return { content: [{ type: 'text', text: `Wrote ${buf.length} bytes to workspace:${filename}` }] };
}

const LIST_WORKSPACE_TOOL = {
  name: 'list_workspace_files',
  description: 'Recursively list files in the workspace (or a subdirectory), with sizes. Capped to maxEntries (default 200) — narrow with subdir for large trees rather than raising the cap, or use grep_workspace to search directly instead of listing then reading.',
  inputSchema: {
    type: 'object',
    properties: {
      subdir: { type: 'string', description: 'Subdirectory relative to workspace root (default: root)' },
      maxEntries: { type: 'number', description: 'Cap on listed entries (default 200)' },
    },
  },
};

async function handleListWorkspaceFiles(args) {
  const root = resolveInWorkspace(args?.subdir);
  const maxEntries = args?.maxEntries || 200;
  const out = [];
  let total = 0;
  async function walk(dir, prefix) {
    const entries = await readdir(dir, { withFileTypes: true }).catch(() => []);
    for (const e of entries) {
      if (e.name === 'node_modules' || e.name === '.git' || e.name === '.mcp-logs') continue;
      const rel = prefix ? `${prefix}/${e.name}` : e.name;
      if (e.isDirectory()) { await walk(path.join(dir, e.name), rel); }
      else {
        total++;
        if (out.length < maxEntries) {
          const s = await stat(path.join(dir, e.name)).catch(() => null);
          out.push(`${rel} (${s?.size ?? '?'} bytes)`);
        }
      }
    }
  }
  await walk(root, '');
  const text = out.length ? out.join('\n') : '(empty)';
  const suffix = total > out.length ? `\n…(${total - out.length} more not shown — narrow with subdir, or use grep_workspace)` : '';
  return { content: [{ type: 'text', text: text + suffix }] };
}

const DOWNLOAD_WORKSPACE_FILE_TOOL = {
  name: 'download_workspace_file',
  description: 'Copy a file from the workspace into the downloadable outputs area and return a link. Use this to fetch results produced by run_shell_command (e.g. a script\'s output file) back out of the container.',
  inputSchema: {
    type: 'object',
    properties: { filename: { type: 'string', description: 'Path relative to the workspace root' } },
    required: ['filename'],
  },
};

async function handleDownloadWorkspaceFile(args) {
  const src = resolveInWorkspace(args.filename);
  const buf = await readFile(src);
  const finalName = `${Date.now()}-${path.basename(src).replace(/[^a-zA-Z0-9._-]/g, '_')}`;
  await writeFile(path.join(OUTPUT_DIR, finalName), buf);
  const tokenQs = FILES_TOKEN ? `?token=${FILES_TOKEN}` : '';
  return { content: [{ type: 'text', text: `Download: ${PUBLIC_BASE}/files/${finalName}${tokenQs}` }] };
}

// ---------- Background processes: for anything that doesn't just run and ----------
// exit (a test server, a watcher, a script that polls/logs over time).
// run_shell_command blocks until exit and returns output once — useless for
// debugging something live. These tools spawn detached, stream stdout/stderr
// to a log file on disk as it's produced, and let Claude poll/tail it.

const LOGS_DIR = path.join(WORKSPACE_DIR, '.mcp-logs');
await mkdir(LOGS_DIR, { recursive: true }).catch(() => {});
const bgProcesses = new Map(); // id -> { proc, command, cwd, logFile, startedAt, status, exitCode }

const START_BG_PROCESS_TOOL = {
  name: 'start_background_process',
  description: 'Start a long-running command (a server, a watcher, a script that logs over time) in the background instead of blocking. Stdout/stderr stream to a log file as they\'re produced — use get_process_logs to tail it and list_processes/stop_process to manage it. Use this instead of run_shell_command for anything that doesn\'t just run and exit.',
  inputSchema: {
    type: 'object',
    properties: {
      command: { type: 'string', description: 'Shell command to run' },
      cwd: { type: 'string', description: 'Working directory relative to the workspace root' },
    },
    required: ['command'],
  },
};

async function handleStartBackgroundProcess(args) {
  const { command, cwd } = args;
  const workDir = resolveInWorkspace(cwd);
  await mkdir(workDir, { recursive: true });
  const id = randomUUID().slice(0, 8);
  const logFile = path.join(LOGS_DIR, `${id}.log`);
  const fh = await open(logFile, 'a');
  const logStream = fh.createWriteStream();
  logStream.write(`$ ${command}\n(cwd: ${workDir})\n\n`);

  const proc = spawn(command, { cwd: workDir, shell: '/bin/bash', detached: true });
  proc.stdout.pipe(logStream, { end: false });
  proc.stderr.pipe(logStream, { end: false });
  const entry = { proc, command, cwd: workDir, logFile, startedAt: Date.now(), status: 'running', exitCode: null };
  bgProcesses.set(id, entry);
  proc.on('exit', (code) => {
    entry.status = 'exited';
    entry.exitCode = code;
    logStream.write(`\n[process exited with code ${code}]\n`);
    logStream.end();
  });
  proc.on('error', (err) => {
    entry.status = 'error';
    logStream.write(`\n[spawn error: ${err.message}]\n`);
    logStream.end();
  });
  return { content: [{ type: 'text', text: `Started process ${id} (pid ${proc.pid}). Use get_process_logs with id "${id}" to tail output.` }] };
}

const GET_PROCESS_LOGS_TOOL = {
  name: 'get_process_logs',
  description: 'Read the log file (stdout+stderr, interleaved in order produced) for a process started with start_background_process. Call repeatedly to poll a live process while debugging. Defaults to the last 50 lines — pass tailLines to see more, or download_workspace_file with ".mcp-logs/<id>.log" for the entire thing.',
  inputSchema: {
    type: 'object',
    properties: {
      id: { type: 'string', description: 'Process id returned by start_background_process' },
      tailLines: { type: 'number', description: 'Return only the last N lines (default 50)' },
    },
    required: ['id'],
  },
};

async function handleGetProcessLogs(args) {
  const { id, tailLines = 50 } = args;
  const entry = bgProcesses.get(id);
  if (!entry) throw new Error(`No such process id: ${id}`);
  let text = await readFile(entry.logFile, 'utf8').catch(() => '(log file not yet created)');
  text = text.split('\n').slice(-tailLines).join('\n');
  let shown = text;
  if (shown.length > 2000) shown = `…(truncated)\n${shown.slice(-2000)}`;
  return {
    content: [{
      type: 'text',
      text: `[${id}] status: ${entry.status}${entry.exitCode !== null ? ` (exit ${entry.exitCode})` : ''}\n\n${shown}\n\nFull log: download_workspace_file with filename ".mcp-logs/${id}.log"`,
    }],
  };
}

const LIST_PROCESSES_TOOL = {
  name: 'list_processes',
  description: 'List all background processes started this session (running, exited, or errored), with pid, command, and status.',
  inputSchema: { type: 'object', properties: {} },
};

async function handleListProcesses() {
  if (!bgProcesses.size) return { content: [{ type: 'text', text: '(no background processes started)' }] };
  const lines = [...bgProcesses.entries()].map(([id, e]) =>
    `[${id}] pid=${e.proc.pid} status=${e.status}${e.exitCode !== null ? ` exit=${e.exitCode}` : ''} started=${new Date(e.startedAt).toISOString()} :: ${e.command}`
  );
  return { content: [{ type: 'text', text: lines.join('\n') }] };
}

const STOP_PROCESS_TOOL = {
  name: 'stop_process',
  description: 'Kill a background process started with start_background_process.',
  inputSchema: {
    type: 'object',
    properties: { id: { type: 'string', description: 'Process id' } },
    required: ['id'],
  },
};

async function handleStopProcess(args) {
  const entry = bgProcesses.get(args.id);
  if (!entry) throw new Error(`No such process id: ${args.id}`);
  if (entry.status !== 'running') return { content: [{ type: 'text', text: `Process ${args.id} is already ${entry.status}.` }] };
  try { process.kill(-entry.proc.pid, 'SIGTERM'); } catch { entry.proc.kill('SIGTERM'); }
  return { content: [{ type: 'text', text: `Sent SIGTERM to process ${args.id}.` }] };
}

const CUSTOM_TOOLS_BG = {
  [START_BG_PROCESS_TOOL.name]: { def: START_BG_PROCESS_TOOL, handler: handleStartBackgroundProcess },
  [GET_PROCESS_LOGS_TOOL.name]: { def: GET_PROCESS_LOGS_TOOL, handler: handleGetProcessLogs },
  [LIST_PROCESSES_TOOL.name]: { def: LIST_PROCESSES_TOOL, handler: handleListProcesses },
  [STOP_PROCESS_TOOL.name]: { def: STOP_PROCESS_TOOL, handler: handleStopProcess },
};

const CUSTOM_TOOLS_EXEC = {
  [RUN_SHELL_TOOL.name]: { def: RUN_SHELL_TOOL, handler: handleRunShellCommand },
  [INSTALL_PACKAGE_TOOL.name]: { def: INSTALL_PACKAGE_TOOL, handler: handleInstallPackage },
  [UPLOAD_FILE_TOOL.name]: { def: UPLOAD_FILE_TOOL, handler: handleUploadFileToWorkspace },
  [LIST_WORKSPACE_TOOL.name]: { def: LIST_WORKSPACE_TOOL, handler: handleListWorkspaceFiles },
  [DOWNLOAD_WORKSPACE_FILE_TOOL.name]: { def: DOWNLOAD_WORKSPACE_FILE_TOOL, handler: handleDownloadWorkspaceFile },
  [GREP_WORKSPACE_TOOL.name]: { def: GREP_WORKSPACE_TOOL, handler: handleGrepWorkspace },
  [READ_WORKSPACE_FILE_TOOL.name]: { def: READ_WORKSPACE_FILE_TOOL, handler: handleReadWorkspaceFile },
  [DUMP_STORAGE_STATE_TOOL.name]: { def: DUMP_STORAGE_STATE_TOOL, handler: handleDumpStorageState },
  [CAPTURE_NETWORK_TRAFFIC_TOOL.name]: { def: CAPTURE_NETWORK_TRAFFIC_TOOL, handler: handleCaptureNetworkTraffic },
};
Object.assign(CUSTOM_TOOLS, CUSTOM_TOOLS_EXEC, CUSTOM_TOOLS_BG);

const sessions = new Map(); // sessionId -> { transport, child, lastActive }

const IDLE_TIMEOUT_MS = 10 * 60_000; // 10 min idle -> reap
setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions) {
    if (now - s.lastActive > IDLE_TIMEOUT_MS) {
      console.log(`Reaping idle session ${id} (${sessions.size} active before reap)`);
      s.child.close().catch(() => {}); // triggers cleanup via child.onclose -> sessions.delete
    }
  }
}, 60_000).unref();

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
  isValidMcpToken(req.headers.authorization?.replace('Bearer ', '') || req.query.token)
    .then((ok) => ok ? next() : res.status(401).json({ error: 'unauthorized' }));
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
      sessions.set(sessionId, { transport, child, lastActive: Date.now() });
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
    // Handle any of our own tools locally — never forwarded to the child.
    if (msg?.method === 'tools/call' && CUSTOM_TOOLS[msg.params?.name]) {
      const { handler, def } = CUSTOM_TOOLS[msg.params.name];
      handler(msg.params.arguments || {})
        .then((result) => transport.send({ jsonrpc: '2.0', id: msg.id, result }))
        .catch((err) => transport.send({
          jsonrpc: '2.0', id: msg.id,
          error: { code: -32002, message: `${def.name} failed: ${err.message}` },
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
      msg.result.tools = [...msg.result.tools, ...Object.values(CUSTOM_TOOLS).map((t) => t.def)];
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
    const s = sessions.get(sessionId);
    s.lastActive = Date.now();
    transport = s.transport;
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
  session.lastActive = Date.now();
  await session.transport.handleRequest(req, res);
});

app.delete('/mcp', async (req, res) => {
  const sessionId = req.headers['mcp-session-id'];
  const session = sessionId && sessions.get(sessionId);
  if (!session) return res.status(400).send('Invalid or missing session ID');
  await session.transport.handleRequest(req, res);
});

app.listen(PORT, () => console.log(`MCP Streamable HTTP server listening on :${PORT}`));
