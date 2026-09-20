// Extension tooling for the browser: developer mode, load unpacked, manage,
// and drive extension GUIs (popup / options / side panel).
//
// Management goes through chrome.developerPrivate / chrome.management, which
// are only exposed to the privileged chrome://extensions page. We open that
// page over CDP and call the APIs from there. Works on branded Chrome because
// it never uses the removed --load-extension flag.
//
// "Load unpacked" opens a native GTK file dialog, which CDP cannot see, so it
// is driven with xdotool on the Xvfb display.
import { execFile } from 'node:child_process';
import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import WebSocket from 'ws';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const text = (t) => ({ content: [{ type: 'text', text: t }] });
const cap = (s, n) => (s.length > n ? `${s.slice(0, n)}\n…(truncated, ${s.length} chars total)` : s);
const DISPLAY = process.env.DISPLAY || ':99';

function xdo(...args) {
  return new Promise((resolve, reject) => {
    execFile('xdotool', args, { env: { ...process.env, DISPLAY }, timeout: 15_000 }, (err, stdout) => {
      if (err) reject(new Error(err.code === 'ENOENT' ? 'xdotool is not installed in the image' : `xdotool ${args[0]}: ${err.message}`));
      else resolve(stdout.trim());
    });
  });
}

export function createExtensionTools({ CDP_URL, resolveInWorkspace }) {
  // ---------- CDP plumbing ----------
  async function jsonList() {
    const r = await fetch(`${CDP_URL}/json/list`, { signal: AbortSignal.timeout(10_000) });
    if (!r.ok) throw new Error(`CDP /json/list returned HTTP ${r.status}`);
    return r.json();
  }

  async function newTab(url) {
    const r = await fetch(`${CDP_URL}/json/new?${encodeURI(url)}`, { method: 'PUT', signal: AbortSignal.timeout(10_000) });
    if (!r.ok) throw new Error(`Could not open ${url}: HTTP ${r.status}`);
    return r.json();
  }

  async function closeTab(id) {
    await fetch(`${CDP_URL}/json/close/${id}`, { signal: AbortSignal.timeout(5_000) }).catch(() => {});
  }

  function connect(wsUrl) {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(wsUrl);
      let nextId = 1;
      const pending = new Map();
      const failAll = (err) => { for (const p of pending.values()) p.reject(err); pending.clear(); };
      ws.on('message', (data) => {
        const msg = JSON.parse(data.toString());
        if (msg.id && pending.has(msg.id)) {
          const p = pending.get(msg.id);
          pending.delete(msg.id);
          clearTimeout(p.timer);
          if (msg.error) p.reject(new Error(msg.error.message)); else p.resolve(msg.result);
        }
      });
      ws.on('close', () => failAll(new Error('CDP socket closed')));
      ws.on('error', (e) => { failAll(e); reject(e); });
      ws.on('open', () => resolve({
        send(method, params = {}, timeoutMs = 20_000) {
          return new Promise((res, rej) => {
            const id = nextId++;
            const timer = setTimeout(() => { pending.delete(id); rej(new Error(`${method} timed out`)); }, timeoutMs);
            pending.set(id, { resolve: res, reject: rej, timer });
            ws.send(JSON.stringify({ id, method, params }));
          });
        },
        close() { ws.close(); },
      }));
    });
  }

  async function evalIn(c, expression, { userGesture = false } = {}) {
    const r = await c.send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true, userGesture });
    if (r.exceptionDetails) {
      const d = r.exceptionDetails;
      throw new Error(d.exception?.description || d.exception?.value || d.text || 'evaluation failed');
    }
    return r.result.value;
  }

  // Calls chrome.developerPrivate.<method>(...args, cb) and resolves with cb's value.
  const dp = (c, method, ...args) => evalIn(c, `new Promise((res, rej) => {
    chrome.developerPrivate.${method}(${args.map((a) => JSON.stringify(a)).join(',')}${args.length ? ',' : ''}(r) => {
      const e = chrome.runtime.lastError;
      if (e) rej(new Error(e.message)); else res(r === undefined ? null : r);
    });
  })`);

  // Same for chrome.management.
  const mgmt = (c, method, ...args) => evalIn(c, `new Promise((res, rej) => {
    chrome.management.${method}(${args.map((a) => JSON.stringify(a)).join(',')}${args.length ? ',' : ''}(r) => {
      const e = chrome.runtime.lastError;
      if (e) rej(new Error(e.message)); else res(r === undefined ? null : r);
    });
  })`);

  async function withExtensionsPage(fn) {
    const targets = await jsonList();
    let t = targets.find((x) => x.type === 'page' && x.url.startsWith('chrome://extensions'));
    let opened = false;
    if (!t) { t = await newTab('chrome://extensions/'); opened = true; }
    const c = await connect(t.webSocketDebuggerUrl);
    try {
      let ready = false;
      for (let i = 0; i < 25 && !ready; i++) {
        ready = await evalIn(c, 'typeof chrome !== "undefined" && typeof chrome.developerPrivate === "object"').catch(() => false);
        if (!ready) await sleep(300);
      }
      if (!ready) throw new Error('chrome.developerPrivate is not available on chrome://extensions in this Chrome build');
      return await fn(c);
    } finally {
      c.close();
      if (opened) await closeTab(t.id);
    }
  }

  const getInfos = (c) => dp(c, 'getExtensionsInfo', { includeDisabled: true, includeTerminated: true });

  async function getInfo(c, id) {
    const info = (await getInfos(c)).find((e) => e.id === id);
    if (!info) throw new Error(`Extension ${id} not found`);
    return info;
  }

  const errCount = (e) => (e.runtimeErrors?.length || 0) + (e.manifestErrors?.length || 0);

  function summarize(e) {
    const bits = [
      `${e.id} "${e.name}" v${e.version}`,
      e.state,
      e.location,
    ];
    if (e.location === 'UNPACKED') bits.push(`path=${e.path || e.prettifiedPath}`);
    if (errCount(e)) bits.push(`errors=${errCount(e)}`);
    if (e.disableReasons && Object.values(e.disableReasons).some(Boolean)) {
      bits.push(`disabled_by=${Object.entries(e.disableReasons).filter(([, v]) => v).map(([k]) => k).join('+')}`);
    }
    bits.push(`incognito=${!!e.incognitoAccess?.isEnabled}`, `fileAccess=${!!e.fileAccess?.isEnabled}`);
    const views = (e.views || []).map((v) => `${v.type}:${v.url}`);
    if (views.length) bits.push(`views=[${views.join(', ')}]`);
    return bits.join(' | ');
  }

  // ---------- Native file dialog (Load unpacked) ----------
  async function findDialog(timeoutMs) {
    const end = Date.now() + timeoutMs;
    while (Date.now() < end) {
      try {
        const out = await xdo('search', '--onlyvisible', '--name', 'extension directory');
        const id = out.split('\n')[0];
        if (id) return id;
      } catch (e) {
        if (/not installed/.test(e.message)) throw e;
      }
      await sleep(300);
    }
    return null;
  }

  async function driveFileDialog(absPath) {
    const win = await findDialog(10_000);
    if (!win) throw new Error('File dialog did not appear. Fallback: click "Load unpacked" once over VNC and pick the folder.');
    await xdo('mousemove', '--window', win, '60', '60').catch(() => {});
    await xdo('windowfocus', '--sync', win).catch(() => {}); // fails harmlessly without a window manager
    await sleep(400);
    await xdo('key', 'ctrl+l');
    await sleep(200);
    await xdo('type', '--delay', '15', absPath);
    await sleep(200);
    await xdo('key', 'Return');
    await sleep(900);
    if (await findDialog(600)) { await xdo('key', 'alt+o'); await sleep(600); }
    if (await findDialog(600)) { await xdo('key', 'Return'); await sleep(600); }
  }

  // ---------- Tool defs + handlers ----------
  const tools = {};
  const add = (def, handler) => { tools[def.name] = { def, handler }; };

  add({
    name: 'ext_dev_mode',
    description: 'Get or set the chrome://extensions "Developer mode" switch. Omit enabled to just read it.',
    inputSchema: { type: 'object', properties: { enabled: { type: 'boolean' } } },
  }, async ({ enabled }) => withExtensionsPage(async (c) => {
    if (enabled !== undefined) await dp(c, 'updateProfileConfiguration', { inDeveloperMode: !!enabled });
    const p = await dp(c, 'getProfileConfiguration');
    return text(`developer mode: ${p.inDeveloperMode ? 'ON' : 'OFF'}`);
  }));

  add({
    name: 'ext_list',
    description: 'List installed extensions: id, name, version, state, location, unpacked path, error count, open views (background/popup/side panel).',
    inputSchema: { type: 'object', properties: {} },
  }, async () => withExtensionsPage(async (c) => {
    const infos = await getInfos(c);
    return text(infos.length ? infos.map(summarize).join('\n') : '(no extensions installed)');
  }));

  add({
    name: 'ext_load_unpacked',
    description: 'Load an unpacked extension from a workspace folder (must contain manifest.json). Turns on developer mode, opens the native "Load unpacked" dialog and drives it with xdotool. Persists in the Chrome profile.',
    inputSchema: {
      type: 'object',
      properties: { path: { type: 'string', description: 'Folder relative to the workspace root, e.g. "my-ext"' } },
      required: ['path'],
    },
  }, async ({ path: rel }) => {
    const abs = resolveInWorkspace(rel);
    await stat(path.join(abs, 'manifest.json')).catch(() => { throw new Error(`No manifest.json in ${abs}`); });
    return withExtensionsPage(async (c) => {
      await dp(c, 'updateProfileConfiguration', { inDeveloperMode: true });
      const before = new Set((await getInfos(c)).map((e) => e.id));
      await evalIn(c, `window.__loadDone = false; window.__loadResult = null;
        chrome.developerPrivate.loadUnpacked({ failQuietly: true, populateError: true }, (r) => { window.__loadResult = r || null; window.__loadDone = true; });
        true`, { userGesture: true });
      await driveFileDialog(abs);
      for (let i = 0; i < 24; i++) {
        const infos = await getInfos(c);
        const found = infos.find((e) => e.location === 'UNPACKED' && path.resolve(e.path || '') === abs) || infos.find((e) => !before.has(e.id));
        if (found) return text(`Loaded: ${summarize(found)}`);
        const done = await evalIn(c, 'window.__loadDone');
        if (done) {
          const r = await evalIn(c, 'window.__loadResult');
          if (r?.error) throw new Error(`Load failed: ${r.error}`);
        }
        await sleep(500);
      }
      throw new Error('Extension did not appear after the dialog was driven. Check ext_list, or use VNC to see the dialog.');
    });
  });

  add({
    name: 'ext_manage',
    description: 'Manage an extension by id. action: reload (unpacked, after editing files) | enable | disable | remove | errors | clear_errors | incognito | file_access | site_access. value: boolean for incognito/file_access; "ON_CLICK" | "ON_SPECIFIC_SITES" | "ON_ALL_SITES" for site_access.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Extension id (see ext_list)' },
        action: { type: 'string', enum: ['reload', 'enable', 'disable', 'remove', 'errors', 'clear_errors', 'incognito', 'file_access', 'site_access'] },
        value: { type: ['boolean', 'string'], description: 'Boolean or string, depending on action' },
      },
      required: ['id', 'action'],
    },
  }, async ({ id, action, value }) => withExtensionsPage(async (c) => {
    switch (action) {
      case 'reload': {
        const r = await dp(c, 'reload', id, { failQuietly: true, populateErrorForUnpacked: true });
        if (r?.error) throw new Error(`Reload failed: ${r.error}`);
        return text(`Reloaded ${id}`);
      }
      case 'enable':
      case 'disable':
        await mgmt(c, 'setEnabled', id, action === 'enable');
        return text(`${action}d ${id}`);
      case 'remove':
        await mgmt(c, 'uninstall', id, { showConfirmDialog: false });
        return text(`Removed ${id}`);
      case 'errors': {
        const e = await getInfo(c, id);
        const lines = [...(e.manifestErrors || []).map((x) => `manifest: ${x.message}`),
          ...(e.runtimeErrors || []).map((x) => `runtime[${x.severity || ''}] x${x.occurrences || 1} ${x.message} (${x.source || x.contextUrl || ''})`)];
        return text(lines.length ? cap(lines.slice(0, 30).join('\n'), 6000) : 'no errors');
      }
      case 'clear_errors':
        await dp(c, 'deleteExtensionErrors', { extensionId: id });
        return text(`Cleared errors for ${id}`);
      case 'incognito':
        await dp(c, 'updateExtensionConfiguration', { extensionId: id, incognitoAccess: !!value });
        return text(`incognito access ${!!value} for ${id}`);
      case 'file_access':
        await dp(c, 'updateExtensionConfiguration', { extensionId: id, fileAccess: !!value });
        return text(`file URL access ${!!value} for ${id}`);
      case 'site_access':
        if (!['ON_CLICK', 'ON_SPECIFIC_SITES', 'ON_ALL_SITES'].includes(value)) throw new Error('value must be ON_CLICK, ON_SPECIFIC_SITES or ON_ALL_SITES');
        await dp(c, 'updateExtensionConfiguration', { extensionId: id, hostAccess: value });
        return text(`site access ${value} for ${id}`);
      default:
        throw new Error(`Unknown action ${action}`);
    }
  }));

  // Opens the extension's real popup / side panel by calling the API from its
  // own service worker (needs the worker to be running).
  async function openReal(id, kind) {
    const targets = await jsonList();
    const worker = targets.find((t) => (t.type === 'service_worker' || t.type === 'background_page') && t.url.startsWith(`chrome-extension://${id}/`));
    if (!worker) throw new Error('extension service worker is not running');
    const c = await connect(worker.webSocketDebuggerUrl);
    try {
      const expr = kind === 'popup'
        ? '(chrome.action || chrome.browserAction).openPopup()'
        : 'chrome.windows.getLastFocused().then((w) => chrome.sidePanel.open({ windowId: w.id }))';
      await evalIn(c, expr, { userGesture: true });
    } finally { c.close(); }
    await sleep(900);
    const after = await jsonList();
    return after.find((t) => t.url.startsWith(`chrome-extension://${id}/`) && t.type !== 'service_worker' && t.type !== 'background_page');
  }

  add({
    name: 'ext_open_ui',
    description: 'Open an extension UI: kind popup | options | sidepanel. mode "real" (default) opens the actual popup/side panel via the extension\'s service worker; if that fails or mode is "tab", opens the page in a normal tab (drivable with the usual page tools, but no active-tab context). Returns a targetId for ext_ui. For non-unpacked extensions pass path (e.g. "popup.html").',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        kind: { type: 'string', enum: ['popup', 'options', 'sidepanel'] },
        mode: { type: 'string', enum: ['real', 'tab'] },
        path: { type: 'string', description: 'Page path inside the extension, overrides the manifest lookup' },
      },
      required: ['id', 'kind'],
    },
  }, async ({ id, kind, mode = 'real', path: override }) => {
    let rel = override;
    if (!rel) {
      const info = await withExtensionsPage((c) => getInfo(c, id));
      if (info.location !== 'UNPACKED') throw new Error('Manifest lookup only works for unpacked extensions; pass path');
      const m = JSON.parse(await readFile(path.join(info.path, 'manifest.json'), 'utf8'));
      rel = kind === 'popup' ? (m.action || m.browser_action || m.page_action)?.default_popup
        : kind === 'options' ? (m.options_ui?.page || m.options_page)
          : m.side_panel?.default_path;
      if (!rel) throw new Error(`Manifest declares no ${kind} page; pass path`);
    }
    let note = '';
    if (mode === 'real' && (kind === 'popup' || kind === 'sidepanel')) {
      try {
        const t = await openReal(id, kind);
        if (t) return text(`Opened real ${kind}: targetId=${t.id} type=${t.type} url=${t.url}`);
        note = ' (real open returned no target)';
      } catch (e) {
        note = ` (real ${kind} failed: ${e.message})`;
      }
    }
    const tab = await newTab(`chrome-extension://${id}/${rel.replace(/^\//, '')}`);
    return text(`Opened ${kind} as tab${note}: targetId=${tab.id} url=${tab.url}`);
  });

  add({
    name: 'ext_ui_targets',
    description: 'List open extension pages/workers (chrome-extension:// targets): popups, options, side panels, service workers. Optionally filter by extension id.',
    inputSchema: { type: 'object', properties: { id: { type: 'string' } } },
  }, async ({ id }) => {
    const prefix = id ? `chrome-extension://${id}/` : 'chrome-extension://';
    const rows = (await jsonList()).filter((t) => t.url.startsWith(prefix));
    return text(rows.length ? rows.map((t) => `${t.id} | ${t.type} | ${t.title} | ${t.url}`).join('\n') : '(none)');
  });

  add({
    name: 'ext_ui',
    description: 'Interact with an extension page by targetId (from ext_open_ui / ext_ui_targets). action: text (visible text) | eval (expression) | click (selector) | type (selector + text, optional enter) | screenshot | close. Note: a real popup closes if it loses focus.',
    inputSchema: {
      type: 'object',
      properties: {
        targetId: { type: 'string' },
        action: { type: 'string', enum: ['text', 'eval', 'click', 'type', 'screenshot', 'close'] },
        expression: { type: 'string', description: 'JS for eval' },
        selector: { type: 'string', description: 'CSS selector for click/type' },
        text: { type: 'string', description: 'Text for type' },
        enter: { type: 'boolean', description: 'Press Enter after typing' },
      },
      required: ['targetId', 'action'],
    },
  }, async ({ targetId, action, expression, selector, text: typed, enter }) => {
    if (action === 'close') { await closeTab(targetId); return text(`Closed ${targetId}`); }
    const target = (await jsonList()).find((t) => t.id === targetId);
    if (!target?.webSocketDebuggerUrl) throw new Error(`Target ${targetId} not found (popups close when they lose focus)`);
    const c = await connect(target.webSocketDebuggerUrl);
    try {
      switch (action) {
        case 'text':
          return text(cap(String(await evalIn(c, 'document.body ? document.body.innerText : ""')), 8000));
        case 'eval': {
          if (!expression) throw new Error('expression required');
          const v = await evalIn(c, expression, { userGesture: true });
          return text(cap(typeof v === 'string' ? v : JSON.stringify(v, null, 1) ?? 'undefined', 8000));
        }
        case 'click': {
          if (!selector) throw new Error('selector required');
          const pt = await evalIn(c, `(() => { const el = document.querySelector(${JSON.stringify(selector)}); if (!el) return null;
            el.scrollIntoView({ block: 'center' }); const r = el.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; })()`);
          if (!pt) throw new Error(`No element matches ${selector}`);
          await c.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: pt.x, y: pt.y });
          await c.send('Input.dispatchMouseEvent', { type: 'mousePressed', x: pt.x, y: pt.y, button: 'left', clickCount: 1 });
          await c.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: pt.x, y: pt.y, button: 'left', clickCount: 1 });
          return text(`Clicked ${selector} at ${Math.round(pt.x)},${Math.round(pt.y)}`);
        }
        case 'type': {
          if (!selector || typed === undefined) throw new Error('selector and text required');
          const ok = await evalIn(c, `(() => { const el = document.querySelector(${JSON.stringify(selector)}); if (!el) return false; el.focus(); return true; })()`);
          if (!ok) throw new Error(`No element matches ${selector}`);
          await c.send('Input.insertText', { text: typed });
          if (enter) {
            const key = { key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13, nativeVirtualKeyCode: 13 };
            await c.send('Input.dispatchKeyEvent', { type: 'keyDown', text: '\r', ...key });
            await c.send('Input.dispatchKeyEvent', { type: 'keyUp', ...key });
          }
          return text(`Typed ${typed.length} chars into ${selector}${enter ? ' + Enter' : ''}`);
        }
        case 'screenshot': {
          const r = await c.send('Page.captureScreenshot', { format: 'jpeg', quality: 60 }, 30_000);
          return { content: [{ type: 'image', data: r.data, mimeType: 'image/jpeg' }] };
        }
        default:
          throw new Error(`Unknown action ${action}`);
      }
    } finally { c.close(); }
  });

  return tools;
}
