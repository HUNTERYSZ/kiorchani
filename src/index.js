// src/index.js — Express server, persistent process for Railway/Render
import 'dotenv/config';
import express        from 'express';
import session        from 'express-session';
import path           from 'path';
import { fileURLToPath } from 'url';
import fetch          from 'node-fetch';
import {
  startToken, stopToken, stopAll,
  getStatus, getAllStatuses, getLogs, updateTokenMeta,
} from './gateway.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app       = express();
const PORT      = process.env.PORT || 3000;

const ADMIN_PASS    = process.env.ADMIN_PASSWORD;
const SESSION_SECRET = process.env.SESSION_SECRET || 'change-me-32chars';
const DISCORD_API   = 'https://discord.com/api/v10';
const BROWSER_UA    = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.120 Safari/537.36';
const SUPER_PROPS   = Buffer.from(JSON.stringify({
  os: 'Windows', browser: 'Chrome', device: '',
  system_locale: 'en-US', browser_user_agent: BROWSER_UA,
  browser_version: '128.0.6613.120', os_version: '10',
  referrer: '', referring_domain: '',
  release_channel: 'stable', client_build_number: 335987, client_event_source: null,
})).toString('base64');

// in-memory token store: Map<id, {id, name, token, message, channelIds, intervalMs, status}>
const tokenStore = new Map();
let   tokenSeq   = 1;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 24 * 60 * 60 * 1000 },
}));
app.use(express.static(path.join(__dirname, '../public')));

function auth(req, res, next) {
  if (req.session?.authed) return next();
  res.status(401).json({ error: 'Please log in.' });
}

// ── Auth ─────────────────────────────────────────────────────────────────────

app.post('/api/login', (req, res) => {
  const { password } = req.body;
  if (!ADMIN_PASS || password !== ADMIN_PASS)
    return res.status(401).json({ error: 'Wrong password.' });
  req.session.authed = true;
  res.json({ ok: true });
});

app.post('/api/logout', (req, res) => {
  req.session.destroy();
  res.json({ ok: true });
});

app.get('/api/me', (req, res) => {
  res.json({ authed: !!req.session?.authed });
});

// ── Token getter ─────────────────────────────────────────────────────────────

app.post('/api/gettoken', auth, async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password)
    return res.status(400).json({ error: 'Email and password required.' });

  try {
    // fetch fingerprint first
    let fingerprint = null;
    try {
      const fp = await fetch(`${DISCORD_API}/experiments`, { headers: { 'User-Agent': BROWSER_UA } });
      if (fp.ok) fingerprint = (await fp.json().catch(() => ({}))).fingerprint ?? null;
    } catch {}

    const headers = {
      'Content-Type': 'application/json',
      'User-Agent': BROWSER_UA,
      'X-Super-Properties': SUPER_PROPS,
      'X-Discord-Locale': 'en-US',
      Origin: 'https://discord.com',
      Referer: 'https://discord.com/login',
    };
    if (fingerprint) headers['X-Fingerprint'] = fingerprint;

    const r = await fetch(`${DISCORD_API}/auth/login`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ login: email, password, undelete: false, login_source: null, gift_code_sku_id: null }),
    });
    const d = await r.json().catch(() => ({}));

    if (d.token) return res.json({ token: d.token });
    if (d.mfa)   return res.json({ mfa: true, ticket: d.ticket });
    if (d.captcha_key) return res.status(429).json({ captcha: true });

    const msg = d.message || d.errors?.login?._errors?.[0]?.message || 'Login failed.';
    res.status(400).json({ error: msg });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/gettoken/mfa', auth, async (req, res) => {
  const { ticket, code } = req.body;
  if (!ticket || !code)
    return res.status(400).json({ error: 'Ticket and code required.' });
  try {
    const r = await fetch(`${DISCORD_API}/auth/mfa/totp`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'User-Agent': BROWSER_UA },
      body: JSON.stringify({ ticket, code: code.replace(/\s/g, '') }),
    });
    const d = await r.json().catch(() => ({}));
    if (d.token) return res.json({ token: d.token });
    res.status(400).json({ error: d.message || 'MFA failed.' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Token CRUD ────────────────────────────────────────────────────────────────

// Add token to queue and start immediately
app.post('/api/tokens', auth, async (req, res) => {
  const { token, message, channelIds, intervalSeconds } = req.body;
  if (!token || !message || !channelIds?.length)
    return res.status(400).json({ error: 'token, message, channelIds required.' });

  const intervalMs = Math.max(60_000, (Number(intervalSeconds) || 120) * 1000);
  const id         = String(tokenSeq++);

  // resolve display name
  let name = 'Unknown';
  try {
    const r = await fetch(`${DISCORD_API}/users/@me`, {
      headers: { Authorization: token, 'User-Agent': BROWSER_UA },
    });
    const d = await r.json().catch(() => ({}));
    if (r.status === 401) return res.status(400).json({ error: 'Invalid token.' });
    name = d.global_name || d.username || 'Unknown';
  } catch {}

  const entry = { id, name, token, message, channelIds, intervalMs, status: 'running' };
  tokenStore.set(id, entry);

  startToken(id, token, { ...entry });
  res.json({ ok: true, id, name });
});

// List tokens (no raw token value exposed)
app.get('/api/tokens', auth, (req, res) => {
  const out = [];
  for (const [id, t] of tokenStore) {
    const s = getStatus(id);
    out.push({
      id,
      name:        t.name,
      message:     t.message,
      channelIds:  t.channelIds,
      intervalMs:  t.intervalMs,
      status:      s?.running ? 'running' : 'stopped',
      sentCount:   s?.sentCount ?? 0,
      nextSendAt:  s?.nextSendAt ?? null,
      startedAt:   s?.startedAt ?? null,
      logs:        s?.logs ?? [],
    });
  }
  res.json(out);
});

// Start a stopped token
app.post('/api/tokens/:id/start', auth, (req, res) => {
  const t = tokenStore.get(req.params.id);
  if (!t) return res.status(404).json({ error: 'Not found.' });
  t.status = 'running';
  stopToken(t.id); // clear any dead session
  startToken(t.id, t.token, { ...t });
  res.json({ ok: true });
});

// Stop a token
app.post('/api/tokens/:id/stop', auth, (req, res) => {
  const t = tokenStore.get(req.params.id);
  if (!t) return res.status(404).json({ error: 'Not found.' });
  t.status = 'stopped';
  stopToken(t.id);
  res.json({ ok: true });
});

// Remove token entirely
app.delete('/api/tokens/:id', auth, (req, res) => {
  stopToken(req.params.id);
  tokenStore.delete(req.params.id);
  res.json({ ok: true });
});

// Update message/interval on a running token
app.patch('/api/tokens/:id', auth, (req, res) => {
  const t = tokenStore.get(req.params.id);
  if (!t) return res.status(404).json({ error: 'Not found.' });
  if (req.body.message)         t.message     = req.body.message;
  if (req.body.intervalSeconds) t.intervalMs  = Math.max(60_000, Number(req.body.intervalSeconds) * 1000);
  if (req.body.channelIds)      t.channelIds  = req.body.channelIds;
  updateTokenMeta(t.id, t);
  res.json({ ok: true });
});

// ── Graceful shutdown ─────────────────────────────────────────────────────────

process.on('SIGTERM', () => { stopAll(); process.exit(0); });
process.on('SIGINT',  () => { stopAll(); process.exit(0); });

app.listen(PORT, () => console.log(`kior-ws running on :${PORT}`));
