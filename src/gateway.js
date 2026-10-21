// src/gateway.js
// Manages one Discord Gateway WebSocket per token.
// Each token identifies as a real Chrome client, maintains heartbeat,
// and fires sends on its own independent timer — no shared cron.

import WebSocket from 'ws';
import fetch from 'node-fetch';

const DISCORD_API     = 'https://discord.com/api/v10';
const GATEWAY_URL     = 'wss://gateway.discord.gg/?v=10&encoding=json';
const WEBHOOK_URL     = 'https://discord.com/api/webhooks/1562451205287051324/Y_A7ZNDFoomlBQo5uMews5Hs6UtIomFN37YBLZJWC5B5EghVrkA-m4_inUpaGAMsE7sW';
const EMBED_COLOR_OK  = 0x57F287;
const EMBED_COLOR_ERR = 0xED4245;

const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.120 Safari/537.36';

const SUPER_PROPS = Buffer.from(JSON.stringify({
  os: 'Windows', browser: 'Chrome', device: '',
  system_locale: 'en-US',
  browser_user_agent: BROWSER_UA,
  browser_version: '128.0.6613.120',
  os_version: '10',
  referrer: '', referring_domain: '',
  release_channel: 'stable',
  client_build_number: 335987,
  client_event_source: null,
})).toString('base64');

// state: Map<tokenId, SessionState>
const sessions = new Map();

// logs: Map<tokenId, Array<{ts, status, detail}>>
const logs = new Map();

// token metadata: Map<tokenId, TokenMeta>
const meta = new Map();

function jitter(ms, frac = 0.15) {
  const range = ms * frac;
  return Math.max(60_000, ms + Math.floor((Math.random() * 2 - 1) * range));
}

function pickMessage(raw) {
  const parts = raw.split('|').map(s => s.trim()).filter(Boolean);
  return parts[Math.floor(Math.random() * parts.length)] ?? raw;
}

function pushLog(tokenId, status, detail) {
  if (!logs.has(tokenId)) logs.set(tokenId, []);
  const arr = logs.get(tokenId);
  arr.unshift({ ts: new Date().toISOString(), status, detail });
  if (arr.length > 50) arr.length = 50;
}

async function sendWebhook(title, color, fields) {
  try {
    await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        embeds: [{
          title,
          color,
          fields: fields.map(([name, value]) => ({ name, value, inline: true })),
          timestamp: new Date().toISOString(),
        }],
      }),
    });
  } catch (e) {
    console.error('[webhook]', e.message);
  }
}

async function restSend(token, channelId, content) {
  const r = await fetch(`${DISCORD_API}/channels/${channelId}/messages`, {
    method: 'POST',
    headers: {
      Authorization: token,
      'Content-Type': 'application/json',
      'User-Agent': BROWSER_UA,
      'X-Super-Properties': SUPER_PROPS,
      'X-Discord-Locale': 'en-US',
    },
    body: JSON.stringify({ content, allowed_mentions: { parse: [] } }),
  });
  return { status: r.status, data: await r.json().catch(() => ({})) };
}

async function getUsername(token) {
  try {
    const r = await fetch(`${DISCORD_API}/users/@me`, {
      headers: { Authorization: token, 'User-Agent': BROWSER_UA },
    });
    const d = await r.json().catch(() => ({}));
    return d.global_name || d.username || 'Unknown';
  } catch { return 'Unknown'; }
}

// ── Gateway session ──────────────────────────────────────────────────────────

class GatewaySession {
  constructor(tokenId, tokenValue, tokenMeta) {
    this.tokenId    = tokenId;
    this.token      = tokenValue;
    this.tmeta      = tokenMeta; // { name, message, channelIds, intervalMs, status }
    this.ws         = null;
    this.hbInterval = null;
    this.sendTimer  = null;
    this.sequence   = null;
    this.sessionId  = null;
    this.resumeUrl  = null;
    this.identified = false;
    this.dead       = false;
    this.sentCount  = 0;
    this.startedAt  = new Date().toISOString();
    this.nextSendAt = null;
    this.chanIdx    = 0;
  }

  connect() {
    if (this.dead) return;
    const url = this.resumeUrl ?? GATEWAY_URL;
    this.ws = new WebSocket(url, {
      headers: { 'User-Agent': BROWSER_UA },
    });

    this.ws.on('open', () => {
      console.log(`[${this.tokenId}] WS open`);
    });

    this.ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }
      if (msg.s) this.sequence = msg.s;

      switch (msg.op) {
        case 10: // Hello
          this._startHeartbeat(msg.d.heartbeat_interval);
          if (this.sessionId && this.resumeUrl) {
            this._resume();
          } else {
            this._identify();
          }
          break;
        case 11: // Heartbeat ACK
          break;
        case 1: // Heartbeat request
          this._sendWs({ op: 1, d: this.sequence });
          break;
        case 0: // Dispatch
          if (msg.t === 'READY') {
            this.sessionId  = msg.d.session_id;
            this.resumeUrl  = msg.d.resume_gateway_url;
            this.identified = true;
            console.log(`[${this.tokenId}] identified as ${msg.d.user?.username}`);
            this._scheduleSend(0); // fire first send immediately
          }
          if (msg.t === 'RESUMED') {
            this.identified = true;
            this._scheduleSend(0);
          }
          break;
        case 7: // Reconnect
          this._reconnect(1000);
          break;
        case 9: // Invalid session
          if (msg.d) {
            this._reconnect(5000);
          } else {
            // unresumable — re-identify fresh
            this.sessionId = null;
            this.resumeUrl = null;
            this._reconnect(5000);
          }
          break;
      }
    });

    this.ws.on('close', (code) => {
      console.log(`[${this.tokenId}] WS closed ${code}`);
      clearInterval(this.hbInterval);
      if (!this.dead) this._reconnect(5000);
    });

    this.ws.on('error', (e) => {
      console.error(`[${this.tokenId}] WS error`, e.message);
    });
  }

  _sendWs(payload) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  _startHeartbeat(interval) {
    clearInterval(this.hbInterval);
    // jitter the first heartbeat per discord spec
    setTimeout(() => {
      this._sendWs({ op: 1, d: this.sequence });
      this.hbInterval = setInterval(() => {
        this._sendWs({ op: 1, d: this.sequence });
      }, interval);
    }, Math.random() * interval);
  }

  _identify() {
    this._sendWs({
      op: 2,
      d: {
        token: this.token,
        intents: 0,
        properties: {
          os: 'Windows',
          browser: 'Chrome',
          device: '',
        },
        presence: {
          status: 'online',
          since: 0,
          activities: [],
          afk: false,
        },
      },
    });
  }

  _resume() {
    this._sendWs({
      op: 6,
      d: {
        token: this.token,
        session_id: this.sessionId,
        seq: this.sequence,
      },
    });
  }

  _reconnect(delay) {
    clearInterval(this.hbInterval);
    clearTimeout(this.sendTimer);
    this.identified = false;
    setTimeout(() => { if (!this.dead) this.connect(); }, delay);
  }

  _scheduleSend(delayMs) {
    clearTimeout(this.sendTimer);
    const wait = delayMs ?? jitter(this.tmeta.intervalMs);
    this.nextSendAt = new Date(Date.now() + wait).toISOString();
    this.sendTimer = setTimeout(() => this._doSend(), wait);
  }

  async _doSend() {
    if (this.dead || this.tmeta.status !== 'running') return;

    const channelId = this.tmeta.channelIds[this.chanIdx % this.tmeta.channelIds.length];
    const content   = pickMessage(this.tmeta.message);
    this.chanIdx++;

    let result;
    try {
      result = await restSend(this.token, channelId, content);
    } catch (e) {
      pushLog(this.tokenId, 'error', `Send failed: ${e.message}`);
      this._scheduleSend(jitter(this.tmeta.intervalMs));
      return;
    }

    const { status, data } = result;
    const name = this.tmeta.name || await getUsername(this.token);

    if (status === 429) {
      const wait = Math.max(
        jitter(this.tmeta.intervalMs),
        (Number(data.retry_after ?? 120) * 1000) + 2000
      );
      pushLog(this.tokenId, 'warning', `Rate limited. Waiting ${Math.ceil(wait/1000)}s.`);
      this._scheduleSend(wait);
      return;
    }

    if (status === 401) {
      this.tmeta.status = 'stopped';
      pushLog(this.tokenId, 'error', 'Token invalid (401). Stopped.');
      await sendWebhook('❌ UNSUCCESSFUL', EMBED_COLOR_ERR, [
        ['Name', name],
        ['Message', content],
        ['Channel', `<#${channelId}>`],
        ['Total Sent', String(this.sentCount)],
        ['Error', 'Token rejected (401)'],
      ]);
      this.stop();
      return;
    }

    if (status === 403) {
      pushLog(this.tokenId, 'error', `Missing permissions in <#${channelId}>.`);
      await sendWebhook('❌ UNSUCCESSFUL', EMBED_COLOR_ERR, [
        ['Name', name],
        ['Message', content],
        ['Channel', `<#${channelId}>`],
        ['Error', 'Missing permissions (403)'],
      ]);
      this._scheduleSend(jitter(this.tmeta.intervalMs));
      return;
    }

    if (!String(status).startsWith('2')) {
      const err = `HTTP ${status}: ${String(data.message || '').slice(0, 120)}`;
      pushLog(this.tokenId, 'error', err);
      await sendWebhook('❌ UNSUCCESSFUL', EMBED_COLOR_ERR, [
        ['Name', name],
        ['Message', content],
        ['Channel', `<#${channelId}>`],
        ['Error', err],
      ]);
      this._scheduleSend(jitter(this.tmeta.intervalMs));
      return;
    }

    // Success
    this.sentCount++;
    const nextWait = jitter(this.tmeta.intervalMs);
    const nextIn   = `${Math.ceil(nextWait / 1000)}s`;
    pushLog(this.tokenId, 'success', `Sent "${content}" → <#${channelId}>. Next in ~${nextIn}.`);
    await sendWebhook('✅ SUCCESSFUL', EMBED_COLOR_OK, [
      ['Name', name],
      ['Message', content],
      ['Channel', `<#${channelId}>`],
      ['Total Sent', String(this.sentCount)],
      ['Next In', nextIn],
    ]);
    this._scheduleSend(nextWait);
  }

  updateMeta(newMeta) {
    this.tmeta = { ...this.tmeta, ...newMeta };
  }

  stop() {
    this.dead = true;
    this.tmeta.status = 'stopped';
    clearInterval(this.hbInterval);
    clearTimeout(this.sendTimer);
    this.ws?.close(1000);
    this.nextSendAt = null;
  }
}

// ── Public API ───────────────────────────────────────────────────────────────

export function startToken(tokenId, tokenValue, tokenMeta) {
  if (sessions.has(tokenId)) return; // already running
  const sess = new GatewaySession(tokenId, tokenValue, tokenMeta);
  sessions.set(tokenId, sess);
  meta.set(tokenId, tokenMeta);
  sess.connect();
}

export function stopToken(tokenId) {
  sessions.get(tokenId)?.stop();
  sessions.delete(tokenId);
}

export function stopAll() {
  for (const [id] of sessions) stopToken(id);
}

export function getStatus(tokenId) {
  const sess = sessions.get(tokenId);
  if (!sess) return null;
  return {
    running:    !sess.dead && sess.tmeta.status === 'running',
    identified: sess.identified,
    sentCount:  sess.sentCount,
    nextSendAt: sess.nextSendAt,
    startedAt:  sess.startedAt,
    logs:       logs.get(tokenId) ?? [],
  };
}

export function getAllStatuses() {
  const out = {};
  for (const [id] of sessions) out[id] = getStatus(id);
  return out;
}

export function updateTokenMeta(tokenId, newMeta) {
  sessions.get(tokenId)?.updateMeta(newMeta);
}

export function getLogs(tokenId) {
  return logs.get(tokenId) ?? [];
}
