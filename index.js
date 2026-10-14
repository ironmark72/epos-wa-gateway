/**
 * EPOS WhatsApp gateway (free-tier friendly).
 *
 *   EPOS (Vercel) ──writes──▶ "WaOutbox" (Supabase Postgres, tenant schema) ◀──claims one at a time── this gateway ──▶ WhatsApp
 *   WhatsApp ──▶ this gateway ──POST──▶ EPOS /api/whatsapp/gateway  (EPOS decides what a message means)
 *
 * - Baileys (WhatsApp Web protocol, no Chromium): ~100 MB RAM, fits Render's free 512 MB.
 * - The WhatsApp login lives in Postgres ("WaAuth"), so restarts, deploys and Render's free-tier sleep never
 *   need a new QR scan.
 * - Linking is done from EPOS (Integrations → WhatsApp): it writes a command into "WaState" and shows the
 *   QR / pairing code this process writes back.
 * - Sends one message at a time with a short randomised gap, checks the number is on WhatsApp first.
 * - A Postgres advisory lock makes sure only one copy runs (two copies would knock each other's session off).
 *
 * Env: DATABASE_URL (Supabase *session* pooler, port 5432), DB_SCHEMA (tenant_ekaksh), EPOS_URL,
 *      GATEWAY_SECRET (same value as WA_GATEWAY_SECRET on Vercel), PORT (Render sets it).
 * HTTP: GET /health only (Supabase pg_cron pings it every 10 minutes so the free instance never sleeps).
 */
const http = require("node:http");
const { Pool } = require("pg");
const pino = require("pino");
const QRCode = require("qrcode");
const {
  default: makeWASocket,
  BufferJSON,
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  initAuthCreds,
  makeCacheableSignalKeyStore,
  proto,
} = require("baileys");

const LOCK_ID = 727272002;
const SCHEMA = (process.env.DB_SCHEMA || "tenant_ekaksh").replace(/[^a-z0-9_]/gi, "");
const EPOS_URL = (process.env.EPOS_URL || "https://epos-pi.vercel.app").replace(/\/$/, "");
const SECRET = process.env.GATEWAY_SECRET || "";
const GAP_MS = Math.max(2000, Number(process.env.GAP_MS) || 5000);
const logger = pino({ level: process.env.LOG_LEVEL || "warn" });
const log = (...a) => console.log(new Date().toISOString(), ...a);

for (const k of ["DATABASE_URL", "GATEWAY_SECRET"]) {
  if (!process.env[k]) {
    console.error(`${k} is not set`);
    process.exit(1);
  }
}
const pool = new Pool({
  connectionString: process.env.DATABASE_URL.replace(/[?&]sslmode=[^&]*/g, ""),
  ssl: /localhost|127\.0\.0\.1/.test(process.env.DATABASE_URL) ? undefined : { rejectUnauthorized: false },
  options: `-c search_path=${SCHEMA}`,
  max: 4,
  idleTimeoutMillis: 30_000,
});
pool.on("error", (e) => log("[pg] pool error", e.message));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ---------------- shared state row ---------------- */

async function setState(patch) {
  const keys = Object.keys(patch);
  if (!keys.length) return;
  const cols = keys.map((k, i) => `"${k}" = $${i + 1}`).join(", ");
  await pool.query(`update "WaState" set ${cols}, "heartbeatAt" = now() where id = 1`, keys.map((k) => patch[k])).catch((e) => log("[state]", e.message));
}

/* ---------------- WhatsApp login stored in Postgres ---------------- */

async function usePgAuthState() {
  const read = async (key) => {
    const { rows } = await pool.query(`select value from "WaAuth" where key = $1`, [key]);
    return rows[0] ? JSON.parse(rows[0].value, BufferJSON.reviver) : null;
  };
  const write = (key, data) =>
    pool.query(`insert into "WaAuth" (key, value) values ($1, $2) on conflict (key) do update set value = excluded.value`, [key, JSON.stringify(data, BufferJSON.replacer)]);
  const creds = (await read("creds")) || initAuthCreds();
  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const out = {};
          if (!ids.length) return out;
          const { rows } = await pool.query(`select key, value from "WaAuth" where key = any($1)`, [ids.map((id) => `${type}-${id}`)]);
          for (const r of rows) {
            let v = JSON.parse(r.value, BufferJSON.reviver);
            if (type === "app-state-sync-key" && v) v = proto.Message.AppStateSyncKeyData.fromObject(v);
            out[r.key.slice(type.length + 1)] = v;
          }
          return out;
        },
        set: async (data) => {
          const client = await pool.connect();
          try {
            await client.query("begin");
            for (const type of Object.keys(data)) {
              for (const id of Object.keys(data[type])) {
                const key = `${type}-${id}`;
                const value = data[type][id];
                if (value) await client.query(`insert into "WaAuth" (key, value) values ($1, $2) on conflict (key) do update set value = excluded.value`, [key, JSON.stringify(value, BufferJSON.replacer)]);
                else await client.query(`delete from "WaAuth" where key = $1`, [key]);
              }
            }
            await client.query("commit");
          } catch (e) {
            await client.query("rollback").catch(() => {});
            throw e;
          } finally {
            client.release();
          }
        },
      },
    },
    saveCreds: () => write("creds", creds),
  };
}

const clearAuth = () => pool.query(`delete from "WaAuth"`);

/* ---------------- connection ---------------- */

let sock = null;
let connected = false;
let starting = false;
let linkUntil = 0; // when not linked, only show QRs while someone is actually trying to link (EPOS button)
let pairPhone = null;
let pairRequested = false;
let retry = 0;

async function isRegistered() {
  const { rows } = await pool.query(`select value from "WaAuth" where key = 'creds'`);
  if (!rows[0]) return false;
  try {
    return Boolean(JSON.parse(rows[0].value, BufferJSON.reviver).registered);
  } catch {
    return false;
  }
}

async function start() {
  if (starting || sock) return;
  starting = true;
  try {
    const registered = await isRegistered();
    if (!registered && Date.now() > linkUntil) {
      await setState({ status: "not_linked", qrSvg: null, pairingCode: null, me: null });
      return;
    }
    const { state, saveCreds } = await usePgAuthState();
    let version;
    try {
      version = (await fetchLatestBaileysVersion()).version;
    } catch {}
    await setState({ status: "connecting", error: null });
    log("[conn] connecting", registered ? "(linked)" : "(linking)", version ? version.join(".") : "");
    const s = makeWASocket({
      version,
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      logger,
      browser: Browsers.ubuntu("Ekaksh EPOS"),
      markOnlineOnConnect: false,
      syncFullHistory: false,
      shouldSyncHistoryMessage: () => false,
      generateHighQualityLinkPreview: false,
    });
    sock = s;
    pairRequested = false;
    s.ev.on("creds.update", saveCreds);
    s.ev.on("connection.update", (u) => onConnection(s, u).catch((e) => log("[conn]", e.message)));
    s.ev.on("messages.upsert", (m) => onIncoming(m).catch((e) => log("[in]", e.message)));
  } finally {
    starting = false;
  }
}

async function onConnection(s, u) {
  if (s !== sock) return;
  if (u.qr) {
    if (pairPhone && !pairRequested) {
      pairRequested = true;
      try {
        const code = await s.requestPairingCode(pairPhone);
        await setState({ status: "qr", pairingCode: code, qrSvg: await QRCode.toString(u.qr, { type: "svg", margin: 1 }) });
        log("[link] pairing code issued for", pairPhone.slice(0, 4) + "******");
        return;
      } catch (e) {
        await setState({ error: `Pairing code failed: ${e.message}` });
      }
    }
    await setState({ status: "qr", qrSvg: await QRCode.toString(u.qr, { type: "svg", margin: 1 }) });
  }
  if (u.connection === "open") {
    connected = true;
    retry = 0;
    linkUntil = 0;
    pairPhone = null;
    const me = String(s.user?.id ?? "").split(":")[0].split("@")[0];
    await setState({ status: "connected", me, qrSvg: null, pairingCode: null, error: null, connectedAt: new Date() });
    log("[conn] connected as", me.slice(0, 4) + "******");
  }
  if (u.connection === "close") {
    connected = false;
    sock = null;
    const code = u.lastDisconnect?.error?.output?.statusCode;
    log("[conn] closed", code, u.lastDisconnect?.error?.message ?? "");
    if (code === DisconnectReason.loggedOut) {
      await clearAuth();
      await setState({ status: "logged_out", me: null, qrSvg: null, pairingCode: null, error: "WhatsApp was unlinked from the phone. Link it again." });
      return;
    }
    if (code === DisconnectReason.connectionReplaced) {
      await setState({ status: "offline", error: "Another copy of the gateway took over this WhatsApp session." });
      await sleep(60_000);
    }
    retry = Math.min(retry + 1, 8);
    setTimeout(() => start().catch((e) => log("[start]", e.message)), Math.min(60_000, 2000 * 2 ** retry));
  }
}

/* ---------------- incoming messages -> EPOS ---------------- */

function textOf(msg) {
  const m = msg.message || {};
  return (
    m.conversation ||
    m.extendedTextMessage?.text ||
    m.imageMessage?.caption ||
    m.videoMessage?.caption ||
    m.buttonsResponseMessage?.selectedDisplayText ||
    m.listResponseMessage?.title ||
    (m.imageMessage ? "[photo]" : m.audioMessage ? "[voice note]" : m.documentMessage ? "[document]" : m.videoMessage ? "[video]" : m.stickerMessage ? "[sticker]" : "")
  );
}

async function deliverToEpos(payload) {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(`${EPOS_URL}/api/whatsapp/gateway`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-gateway-secret": SECRET },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(30_000),
      });
      if (res.ok) return true;
      log("[in] EPOS answered", res.status, (await res.text()).slice(0, 200));
      if (res.status === 401 || res.status === 400) return false;
    } catch (e) {
      log("[in] EPOS unreachable:", e.message);
    }
    await sleep(3000 * attempt);
  }
  return false;
}

async function onIncoming({ messages, type }) {
  if (type !== "notify") return;
  for (const msg of messages) {
    const jid = msg.key?.remoteJid || "";
    if (msg.key?.fromMe || !jid || jid.endsWith("@g.us") || jid === "status@broadcast" || jid.endsWith("@newsletter")) continue;
    const pnJid = jid.endsWith("@s.whatsapp.net") ? jid : msg.key?.senderPn || msg.key?.participantPn || "";
    const digits = String(pnJid).split("@")[0].split(":")[0];
    if (!/^\d{8,15}$/.test(digits)) continue;
    const text = textOf(msg);
    if (!text) continue;
    const ok = await deliverToEpos({ from: digits, name: msg.pushName || null, text: text.slice(0, 4000), waId: msg.key.id });
    log("[in]", digits.slice(0, 4) + "******", ok ? "delivered" : "NOT delivered");
  }
}

/* ---------------- outbox worker ---------------- */

async function claim() {
  const { rows } = await pool.query(`
    update "WaOutbox" set status = 'SENDING', attempts = attempts + 1
    where id = (
      select id from "WaOutbox"
      where (status = 'QUEUED' and "notBefore" <= now())
         or (status = 'SENDING' and "createdAt" < now() - interval '10 minutes' and attempts < 3)
      order by "notBefore", "createdAt"
      limit 1
      for update skip locked
    )
    returning id, "to", body, kind, attempts`);
  return rows[0] || null;
}

async function sendOne(m) {
  const digits = String(m.to).replace(/\D/g, "");
  const number = digits.length === 10 ? `91${digits}` : digits;
  const [hit] = await sock.onWhatsApp(`${number}@s.whatsapp.net`);
  if (!hit?.exists) throw Object.assign(new Error("This number is not on WhatsApp."), { permanent: true });
  const jid = hit.jid;
  // Look like a person: show "typing…" for a moment proportional to the message length.
  await sock.presenceSubscribe(jid).catch(() => {});
  await sock.sendPresenceUpdate("composing", jid).catch(() => {});
  await sleep(Math.min(3000, 800 + m.body.length * 10));
  await sock.sendPresenceUpdate("paused", jid).catch(() => {});
  const res = await sock.sendMessage(jid, { text: m.body });
  return res?.key?.id ?? null;
}

async function work() {
  for (;;) {
    try {
      if (!connected || !sock) {
        await sleep(5000);
        continue;
      }
      const m = await claim();
      if (!m) {
        await sleep(3000);
        continue;
      }
      try {
        const waId = await sendOne(m);
        await pool.query(`update "WaOutbox" set status = 'SENT', "sentAt" = now(), "waId" = $2, error = null where id = $1`, [m.id, waId]);
        log("[send] sent", m.id, m.kind);
      } catch (e) {
        const permanent = e.permanent || m.attempts >= 3;
        await pool.query(
          permanent
            ? `update "WaOutbox" set status = 'FAILED', error = $2 where id = $1`
            : `update "WaOutbox" set status = 'QUEUED', error = $2, "notBefore" = now() + interval '2 minutes' where id = $1`,
          [m.id, String(e.message || e).slice(0, 500)],
        );
        log("[send] failed", m.id, e.message);
      }
      // Human-ish pace between messages: gap .. 1.6 x gap.
      await sleep(GAP_MS * (1 + Math.random() * 0.6));
    } catch (e) {
      log("[worker]", e.message);
      await sleep(10_000);
    }
  }
}

/* ---------------- commands from EPOS ---------------- */

async function commands() {
  for (;;) {
    try {
      // RETURNING gives the updated row, so read the command through a CTE before clearing it.
      const { rows } = await pool.query(`
        with c as (select command, "commandArg" from "WaState" where id = 1 and command is not null for update)
        update "WaState" s set command = null, "commandArg" = null from c where s.id = 1
        returning c.command, c."commandArg" as command_arg`);
      const c = rows[0];
      if (c) log("[cmd]", c.command);
      if (c?.command === "link" || c?.command === "pair") {
        linkUntil = Date.now() + 5 * 60_000;
        pairPhone = c.command === "pair" ? String(c.command_arg || "").replace(/\D/g, "") || null : null;
        if (pairPhone && pairPhone.length === 10) pairPhone = `91${pairPhone}`;
        if (sock && !connected) {
          sock.end(undefined);
          sock = null;
        }
        await setState({ pairingCode: null, qrSvg: null, error: null });
        await start();
      } else if (c?.command === "logout") {
        if (sock) await sock.logout().catch(() => {});
        sock = null;
        connected = false;
        await clearAuth();
        await setState({ status: "not_linked", me: null, qrSvg: null, pairingCode: null, error: null });
      }
      // Linking window over and still not linked: stop generating QRs.
      if (!connected && sock && linkUntil && Date.now() > linkUntil && !(await isRegistered())) {
        sock.end(undefined);
        sock = null;
        linkUntil = 0;
        await setState({ status: "not_linked", qrSvg: null, pairingCode: null });
      }
      if (!sock && !starting && (await isRegistered())) await start();
    } catch (e) {
      log("[cmd]", e.message);
    }
    await sleep(3000);
  }
}

/* ---------------- keep-awake ---------------- */

/**
 * Render's free plan sleeps after 15 minutes without visits. Supabase pg_cron pings /health every 10 minutes;
 * Render tells each service its public address (RENDER_EXTERNAL_URL), so the gateway points that job at itself
 * on every boot.
 */
async function registerKeepalive() {
  const url = (process.env.RENDER_EXTERNAL_URL || "").replace(/\/$/, "");
  if (!/^https:\/\/[a-z0-9.-]+$/i.test(url)) return;
  try {
    await pool.query("select cron.schedule('epos-wa-gateway-keepalive', '*/10 * * * *', $1)", [`select net.http_get('${url}/health')`]);
    log("[keepalive] pinging", url);
  } catch (e) {
    log("[keepalive] could not register:", e.message);
  }
}

/* ---------------- boot ---------------- */

let lockClient = null;

async function acquireLock() {
  for (;;) {
    try {
      lockClient = await pool.connect();
      const { rows } = await lockClient.query("select pg_try_advisory_lock($1) as ok", [LOCK_ID]);
      if (rows[0].ok) return;
      lockClient.release();
      lockClient = null;
      log("[lock] another gateway is running; standing by");
    } catch (e) {
      log("[lock]", e.message);
      lockClient?.release();
      lockClient = null;
    }
    await sleep(15_000);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, active: Boolean(lockClient), connected }));
    return;
  }
  res.writeHead(404).end();
});
server.listen(Number(process.env.PORT) || 3000, () => log("[http] listening on", Number(process.env.PORT) || 3000));

(async () => {
  await acquireLock();
  log("[lock] acquired; this copy is active · schema", SCHEMA);
  setInterval(() => pool.query(`update "WaState" set "heartbeatAt" = now() where id = 1`).catch(() => {}), 30_000);
  await pool.query(`insert into "WaState" (id) values (1) on conflict (id) do nothing`).catch(() => {});
  await pool.query(`update "WaState" set "heartbeatAt" = now() where id = 1`);
  await registerKeepalive();
  start().catch((e) => log("[start]", e.message));
  commands();
  work();
})();

async function shutdown() {
  log("[exit] shutting down");
  try {
    sock?.end(undefined);
    await setState({ status: "offline" });
    await lockClient?.query("select pg_advisory_unlock($1)", [LOCK_ID]);
    lockClient?.release();
    await pool.end();
  } catch {}
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
