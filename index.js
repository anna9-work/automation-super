import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/**
 * =========================================================
 *  LINE Bot for Inventory（高頻穩定版 v2）
 *
 *  ✅ webhook 立刻回 200
 *  ✅ Reply 優先；Reply 失敗必定 Push fallback
 *  ✅ 只對「出庫」加鎖（避免高頻查詢被鎖排隊）
 *  ✅ Supabase fetch timeout（避免 RPC 卡住）
 *  ✅ 加入 uptime / eid / stage log（抓出卡點）
 *
 *  功能：
 *  - 查詢：只在「當日有庫存」清單內做關鍵字比對
 *  - 快照：public.get_business_day_stock
 *  - 出庫：fifo_out_and_log
 *  - 支援 message + postback
 *  - db 指令：回覆 bot 版本、db host、biz_date(05:00切日)、uptime
 * =========================================================
 */

/* ======== Environment ======== */
const {
  PORT = 3000,
  LINE_CHANNEL_ACCESS_TOKEN,
  LINE_CHANNEL_SECRET,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  DEFAULT_GROUP = 'default',
  GAS_WEBHOOK_URL: ENV_GAS_URL,
  GAS_WEBHOOK_SECRET: ENV_GAS_SECRET,
} = process.env;

if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) console.error('缺少 LINE 環境變數');
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) console.error('缺少 Supabase 環境變數 (URL / SERVICE_ROLE_KEY)');

const BOT_VER = 'V2026-01-14_REPLY_FIRST_OUT_LOCK_TRACE';
const STARTED_AT_MS = Date.now();

/* ======== App / LINE / Supabase ======== */
const app = express(); // ⚠️ webhook 前不可掛 body parser

app.use((req, _res, next) => {
  const up = process.uptime().toFixed(1);
  console.log(
    `[請求] ${req.method} ${req.path} up=${up}s ua=${req.headers['user-agent'] || ''} x-line-signature=${
      req.headers['x-line-signature'] ? 'yes' : 'no'
    }`,
  );
  next();
});

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });

function getSupabaseHost() {
  try {
    const u = new URL(SUPABASE_URL);
    return u.host;
  } catch {
    return String(SUPABASE_URL || '');
  }
}
const SUPA_HOST = getSupabaseHost();

/** ✅ Supabase 全域 timeout fetch（避免 RPC 卡住） */
const SUPA_TIMEOUT_MS = 8000;
function fetchWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), SUPA_TIMEOUT_MS);
  return fetch(url, { ...options, signal: controller.signal }).finally(() => clearTimeout(id));
}

const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
  global: { fetch: fetchWithTimeout },
});

/* ======== Runtime caches ======== */
const LAST_WAREHOUSE_CODE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> warehouse_code
const LAST_SKU_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> sku(lower)

const WH_LABEL_CACHE = new Map(); // key -> { ts, val }
const WH_CODE_CACHE = new Map(); // key -> { ts, val }
const WH_CACHE_TTL_MS = 60 * 60 * 1000; // 1h

const STOCK_LIST_CACHE = new Map(); // key=`${branch}::${bizDate}` -> { ts, rows }
const STOCK_LIST_TTL_MS = 60 * 1000; // 60s

const BRANCH_ROLE_CACHE = new Map(); // key=`${src.type}::${groupId||userId}` -> { ts, val }
const BRANCH_ROLE_TTL_MS = 30 * 1000; // 30s

const EVENT_DEDUP = new Map(); // key -> ts
const EVENT_DEDUP_TTL_MS = 3 * 60 * 1000; // 3m

/** ✅ 只給出庫使用的鎖：同群組同使用者 */
const OUT_LOCKS = new Map(); // key -> Promise chain

/* ======== Fixed warehouse labels (code -> 中文) ======== */
const FIX_CODE_TO_NAME = new Map([
  ['main', '總倉'],
  ['main_warehouse', '總倉'],
  ['prize', '代夾物'],
  ['swap', '夾換品'],
  ['unspecified', '未指定'],
  ['withdraw', '撤台'],
]);

/* ======== Helpers ======== */
const skuKey = (s) => String(s || '').trim().toLowerCase();
const skuDisplay = (s) => String(s || '').trim();

function pickNum(val, fallback = 0) {
  const n = Number(val);
  return Number.isFinite(n) ? n : fallback;
}

/* ✅ 05:00 切日：biz_date = (台北現在時間 - 5hr) 的日期 */
function getBizDate0500TPE() {
  const d = new Date(Date.now() - 5 * 60 * 60 * 1000);
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d); // yyyy-mm-dd
}

function tpeNowISO() {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(new Date());
  return s.replace(' ', 'T') + '+08:00';
}

function nowMs() {
  return Date.now();
}

function cacheGet(map, key, ttlMs) {
  const it = map.get(key);
  if (!it) return null;
  if (nowMs() - it.ts > ttlMs) {
    map.delete(key);
    return null;
  }
  return it.val;
}
function cacheSet(map, key, val) {
  map.set(key, { ts: nowMs(), val });
}

function pruneDedup() {
  const t = nowMs();
  for (const [k, ts] of EVENT_DEDUP.entries()) {
    if (t - ts > EVENT_DEDUP_TTL_MS) EVENT_DEDUP.delete(k);
  }
}
function isDupEvent(key) {
  pruneDedup();
  if (!key) return false;
  if (EVENT_DEDUP.has(key)) return true;
  EVENT_DEDUP.set(key, nowMs());
  return false;
}

/** ✅ 只給出庫用：串行鎖 */
async function withOutLock(key, fn) {
  const prev = OUT_LOCKS.get(key) || Promise.resolve();
  let resolveNext;
  const next = new Promise((r) => (resolveNext = r));
  OUT_LOCKS.set(key, prev.then(() => next).catch(() => next));

  await prev;
  try {
    return await fn();
  } finally {
    resolveNext();
    setTimeout(() => {
      if (OUT_LOCKS.get(key) === next) OUT_LOCKS.delete(key);
    }, 1000).unref?.();
  }
}

/* ======== Reply-first sender ======== */
function getPushTarget(event) {
  const src = event?.source || {};
  if (src.type === 'group' && src.groupId) return { type: 'group', id: src.groupId };
  if (src.type === 'room' && src.roomId) return { type: 'room', id: src.roomId };
  if (src.userId) return { type: 'user', id: src.userId };
  return null;
}

async function sendMessage(event, msg, meta = {}) {
  const up = process.uptime().toFixed(1);
  const eid = meta.eid || '-';

  // 1) reply
  const token = event?.replyToken;
  if (token) {
    const t0 = Date.now();
    try {
      await client.replyMessage(token, msg);
      console.log(`[LINE REPLY] ok eid=${eid} ms=${Date.now() - t0} up=${up}s`);
      return;
    } catch (e) {
      console.warn(
        `[LINE REPLY] fail eid=${eid} ms=${Date.now() - t0} up=${up}s msg=${e?.message || e} code=${e?.statusCode || '-'} details=${JSON.stringify(
          e?.originalError?.response?.data || e?.response?.data || {},
        )}`,
      );
      // fallback push
    }
  }

  // 2) push
  const target = getPushTarget(event);
  if (!target) return;

  const t0 = Date.now();
  try {
    await client.pushMessage(target.id, msg);
    console.log(`[LINE PUSH] ok eid=${eid} ms=${Date.now() - t0} up=${up}s to=${target.type}:${target.id.slice(0, 6)}...`);
  } catch (e) {
    console.error(
      `[LINE PUSH] fail eid=${eid} ms=${Date.now() - t0} up=${up}s msg=${e?.message || e} code=${e?.statusCode || '-'} details=${JSON.stringify(
        e?.originalError?.response?.data || e?.response?.data || {},
      )}`,
    );
  }
}

async function sendText(event, text, meta = {}) {
  const msg = { type: 'text', text: String(text || '') };
  console.log(`[SEND] eid=${meta.eid || '-'} type=text len=${msg.text.length}`);
  return sendMessage(event, msg, meta);
}

async function sendTextWithQuickReply(event, text, quickReply, meta = {}) {
  const msg = { type: 'text', text: String(text || ''), quickReply };
  console.log(`[SEND] eid=${meta.eid || '-'} type=quickReply items=${quickReply?.items?.length || 0}`);
  return sendMessage(event, msg, meta);
}

/* ======== Warehouse resolvers ======== */
async function resolveWarehouseLabel(codeOrName) {
  const key = String(codeOrName || '').trim();
  if (!key) return '未指定';

  const cached = cacheGet(WH_LABEL_CACHE, key, WH_CACHE_TTL_MS);
  if (cached) return cached;

  if (FIX_CODE_TO_NAME.has(key)) {
    const name = FIX_CODE_TO_NAME.get(key);
    cacheSet(WH_LABEL_CACHE, key, name);
    cacheSet(WH_CODE_CACHE, name, key);
    return name;
  }

  try {
    const { data } = await supabase
      .from('warehouse_kinds')
      .select('kind_id, kind_name')
      .or(`kind_id.eq.${key},kind_name.eq.${key}`)
      .limit(1)
      .maybeSingle();
    if (data?.kind_name) {
      cacheSet(WH_LABEL_CACHE, key, data.kind_name);
      cacheSet(WH_LABEL_CACHE, data.kind_id, data.kind_name);
      cacheSet(WH_CODE_CACHE, data.kind_name, data.kind_id);
      return data.kind_name;
    }
  } catch {}

  cacheSet(WH_LABEL_CACHE, key, key);
  return key;
}

async function getWarehouseCodeForLabel(displayNameOrCode) {
  const label = String(displayNameOrCode || '').trim();
  if (!label) return 'unspecified';

  if (/^[a-z0-9_]+$/i.test(label)) {
    if (FIX_CODE_TO_NAME.has(label)) return label;

    const cached = cacheGet(WH_CODE_CACHE, label, WH_CACHE_TTL_MS);
    if (cached) return cached;

    try {
      const { data } = await supabase
        .from('warehouse_kinds')
        .select('kind_id, kind_name')
        .or(`kind_id.eq.${label},kind_name.eq.${label}`)
        .limit(1)
        .maybeSingle();
      if (data?.kind_id) {
        cacheSet(WH_CODE_CACHE, data.kind_name, data.kind_id);
        cacheSet(WH_LABEL_CACHE, data.kind_id, data.kind_name);
        return data.kind_id;
      }
    } catch {}
    return label.toLowerCase();
  }

  const cachedZh = cacheGet(WH_CODE_CACHE, label, WH_CACHE_TTL_MS);
  if (cachedZh) return cachedZh;

  for (const [code, name] of FIX_CODE_TO_NAME.entries()) {
    if (name === label) {
      cacheSet(WH_CODE_CACHE, name, code);
      return code;
    }
  }

  try {
    const { data } = await supabase
      .from('warehouse_kinds')
      .select('kind_id, kind_name')
      .or(`kind_name.eq.${label},kind_id.eq.${label}`)
      .limit(1)
      .maybeSingle();
    if (data?.kind_id) {
      cacheSet(WH_CODE_CACHE, data.kind_name, data.kind_id);
      cacheSet(WH_LABEL_CACHE, data.kind_id, data.kind_name);
      return data.kind_id;
    }
  } catch {}

  return 'unspecified';
}

/* ======== Branch & User ======== */
async function resolveAuthUuidFromLineUserId(lineUserId) {
  if (!lineUserId) return null;
  const { data, error } = await supabase
    .from('line_user_map')
    .select('auth_user_id')
    .eq('line_user_id', lineUserId)
    .maybeSingle();
  if (error) return null;
  return data?.auth_user_id || null;
}

async function resolveBranchAndRole(event) {
  const src = event.source || {};
  const userId = src.userId || null;
  const isGroup = src.type === 'group';

  const cacheKey = `${src.type}::${isGroup ? src.groupId : userId || ''}`;
  const cached = cacheGet(BRANCH_ROLE_CACHE, cacheKey, BRANCH_ROLE_TTL_MS);
  if (cached) return cached;

  let role = 'user',
    blocked = false;

  if (userId) {
    const { data: u } = await supabase
      .from('users')
      .select('角色, 黑名單, 群組')
      .eq('user_id', userId)
      .maybeSingle();
    role = u?.角色 || 'user';
    blocked = !!u?.黑名單;
  }

  let out;
  if (isGroup) {
    const { data: lg } = await supabase
      .from('line_groups')
      .select('群組')
      .eq('line_group_id', src.groupId)
      .maybeSingle();
    out = { branch: lg?.群組 || null, role, blocked, needBindMsg: '此群組尚未綁定分店，請管理員設定' };
  } else {
    const { data: u2 } = await supabase
      .from('users')
      .select('群組')
      .eq('user_id', userId)
      .maybeSingle();
    out = { branch: u2?.群組 || null, role, blocked, needBindMsg: '此使用者尚未綁定分店，請管理員設定' };
  }

  cacheSet(BRANCH_ROLE_CACHE, cacheKey, out);
  return out;
}

async function autoRegisterUser(lineUserId) {
  if (!lineUserId) return;
  const { data } = await supabase.from('users').select('user_id').eq('user_id', lineUserId).maybeSingle();
  if (!data)
    await supabase.from('users').insert({ user_id: lineUserId, 群組: DEFAULT_GROUP, 角色: 'user', 黑名單: false });
}

/* ======== Stock RPCs ======== */
async function getWarehouseStockBySku(branch, sku, meta = {}) {
  const group = String(branch || '').trim().toLowerCase();
  const s = skuKey(sku);
  if (!group || !s) return [];

  const bizDate = getBizDate0500TPE();
  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER} eid=${meta.eid || '-'}`);
  console.log(`[庫存 RPC] eid=${meta.eid || '-'} group=${group} bizDate=${bizDate} sku=${s} stage=before`);

  const t0 = Date.now();
  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: null,
  });

  if (error) {
    console.log(`[庫存 RPC] eid=${meta.eid || '-'} stage=error ms=${Date.now() - t0} msg=${error.message}`);
    throw error;
  }

  const rows = Array.isArray(data) ? data : [];
  const keptRaw = rows
    .map((r) => {
      const whCode = String(r.warehouse_code || '').trim() || 'unspecified';
      const box = pickNum(r.box ?? r['庫存箱數'] ?? 0, 0);
      const piece = pickNum(r.piece ?? r['庫存散數'] ?? 0, 0);
      const unitsPerBox = pickNum(r.units_per_box ?? r['箱入數'] ?? 1, 1);
      const unitPricePiece = pickNum(r.unit_price_piece ?? r['單價'] ?? 0, 0);
      return { warehouseCode: whCode, box, piece, unitsPerBox, unitPricePiece };
    })
    .filter((w) => w.box > 0 || w.piece > 0);

  const kept = await Promise.all(
    keptRaw.map(async (w) => ({
      ...w,
      warehouseLabel: await resolveWarehouseLabel(w.warehouseCode),
    })),
  );

  console.log(
    `[庫存 RPC] eid=${meta.eid || '-'} stage=after ms=${Date.now() - t0} rows=${rows.length} kept=${kept.length} wh=${kept
      .map((x) => `${x.warehouseCode}:${x.box}/${x.piece}`)
      .join(',')}`,
  );

  return kept;
}

async function getWarehouseSnapshot(branch, sku, warehouseCodeOrLabel, meta = {}) {
  const group = String(branch || '').trim().toLowerCase();
  const s = skuKey(sku);
  const whCode = await getWarehouseCodeForLabel(warehouseCodeOrLabel || 'unspecified');
  const bizDate = getBizDate0500TPE();

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER} eid=${meta.eid || '-'}`);

  const t0 = Date.now();
  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: whCode,
  });
  if (error) throw error;

  const row = Array.isArray(data) ? data[0] : data;

  if (!row) {
    return {
      warehouseCode: whCode,
      warehouseLabel: await resolveWarehouseLabel(whCode),
      box: 0,
      piece: 0,
      unitsPerBox: 1,
      unitPricePiece: 0,
      stockAmount: 0,
      _ms: Date.now() - t0,
    };
  }

  const box = pickNum(row.box ?? row['庫存箱數'] ?? 0, 0);
  const piece = pickNum(row.piece ?? row['庫存散數'] ?? 0, 0);
  const unitsPerBox = pickNum(row.units_per_box ?? row['箱入數'] ?? 1, 1);
  const unitPricePiece = pickNum(row.unit_price_piece ?? row['單價'] ?? 0, 0);
  const stockAmount = (box * unitsPerBox + piece) * unitPricePiece;

  return {
    warehouseCode: whCode,
    warehouseLabel: await resolveWarehouseLabel(whCode),
    box,
    piece,
    unitsPerBox,
    unitPricePiece,
    stockAmount,
    _ms: Date.now() - t0,
  };
}

/* ======== Today Stock list cache ======== */
async function getTodayStockRows(branch, meta = {}) {
  const group = String(branch || '').trim().toLowerCase();
  if (!group) return [];

  const bizDate = getBizDate0500TPE();
  const key = `${group}::${bizDate}`;

  const cached = STOCK_LIST_CACHE.get(key);
  if (cached && Date.now() - cached.ts < STOCK_LIST_TTL_MS) {
    console.log(`[STOCK LIST] eid=${meta.eid || '-'} cache hit kept=${cached.rows.length}`);
    return cached.rows;
  }

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER} eid=${meta.eid || '-'}`);
  const t0 = Date.now();
  const { data, error } = await supabase.rpc('daily_sheet_rows_full', {
    p_biz_date: bizDate,
    p_group: group,
  });
  if (error) {
    console.log(`[STOCK LIST] eid=${meta.eid || '-'} rpc error ms=${Date.now() - t0} msg=${error.message}`);
    throw error;
  }

  const rows = Array.isArray(data) ? data : [];
  const kept = rows.filter((r) => pickNum(r['庫存箱數'] ?? 0, 0) > 0 || pickNum(r['庫存散數'] ?? 0, 0) > 0);

  STOCK_LIST_CACHE.set(key, { ts: Date.now(), rows: kept });
  console.log(`[STOCK LIST] eid=${meta.eid || '-'} rpc ok ms=${Date.now() - t0} rows=${rows.length} kept=${kept.length}`);
  return kept;
}

async function searchByNameInStock(keyword, branch, meta = {}) {
  const k = String(keyword || '').trim();
  if (!k) return [];
  const rows = await getTodayStockRows(branch, meta);

  const seen = new Set();
  const out = [];

  for (const r of rows) {
    const sku = skuKey(r.product_sku || r['貨品編號']);
    if (!sku || seen.has(sku)) continue;

    const name = String(r['貨品名稱'] || '').trim();
    if (name.toLowerCase().includes(k.toLowerCase())) {
      seen.add(sku);
      out.push({
        sku,
        name,
        unitsPerBox: pickNum(r['箱入數'] ?? 1, 1),
        price: pickNum(r['單價'] ?? 0, 0),
      });
      if (out.length >= 10) break;
    }
  }
  return out;
}

async function searchBySkuInStock(skuInput, branch, meta = {}) {
  const s = skuKey(skuInput);
  if (!s) return [];
  const rows = await getTodayStockRows(branch, meta);

  const exact = rows.find((r) => skuKey(r.product_sku || r['貨品編號']) === s);
  if (exact) {
    return [
      {
        sku: s,
        name: String(exact['貨品名稱'] || s).trim(),
        unitsPerBox: pickNum(exact['箱入數'] ?? 1, 1),
        price: pickNum(exact['單價'] ?? 0, 0),
      },
    ];
  }

  const out = [];
  const seen = new Set();
  for (const r of rows) {
    const sku = skuKey(r.product_sku || r['貨品編號']);
    if (!sku || seen.has(sku)) continue;
    if (sku.includes(s)) {
      seen.add(sku);
      out.push({
        sku,
        name: String(r['貨品名稱'] || sku).trim(),
        unitsPerBox: pickNum(r['箱入數'] ?? 1, 1),
        price: pickNum(r['單價'] ?? 0, 0),
      });
      if (out.length >= 10) break;
    }
  }
  return out;
}

/* ======== Quick Replies ======== */
function buildQuickReplyForProducts(items) {
  const actions = items.slice(0, 12).map((p) => ({
    type: 'action',
    action: { type: 'message', label: `${p.name}`.slice(0, 20), text: `編號 ${p.sku}` },
  }));
  return { items: actions };
}
function buildQuickReplyForWarehousesForQuery(warehouseList) {
  const items = warehouseList.slice(0, 12).map((w) => ({
    type: 'action',
    action: {
      type: 'message',
      label: `${w.warehouseLabel}（${w.box}箱/${w.piece}件）`.slice(0, 20),
      text: `倉 ${w.warehouseLabel}`,
    },
  }));
  return { items };
}
function buildQuickReplyForWarehouses(baseText, warehouseList, wantBox, wantPiece) {
  const items = warehouseList.slice(0, 12).map((w) => {
    const label = `${w.warehouseLabel}（${w.box}箱/${w.piece}散）`.slice(0, 20);
    const text = `${baseText} ${wantBox > 0 ? `${wantBox}箱 ` : ''}${wantPiece > 0 ? `${wantPiece}件 ` : ''}@${w.warehouseLabel}`.trim();
    return { type: 'action', action: { type: 'message', label, text } };
  });
  return { items };
}

/* ======== Command parser ======== */
function parseCommand(text) {
  const t = (text || '').trim();
  if (!t) return null;

  if (/^(db|DB|版本)$/.test(t)) return { type: 'db' };
  if (!/^(查|查詢|條碼|編號|#|入庫|入|出庫|出|倉)/.test(t)) return null;

  const mWhSel = t.match(/^倉(?:庫)?\s*(.+)$/);
  if (mWhSel) return { type: 'wh_select', warehouse: mWhSel[1].trim() };

  const mSkuHash = t.match(/^#\s*(.+)$/);
  if (mSkuHash) return { type: 'sku', sku: mSkuHash[1].trim() };

  const mSku = t.match(/^編號[:：]?\s*(.+)$/);
  if (mSku) return { type: 'sku', sku: mSku[1].trim() };

  const mQuery = t.match(/^查(?:詢)?\s*(.+)$/);
  if (mQuery) return { type: 'query', keyword: mQuery[1].trim() };

  const mChange = t.match(
    /^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散|件))?\s*(?:(\d+))?(?:\s*(?:@|（?\(?倉庫[:：=]\s*)([^)）]+)\)?)?\s*$/,
  );
  if (mChange) {
    const box = mChange[2] ? parseInt(mChange[2], 10) : 0;
    const pieceLabeled = mChange[3] ? parseInt(mChange[3], 10) : 0;
    const pieceTail = mChange[4] ? parseInt(mChange[4], 10) : 0;

    const rawHasDigit = /\d+/.test(t);
    const hasBoxOrPieceUnit = /箱|個|散|件/.test(t);
    const piece =
      pieceLabeled ||
      pieceTail ||
      (!hasBoxOrPieceUnit && rawHasDigit && box === 0 ? parseInt(t.replace(/[^\d]/g, ''), 10) || 0 : 0);

    const warehouse = (mChange[5] || '').trim();

    return {
      type: 'change',
      action: /入/.test(mChange[1]) ? 'in' : 'out',
      box,
      piece,
      warehouse: warehouse || null,
    };
  }

  return null;
}

function parsePostback(data) {
  const s = String(data || '').trim();
  if (!s) return null;
  const params = new URLSearchParams(s);
  const a = params.get('a');
  if (a === 'wh_select') {
    return { type: 'wh_select_postback', sku: skuKey(params.get('sku')), wh: params.get('wh') };
  }
  return null;
}

/* ======== 出庫（RPC：fifo_out_and_log） ======== */
async function callOutOnceTx({ branch, sku, outBox, outPiece, warehouseCode, lineUserId, meta = {} }) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error(`找不到對應的使用者，請先在後台綁定帳號。`);

  const args = {
    p_group: String(branch || '').trim().toLowerCase(),
    p_sku: skuKey(sku),
    p_warehouse_name: String(warehouseCode || 'unspecified').trim(),
    p_out_box: String(outBox ?? ''),
    p_out_piece: String(outPiece ?? ''),
    p_user_id: authUuid,
    p_source: 'LINE',
    p_at: new Date().toISOString(),
  };

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER} eid=${meta.eid || '-'}`);
  const t0 = Date.now();
  const { data, error } = await supabase.rpc('fifo_out_and_log', args);
  if (error) {
    console.log(`[OUT RPC] eid=${meta.eid || '-'} error ms=${Date.now() - t0} msg=${error.message}`);
    throw error;
  }

  const row = Array.isArray(data) ? data[0] : data;
  console.log(`[OUT RPC] eid=${meta.eid || '-'} ok ms=${Date.now() - t0}`);
  return {
    productName: row?.product_name || sku,
    unitsPerBox: Number(row?.units_per_box || 1) || 1,
    unitPricePiece: Number(row?.unit_price_piece || 0),
    outBox: Number(row?.out_box || outBox || 0),
    outPiece: Number(row?.out_piece || outPiece || 0),
    warehouseCode: String(warehouseCode || 'unspecified'),
  };
}

/* ======== GAS (optional, fire-and-forget) ======== */
let GAS_URL_CACHE = (ENV_GAS_URL || '').trim();
let GAS_SECRET_CACHE = (ENV_GAS_SECRET || '').trim();
let GAS_LOADED_ONCE = false;
let GAS_LAST_LOAD_MS = 0;

async function loadGasConfigFromDBIfNeeded(force = false) {
  const now = Date.now();
  const hasCache = GAS_URL_CACHE && GAS_SECRET_CACHE;
  if (!force && hasCache && GAS_LOADED_ONCE && now - GAS_LAST_LOAD_MS < 5 * 60 * 1000) return;

  try {
    const { data, error } = await supabase.rpc('get_app_settings', {
      keys: ['gas_webhook_url', 'gas_webhook_secret'],
    });
    if (error) throw error;
    if (Array.isArray(data)) {
      for (const row of data) {
        const k = String(row.key || '').trim();
        const v = String(row.value || '').trim();
        if (k === 'gas_webhook_url' && v) GAS_URL_CACHE = v;
        if (k === 'gas_webhook_secret' && v) GAS_SECRET_CACHE = v;
      }
    }
    GAS_LOADED_ONCE = true;
    GAS_LAST_LOAD_MS = now;
  } catch (e) {
    GAS_LOADED_ONCE = true;
    GAS_LAST_LOAD_MS = now;
    console.warn('⚠️ 載入 GAS 設定失敗：', e?.message || e);
  }
}
async function getGasConfig() {
  if (!GAS_LOADED_ONCE || !GAS_URL_CACHE || !GAS_SECRET_CACHE) await loadGasConfigFromDBIfNeeded(true);
  return { url: GAS_URL_CACHE, secret: GAS_SECRET_CACHE };
}
async function postInventoryToGAS(payload) {
  const { url, secret } = await getGasConfig();
  if (!url || !secret) return;

  const cleanBaseUrl = url.replace(/\?.*$/, '');
  const callUrl = `${cleanBaseUrl}?secret=${encodeURIComponent(secret)}`;
  try {
    const res = await fetch(callUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      console.warn('[GAS WARN]', res.status, txt);
    }
  } catch (e) {
    console.warn('[GAS ERROR]', e);
  }
}

/* ======== Last SKU helpers ======== */
function setLastSku(lineUserId, branch, sku) {
  if (!lineUserId) return;
  LAST_SKU_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, skuKey(sku));
}
function getLastSku(lineUserId, branch) {
  if (!lineUserId) return null;
  const mem = LAST_SKU_BY_USER_BRANCH.get(`${lineUserId}::${branch}`);
  return mem ? skuKey(mem) : null;
}

/* ======== Log / dedup / eid ======== */
function logEventSummary(event) {
  try {
    const src = event?.source || {};
    const msg = event?.message || {};
    const isGroup = src.type === 'group';
    console.log(
      `[LINE EVENT] type=${event?.type} msgType=${msg?.type || '-'} source=${src.type || '-'} groupId=${isGroup ? src.groupId : '-'} userId=${src.userId || '-'} text="${msg?.type === 'text' ? msg.text : ''}"`,
    );
  } catch {}
}

function buildEventDedupKey(ev) {
  try {
    const src = ev?.source || {};
    const msg = ev?.message || {};
    const who = src.type === 'group' ? src.groupId : src.userId;
    if (ev?.type === 'message' && msg?.id) return `m:${msg.id}`;
    if (ev?.type === 'postback') return `p:${ev?.timestamp || 0}:${who || ''}:${ev?.postback?.data || ''}`;
    return `x:${ev?.timestamp || 0}:${src.type}:${who || ''}:${ev?.replyToken || ''}:${ev?.type || ''}`;
  } catch {
    return null;
  }
}

function makeEid(ev) {
  const ts = String(ev?.timestamp || Date.now());
  const tail = ts.length > 6 ? ts.slice(-6) : ts;
  const src = ev?.source || {};
  const who = src.type === 'group' ? src.groupId : src.userId;
  const w = who ? String(who).slice(0, 6) : 'noid';
  return `${tail}-${w}`;
}

/** 只出庫鎖：同群組同使用者 */
function buildOutLockKey(ev) {
  const src = ev?.source || {};
  const gid = src.type === 'group' ? src.groupId : '';
  const uid = src.userId || '';
  if (gid && uid) return `g:${gid}::u:${uid}`;
  if (uid) return `u:${uid}`;
  if (gid) return `g:${gid}`;
  return 'unknown';
}

/* ======== Server endpoints ======== */
const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));
app.get('/webhook', (_req, res) => res.status(200).send('OK'));
app.post('/webhook', line.middleware(lineConfig), lineHandler);

app.use((err, req, res, next) => {
  if (req.path === '/webhook') {
    console.error('[LINE MIDDLEWARE ERROR]', err?.message || err);
    return res.status(400).end();
  }
  return next(err);
});

/* ======== Main Handler ======== */
async function lineHandler(req, res) {
  try {
    const events = req.body?.events || [];
    res.status(200).send('OK');

    setImmediate(async () => {
      for (const ev of events) {
        const eid = makeEid(ev);
        logEventSummary(ev);

        const dedupKey = buildEventDedupKey(ev);
        if (isDupEvent(dedupKey)) {
          console.log(`[DEDUP] skip eid=${eid} key=${dedupKey}`);
          continue;
        }

        try {
          await handleEvent(ev, { eid });
        } catch (err) {
          console.error(`[HANDLE EVENT ERROR] eid=${eid}`, err?.message || err);
          const m = String(err?.name || '').includes('Abort')
            ? '系統忙碌（查詢逾時），請再試一次'
            : `系統忙碌或發生錯誤：${err?.message || '未知錯誤'}`;
          await sendText(ev, m, { eid });
        }
      }
    });
  } catch (e) {
    console.error('[WEBHOOK ERROR]', e);
    try {
      return res.status(200).send('OK');
    } catch {}
  }
}

/* ======== Event logic ======== */
async function handleEvent(event, meta = {}) {
  const { eid } = meta;
  const source = event.source || {};
  const isGroup = source.type === 'group';
  const lineUserId = source.userId || null;

  if (!isGroup && lineUserId) await autoRegisterUser(lineUserId);

  const { branch, role, blocked, needBindMsg } = await resolveBranchAndRole(event);
  if (blocked) return;

  if (!branch) {
    await sendText(event, needBindMsg || '尚未綁定分店', { eid });
    return;
  }

  // postback（點倉庫）：不鎖（只是查快照 + 記 lastWh）
  if (event.type === 'postback') {
    const pb = parsePostback(event?.postback?.data);
    if (!pb) return;

    if (pb.type === 'wh_select_postback') {
      const sku = pb.sku || getLastSku(lineUserId, branch);
      if (!sku) return sendText(event, '請先選商品（查/編號）再選倉庫', { eid });

      const whCode = await getWarehouseCodeForLabel(pb.wh);
      LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whCode);

      const snap = await getWarehouseSnapshot(branch, sku, whCode, { eid });
      await sendText(event, `編號：${skuDisplay(sku)}\n倉庫類別：${snap.warehouseLabel}\n庫存：${snap.box}箱${snap.piece}散`, { eid });
      return;
    }
    return;
  }

  if (event.type !== 'message' || event.message.type !== 'text') return;

  const text = event.message.text || '';
  const parsed = parseCommand(text);
  if (!parsed) return;

  if (parsed.type === 'db') {
    const bizDate = getBizDate0500TPE();
    const up = process.uptime().toFixed(1);
    const startedAgo = ((Date.now() - STARTED_AT_MS) / 1000).toFixed(1);
    await sendText(
      event,
      `BOT=${BOT_VER}\nDB_HOST=${SUPA_HOST}\nBIZ_DATE_0500=${bizDate}\nSUPA_TIMEOUT_MS=${SUPA_TIMEOUT_MS}\nUPTIME=${up}s\nSTARTED_AGO=${startedAgo}s`,
      { eid },
    );
    return;
  }

  if (parsed.type === 'wh_select') {
    const sku = getLastSku(lineUserId, branch);
    if (!sku) return sendText(event, '請先選商品（查/編號）再選倉庫', { eid });

    const whCode = await getWarehouseCodeForLabel(parsed.warehouse);
    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whCode);

    const snap = await getWarehouseSnapshot(branch, sku, whCode, { eid });
    await sendText(event, `編號：${skuDisplay(sku)}\n倉庫類別：${snap.warehouseLabel}\n庫存：${snap.box}箱${snap.piece}散`, { eid });
    return;
  }

  const doQueryCommon = async (p) => {
    const sku = skuKey(p.sku);
    const whList = await getWarehouseStockBySku(branch, sku, { eid });
    if (!whList.length) return sendText(event, '無此商品庫存', { eid });

    setLastSku(lineUserId, branch, sku);

    if (whList.length >= 2) {
      await sendTextWithQuickReply(event, `名稱：${p.name}\n編號：${skuDisplay(sku)}\n👉請選擇倉庫`, buildQuickReplyForWarehousesForQuery(whList), { eid });
      return;
    }

    const chosen = whList[0];
    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, chosen.warehouseCode);

    await sendText(
      event,
      `名稱：${p.name}\n編號：${skuDisplay(sku)}\n箱入數：${p.unitsPerBox}\n單價：${p.price}\n倉庫類別：${chosen.warehouseLabel}\n庫存：${chosen.box}箱${chosen.piece}散`,
      { eid },
    );
  };

  // 查 關鍵字：不鎖（可高頻）
  if (parsed.type === 'query') {
    const list = await searchByNameInStock(parsed.keyword, branch, { eid });
    if (!list.length) return sendText(event, '無此商品庫存', { eid });

    if (list.length > 1) {
      await sendTextWithQuickReply(event, `找到以下與「${parsed.keyword}」相關的庫存品項`, buildQuickReplyForProducts(list), { eid });
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }

  // 編號 / #：不鎖（可高頻）
  if (parsed.type === 'sku') {
    const list = await searchBySkuInStock(parsed.sku, branch, { eid });
    if (!list.length) return sendText(event, '無此商品庫存', { eid });

    if (list.length > 1) {
      await sendTextWithQuickReply(event, `找到以下與「${parsed.sku}」相關的庫存品項`, buildQuickReplyForProducts(list), { eid });
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }

  // 出庫：✅ 只有這段加鎖（避免重扣）
  if (parsed.type === 'change') {
    if (parsed.action === 'in') {
      if (role !== '主管') return sendText(event, '您無法使用「入庫」', { eid });
      return sendText(event, '入庫請改用 App 進行；LINE 僅提供出庫', { eid });
    }

    const outLockKey = buildOutLockKey(event);
    await withOutLock(outLockKey, async () => {
      const outBox = parsed.box || 0;
      const outPiece = parsed.piece || 0;
      if (outBox === 0 && outPiece === 0) return;

      const skuLast = getLastSku(lineUserId, branch);
      if (!skuLast) return sendText(event, '請先用「查 商品」或「編號」選定「有庫存」商品後再出庫。', { eid });

      const whList = await getWarehouseStockBySku(branch, skuLast, { eid });
      if (!whList.length) return sendText(event, '所有倉庫皆無庫存，無法出庫。', { eid });

      const lastWhKey = `${lineUserId || ''}::${branch}`;
      const lastWhCode = LAST_WAREHOUSE_CODE_BY_USER_BRANCH.get(lastWhKey) || null;

      let chosenWhCode = null;

      if (parsed.warehouse) {
        chosenWhCode = await getWarehouseCodeForLabel(parsed.warehouse);
      } else if (lastWhCode) {
        const matched = whList.find((w) => w.warehouseCode === lastWhCode);
        if (matched) chosenWhCode = matched.warehouseCode;
      }

      if (!chosenWhCode) {
        if (whList.length >= 2) {
          await sendTextWithQuickReply(event, '請選擇要出庫的倉庫', buildQuickReplyForWarehouses('出', whList, outBox, outPiece), { eid });
          return;
        }
        chosenWhCode = whList[0].warehouseCode;
      }

      LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(lastWhKey, chosenWhCode);

      const snapBefore = await getWarehouseSnapshot(branch, skuLast, chosenWhCode, { eid });
      if (outBox > 0 && snapBefore.box < outBox) {
        return sendText(event, `庫存不足，無法出庫（倉別：${snapBefore.warehouseLabel}）\n目前庫存：${snapBefore.box}箱${snapBefore.piece}散`, { eid });
      }
      if (outPiece > 0 && snapBefore.piece < outPiece) {
        return sendText(event, `庫存不足，無法出庫（倉別：${snapBefore.warehouseLabel}）\n目前庫存：${snapBefore.box}箱${snapBefore.piece}散`, { eid });
      }

      const result = await callOutOnceTx({
        branch,
        sku: skuLast,
        outBox,
        outPiece,
        warehouseCode: chosenWhCode,
        lineUserId,
        meta: { eid },
      });

      const snapAfter = await getWarehouseSnapshot(branch, skuLast, chosenWhCode, { eid });

      await sendText(
        event,
        `✅ 出庫成功\n編號：${skuDisplay(skuLast)}\n倉別：${snapAfter.warehouseLabel}\n出庫：${Number(result.outBox || outBox)}箱 ${Number(result.outPiece || outPiece)}件\n👉目前庫存：${snapAfter.box}箱${snapAfter.piece}散`,
        { eid },
      );

      // GAS fire-and-forget
      try {
        const outAmountForGas =
          (Number(result.outBox || outBox) * snapAfter.unitsPerBox + Number(result.outPiece || outPiece)) *
          Number(snapAfter.unitPricePiece || result.unitPricePiece || 0);

        const payload = {
          type: 'log',
          group: String(branch || '').trim().toLowerCase(),
          sku: skuDisplay(skuLast),
          name: result.productName,
          units_per_box: snapAfter.unitsPerBox,
          unit_price: Number(snapAfter.unitPricePiece || result.unitPricePiece || 0),
          in_box: 0,
          in_piece: 0,
          out_box: Number(result.outBox || outBox),
          out_piece: Number(result.outPiece || outPiece),
          stock_box: Number(snapAfter.box || 0),
          stock_piece: Number(snapAfter.piece || 0),
          out_amount: outAmountForGas,
          stock_amount: Number(snapAfter.stockAmount || 0),
          warehouse: snapAfter.warehouseLabel,
          warehouse_code: chosenWhCode,
          created_at: tpeNowISO(),
          bot_ver: BOT_VER,
          db_host: SUPA_HOST,
          biz_date_0500: getBizDate0500TPE(),
        };

        postInventoryToGAS(payload).catch((e) => console.warn('[GAS ERROR]', e?.message || e));
      } catch (e) {
        console.warn('[GAS PAYLOAD ERROR]', e?.message || e);
      }
    });

    return;
  }
}

/* ======== Start server ======== */
app.listen(PORT, () => {
  const up = process.uptime().toFixed(1);
  console.log(`server up :${PORT} ver=${BOT_VER} db_host=${SUPA_HOST} up=${up}s`);
});
