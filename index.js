import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/**
 * =========================================================
 *  LINE Bot for Inventory（快回 + 去重 + 只有出庫鎖 5 秒）
 *  - 查詢：只在「當日有庫存（約 200 筆）」內做關鍵字比對（快）
 *  - 快照：public.get_business_day_stock（與試算表一致）
 *  - 出庫：fifo_out_and_log（單一交易）
 *  - 支援 message + postback
 *  - webhook 立刻回 200（避免 LINE 重送）
 *
 *  ✅ 重點修正：
 *  A) 只對「出庫」做 per-user lock（5 秒），查詢/選倉/點品項不鎖
 *  B) 事件去重（webhookEventId / message.id）避免 LINE 重送造成卡與重複處理
 *  C) reply 失敗 → 自動 push fallback（可 push 到 groupId 或 userId）
 *  D) Supabase / LINE / GAS 全部加 timeout，避免卡死
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

const BOT_VER = 'V2026-01-14_OUT_LOCK_5S_ONLY_REPLY_PUSH_FALLBACK_DEDUP_TIMEOUT';

/* ======== Timeouts ======== */
const SUPA_TIMEOUT_MS = 8000; // Supabase RPC timeout
const LINE_TIMEOUT_MS = 8000; // LINE reply/push timeout
const GAS_TIMEOUT_MS = 6000;  // GAS webhook timeout

/* ======== Utilities: timeout fetch wrapper ======== */
async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(t);
  }
}

/* ======== App / Supabase ======== */
const app = express(); // webhook 前不可掛 body parser

const STARTED_AT = Date.now();
app.use((req, _res, next) => {
  const up = ((Date.now() - STARTED_AT) / 1000).toFixed(1);
  console.log(
    `[請求] ${req.method} ${req.path} up=${up}s ua=${req.headers['user-agent'] || ''} x-line-signature=${
      req.headers['x-line-signature'] ? 'yes' : 'no'
    }`,
  );
  next();
});

const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };
const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });

// Supabase: 用自訂 fetch 加 timeout（避免卡死）
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
  global: {
    fetch: (url, options) => fetchWithTimeout(url, options, SUPA_TIMEOUT_MS),
  },
});

function getSupabaseHost() {
  try {
    const u = new URL(SUPABASE_URL);
    return u.host;
  } catch {
    return String(SUPABASE_URL || '');
  }
}
const SUPA_HOST = getSupabaseHost();

/* ======== Runtime caches ======== */
const LAST_WAREHOUSE_CODE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> warehouse_code
const LAST_SKU_BY_USER_BRANCH = new Map();            // key=`${userId}::${branch}` -> sku(lower)

const WH_LABEL_CACHE = new Map(); // key: kind_id 或 kind_name → kind_name（中文）
const WH_CODE_CACHE = new Map();  // key: kind_name（中文） → kind_id（代碼）

// 當日庫存清單快取（3 秒）
const STOCK_LIST_CACHE = new Map(); // key=`${branch}::${bizDate}` -> { ts, rows }

// 事件去重（2 分鐘）
const EVENT_DEDUP = new Map(); // id -> ts
const EVENT_DEDUP_TTL_MS = 2 * 60 * 1000;

// 出庫鎖（只鎖出庫，5 秒）
const OUT_LOCK = new Map(); // key=`${userId}::${branch}` -> untilMs
const OUT_LOCK_MS = 5000;

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

/* ======== Dedup ======== */
function gcDedup() {
  const now = Date.now();
  for (const [k, ts] of EVENT_DEDUP.entries()) {
    if (now - ts > EVENT_DEDUP_TTL_MS) EVENT_DEDUP.delete(k);
  }
}

function getEventDedupId(ev) {
  // LINE v2 常見：webhookEventId（最可靠）
  if (ev?.webhookEventId) return `weid:${ev.webhookEventId}`;
  // message.id 也可（同一訊息重送仍會一致）
  if (ev?.message?.id) return `mid:${ev.message.id}`;
  // 最後退回：replyToken（不一定穩定）
  if (ev?.replyToken) return `rt:${ev.replyToken}`;
  return null;
}

/* ======== Warehouse resolvers（對齊 warehouse_kinds） ======== */
async function resolveWarehouseLabel(codeOrName) {
  const key = String(codeOrName || '').trim();
  if (!key) return '未指定';
  if (WH_LABEL_CACHE.has(key)) return WH_LABEL_CACHE.get(key);

  if (FIX_CODE_TO_NAME.has(key)) {
    const name = FIX_CODE_TO_NAME.get(key);
    WH_LABEL_CACHE.set(key, name);
    WH_CODE_CACHE.set(name, key);
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
      WH_LABEL_CACHE.set(key, data.kind_name);
      WH_LABEL_CACHE.set(data.kind_id, data.kind_name);
      WH_CODE_CACHE.set(data.kind_name, data.kind_id);
      return data.kind_name;
    }
  } catch {}

  WH_LABEL_CACHE.set(key, key);
  return key;
}

async function getWarehouseCodeForLabel(displayNameOrCode) {
  const label = String(displayNameOrCode || '').trim();
  if (!label) return 'unspecified';

  // code 直接回
  if (/^[a-z0-9_]+$/i.test(label)) {
    if (FIX_CODE_TO_NAME.has(label)) return label;
    try {
      const { data } = await supabase
        .from('warehouse_kinds')
        .select('kind_id, kind_name')
        .or(`kind_id.eq.${label},kind_name.eq.${label}`)
        .limit(1)
        .maybeSingle();
      if (data?.kind_id) {
        WH_CODE_CACHE.set(data.kind_name, data.kind_id);
        WH_LABEL_CACHE.set(data.kind_id, data.kind_name);
        return data.kind_id;
      }
    } catch {}
    return label.toLowerCase();
  }

  if (WH_CODE_CACHE.has(label)) return WH_CODE_CACHE.get(label);

  for (const [code, name] of FIX_CODE_TO_NAME.entries()) {
    if (name === label) {
      WH_CODE_CACHE.set(name, code);
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
      WH_CODE_CACHE.set(data.kind_name, data.kind_id);
      WH_LABEL_CACHE.set(data.kind_id, data.kind_name);
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
  if (error) {
    console.warn('[resolveAuthUuid] line_user_map error:', error);
    return null;
  }
  return data?.auth_user_id || null;
}

async function resolveBranchAndRole(event) {
  const src = event.source || {};
  const userId = src.userId || null;
  const isGroup = src.type === 'group';
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

  if (isGroup) {
    const { data: lg } = await supabase
      .from('line_groups')
      .select('群組')
      .eq('line_group_id', src.groupId)
      .maybeSingle();
    return { branch: lg?.群組 || null, role, blocked, needBindMsg: '此群組尚未綁定分店，請管理員設定' };
  } else {
    const { data: u2 } = await supabase
      .from('users')
      .select('群組')
      .eq('user_id', userId)
      .maybeSingle();
    return { branch: u2?.群組 || null, role, blocked, needBindMsg: '此使用者尚未綁定分店，請管理員設定' };
  }
}

async function autoRegisterUser(lineUserId) {
  if (!lineUserId) return;
  const { data } = await supabase.from('users').select('user_id').eq('user_id', lineUserId).maybeSingle();
  if (!data)
    await supabase.from('users').insert({ user_id: lineUserId, 群組: DEFAULT_GROUP, 角色: 'user', 黑名單: false });
}

/* ======== Last product helpers ======== */
function setLastSku(lineUserId, branch, sku) {
  if (!lineUserId) return;
  LAST_SKU_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, skuKey(sku));
}
function getLastSku(lineUserId, branch) {
  if (!lineUserId) return null;
  const mem = LAST_SKU_BY_USER_BRANCH.get(`${lineUserId}::${branch}`);
  return mem ? skuKey(mem) : null;
}

/* ======== LINE reply/push with timeout + fallback ======== */
async function lineReplyWithTimeout(replyToken, message) {
  const t0 = Date.now();
  const p = client.replyMessage(replyToken, message);
  const timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('LINE reply timeout')), LINE_TIMEOUT_MS));
  await Promise.race([p, timeout]);
  return Date.now() - t0;
}

async function linePushWithTimeout(to, message) {
  const t0 = Date.now();
  const p = client.pushMessage(to, message);
  const timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('LINE push timeout')), LINE_TIMEOUT_MS));
  await Promise.race([p, timeout]);
  return Date.now() - t0;
}

async function replyOrPush(event, message) {
  const up = ((Date.now() - STARTED_AT) / 1000).toFixed(1);
  const replyToken = event?.replyToken || null;
  const src = event?.source || {};
  const isGroup = src.type === 'group';
  const isRoom = src.type === 'room';
  const userId = src.userId || null;
  const groupId = src.groupId || null;
  const roomId = src.roomId || null;

  // 先嘗試 reply（最快）
  if (replyToken) {
    try {
      const ms = await lineReplyWithTimeout(replyToken, message);
      console.log(`[線路回覆] ok ms=${ms} up=${up}s`);
      return true;
    } catch (e) {
      console.warn('[線路回覆] fail -> push fallback:', e?.message || e);
    }
  }

  // reply 失敗 -> push
  const to = isGroup ? groupId : isRoom ? roomId : userId;
  if (to) {
    try {
      const ms = await linePushWithTimeout(to, message);
      console.log(`[LINE PUSH] ok ms=${ms} to=${isGroup ? 'group' : isRoom ? 'room' : 'user'}:${String(to).slice(0, 12)}...`);
      return true;
    } catch (e2) {
      console.error('[LINE PUSH] fail:', e2?.message || e2);
    }
  }
  return false;
}

/* ======== 業務日結存：單一 SKU（快照） ======== */
async function getWarehouseStockBySku(branch, sku) {
  const group = String(branch || '').trim().toLowerCase();
  const s = skuKey(sku);
  if (!group || !s) return [];

  const bizDate = getBizDate0500TPE();

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER}`);
  console.log(`[庫存 RPC] group=${group} bizDate=${bizDate} sku=${s} stage=before`);

  const t0 = Date.now();
  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: null,
  });
  const ms = Date.now() - t0;
  if (error) {
    console.log(`[庫存 RPC] error ms=${ms} msg=${error.message}`);
    throw error;
  }
  console.log(`[RPC] 取得業務日庫存成功，耗時 ${ms} 毫秒`);

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
    `[庫存 RPC] stage=after rows=${rows.length} kept=${kept.length} wh=${kept
      .map((x) => `${x.warehouseCode}:${x.box}/${x.piece}`)
      .join(',')}`,
  );

  return kept;
}

async function getWarehouseSnapshot(branch, sku, warehouseCodeOrLabel) {
  const group = String(branch || '').trim().toLowerCase();
  const s = skuKey(sku);
  const whCode = await getWarehouseCodeForLabel(warehouseCodeOrLabel || 'unspecified');
  const bizDate = getBizDate0500TPE();

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER}`);
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
  };
}

/* ======== ✅ 當日有庫存清單（一次 RPC + 快取） ======== */
async function getTodayStockRows(branch) {
  const group = String(branch || '').trim().toLowerCase();
  if (!group) return [];
  const bizDate = getBizDate0500TPE();
  const key = `${group}::${bizDate}`;

  const cached = STOCK_LIST_CACHE.get(key);
  if (cached && Date.now() - cached.ts < 3000) return cached.rows; // 3 秒快取

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER}`);
  const t0 = Date.now();
  const { data, error } = await supabase.rpc('daily_sheet_rows_full', { p_biz_date: bizDate, p_group: group });
  const ms = Date.now() - t0;
  if (error) throw error;
  console.log(`[RPC] daily_sheet_rows_full ok ms=${ms}`);

  const rows = Array.isArray(data) ? data : [];
  const kept = rows.filter((r) => pickNum(r['庫存箱數'] ?? 0, 0) > 0 || pickNum(r['庫存散數'] ?? 0, 0) > 0);

  console.log(`[庫存清單] rpc ok ms=${ms} 行=${rows.length} 保留=${kept.length}`);

  STOCK_LIST_CACHE.set(key, { ts: Date.now(), rows: kept });
  return kept;
}

/* ======== ✅ 關鍵字查詢：只在當日庫存內比對（超快） ======== */
async function searchByNameInStock(keyword, branch) {
  const k = String(keyword || '').trim();
  if (!k) return [];
  const rows = await getTodayStockRows(branch);

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

async function searchBySkuInStock(skuInput, branch) {
  const s = skuKey(skuInput);
  if (!s) return [];
  const rows = await getTodayStockRows(branch);

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
    const text = `${baseText} ${wantBox > 0 ? `${wantBox}箱 ` : ''}${wantPiece > 0 ? `${wantPiece}件 ` : ''}@${w.warehouseLabel}`
      .trim();
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

  const mBarcode = t.match(/^條碼[:：]?\s*(.+)$/);
  if (mBarcode) return { type: 'barcode', barcode: mBarcode[1].trim() };

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

/* ======== ✅ Postback parser ======== */
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

/* ======== 單一交易出庫（RPC：fifo_out_and_log） ======== */
async function callOutOnceTx({ branch, sku, outBox, outPiece, warehouseCode, lineUserId }) {
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

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER}`);
  const { data, error } = await supabase.rpc('fifo_out_and_log', args);
  if (error) throw error;

  const row = Array.isArray(data) ? data[0] : data;
  return {
    productName: row?.product_name || sku,
    unitsPerBox: Number(row?.units_per_box || 1) || 1,
    unitPricePiece: Number(row?.unit_price_piece || 0),
    outBox: Number(row?.out_box || outBox || 0),
    outPiece: Number(row?.out_piece || outPiece || 0),
    warehouseCode: String(warehouseCode || 'unspecified'),
  };
}

/* ======== GAS Webhook (optional, fire-and-forget) ======== */
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
    console.log('[GAS CONFIG] url =', GAS_URL_CACHE ? GAS_URL_CACHE.slice(0, 80) : '(empty)');
  } catch (e) {
    GAS_LOADED_ONCE = true;
    GAS_LAST_LOAD_MS = now;
    console.warn('⚠️ 載入 GAS 設定失敗（RPC get_app_settings）：', e?.message || e);
  }
}

async function getGasConfig() {
  if (!GAS_LOADED_ONCE || !GAS_URL_CACHE || !GAS_SECRET_CACHE) await loadGasConfigFromDBIfNeeded(true);
  return { url: GAS_URL_CACHE, secret: GAS_SECRET_CACHE };
}

async function postInventoryToGAS(payload) {
  const { url, secret } = await getGasConfig();
  if (!url || !secret) {
    console.warn('⚠️ GAS 未設定（略過推送）');
    return;
  }
  const cleanBaseUrl = url.replace(/\?.*$/, '');
  const callUrl = `${cleanBaseUrl}?secret=${encodeURIComponent(secret)}`;
  try {
    console.log('[GAS CALL]', cleanBaseUrl);
    const res = await fetchWithTimeout(
      callUrl,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      },
      GAS_TIMEOUT_MS,
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      console.warn('[GAS WARN]', res.status, txt);
    }
  } catch (e) {
    console.warn('[GAS ERROR]', e?.message || e);
  }
}

/* ======== Event logging ======== */
function logEventSummary(event) {
  try {
    const src = event?.source || {};
    const msg = event?.message || {};
    const isGroup = src.type === 'group';
    const isRoom = src.type === 'room';
    console.log(
      `[LINE EVENT] type=${event?.type} msgType=${msg?.type || '-'} source=${src.type || '-'} groupId=${
        isGroup ? src.groupId : '-'
      } roomId=${isRoom ? src.roomId : '-'} userId=${src.userId || '-'} text="${msg?.type === 'text' ? msg.text : ''}"`,
    );
    if (event?.type === 'postback') {
      console.log(`[LINE POSTBACK] data=${event?.postback?.data || ''}`);
    }
  } catch (e) {
    console.error('[LINE EVENT LOG ERROR]', e);
  }
}

/* ======== Server endpoints ======== */
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));
app.get('/webhook', (_req, res) => res.status(200).send('OK'));
app.get('/line/webhook', (_req, res) => res.status(200).send('OK'));
app.post('/webhook', line.middleware(lineConfig), lineHandler);
app.post('/line/webhook', line.middleware(lineConfig), lineHandler);

app.use((err, req, res, next) => {
  if (req.path === '/webhook' || req.path === '/line/webhook') {
    console.error('[LINE MIDDLEWARE ERROR]', err?.message || err);
    return res.status(400).end();
  }
  return next(err);
});

/* ======== ✅ Main Handler：先回 200 再處理 ======== */
async function lineHandler(req, res) {
  try {
    const events = req.body?.events || [];
    res.status(200).send('OK');

    setImmediate(() => {
      events.forEach(async (ev) => {
        // 去重
        gcDedup();
        const did = getEventDedupId(ev);
        if (did) {
          if (EVENT_DEDUP.has(did)) {
            console.log(`[DEDUP] skip ${did}`);
            return;
          }
          EVENT_DEDUP.set(did, Date.now());
        }

        logEventSummary(ev);

        try {
          await handleEvent(ev);
        } catch (err) {
          console.error('[HANDLE EVENT ERROR]', err);
          await replyOrPush(ev, {
            type: 'text',
            text: `系統忙碌或發生錯誤：${err?.message || '未知錯誤'}`,
          });
        }
      });
    });
  } catch (e) {
    console.error('[WEBHOOK ERROR]', e);
    try {
      return res.status(200).send('OK');
    } catch {}
  }
}

/* ======== Event logic ======== */
async function handleEvent(event) {
  const source = event.source || {};
  const isGroup = source.type === 'group';
  const lineUserId = source.userId || null;

  if (!isGroup && lineUserId) await autoRegisterUser(lineUserId);

  const { branch, role, blocked, needBindMsg } = await resolveBranchAndRole(event);
  if (blocked) return;

  if (!branch) {
    await replyOrPush(event, { type: 'text', text: needBindMsg || '尚未綁定分店' });
    return;
  }

  // db 指令
  if (event.type === 'message' && event.message.type === 'text') {
    const parsed0 = parseCommand(event.message.text || '');
    if (parsed0?.type === 'db') {
      const bizDate = getBizDate0500TPE();
      const up = ((Date.now() - STARTED_AT) / 1000).toFixed(1);
      await replyOrPush(event, {
        type: 'text',
        text: `BOT=${BOT_VER}\nDB_HOST=${SUPA_HOST}\nBIZ_DATE_0500=${bizDate}\nSUPA_TIMEOUT_MS=${SUPA_TIMEOUT_MS}\nUPTIME=${up}s`,
      });
      return;
    }
  }

  // postback（點倉庫）
  if (event.type === 'postback') {
    const pb = parsePostback(event?.postback?.data);
    if (!pb) return;

    if (pb.type === 'wh_select_postback') {
      const sku = pb.sku || getLastSku(lineUserId, branch);
      if (!sku) {
        await replyOrPush(event, { type: 'text', text: '請先選商品（查/編號）再選倉庫' });
        return;
      }

      const whCode = await getWarehouseCodeForLabel(pb.wh);
      LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whCode);

      const snap = await getWarehouseSnapshot(branch, sku, whCode);
      await replyOrPush(event, {
        type: 'text',
        text: `編號：${skuDisplay(sku)}\n倉庫類別：${snap.warehouseLabel}\n庫存：${snap.box}箱${snap.piece}散`,
      });
      return;
    }
    return;
  }

  if (event.type !== 'message' || event.message.type !== 'text') return;

  const text = event.message.text || '';
  const parsed = parseCommand(text);
  if (!parsed) return;

  // 倉庫選擇（文字）
  if (parsed.type === 'wh_select') {
    const sku = getLastSku(lineUserId, branch);
    if (!sku) {
      await replyOrPush(event, { type: 'text', text: '請先選商品（查/編號）再選倉庫' });
      return;
    }

    const whCode = await getWarehouseCodeForLabel(parsed.warehouse);
    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whCode);

    const snap = await getWarehouseSnapshot(branch, sku, whCode);
    await replyOrPush(event, {
      type: 'text',
      text: `編號：${skuDisplay(sku)}\n倉庫類別：${snap.warehouseLabel}\n庫存：${snap.box}箱${snap.piece}散`,
    });
    return;
  }

  // 查詢共用
  const doQueryCommon = async (p) => {
    const sku = skuKey(p.sku);
    const whList = await getWarehouseStockBySku(branch, sku);
    if (!whList.length) {
      await replyOrPush(event, { type: 'text', text: '無此商品庫存' });
      return;
    }

    setLastSku(lineUserId, branch, sku);

    if (whList.length >= 2) {
      await replyOrPush(event, {
        type: 'text',
        text: `名稱：${p.name}\n編號：${skuDisplay(sku)}\n👉請選擇倉庫`,
        quickReply: buildQuickReplyForWarehousesForQuery(whList),
      });
      return;
    }

    const chosen = whList[0];
    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, chosen.warehouseCode);

    await replyOrPush(event, {
      type: 'text',
      text:
        `名稱：${p.name}\n編號：${skuDisplay(sku)}\n箱入數：${p.unitsPerBox}\n單價：${p.price}\n` +
        `倉庫類別：${chosen.warehouseLabel}\n庫存：${chosen.box}箱${chosen.piece}散`,
    });
  };

  // 查 關鍵字
  if (parsed.type === 'query') {
    const list = await searchByNameInStock(parsed.keyword, branch);
    if (!list.length) {
      await replyOrPush(event, { type: 'text', text: '無此商品庫存' });
      return;
    }

    if (list.length > 1) {
      await replyOrPush(event, {
        type: 'text',
        text: `找到以下與「${parsed.keyword}」相關的庫存品項`,
        quickReply: buildQuickReplyForProducts(list),
      });
      return;
    }

    await doQueryCommon(list[0]);
    return;
  }

  // 編號 / #
  if (parsed.type === 'sku') {
    const list = await searchBySkuInStock(parsed.sku, branch);
    if (!list.length) {
      await replyOrPush(event, { type: 'text', text: '無此商品庫存' });
      return;
    }

    if (list.length > 1) {
      await replyOrPush(event, {
        type: 'text',
        text: `找到以下與「${parsed.sku}」相關的庫存品項`,
        quickReply: buildQuickReplyForProducts(list),
      });
      return;
    }

    await doQueryCommon(list[0]);
    return;
  }

  // 入/出庫
  if (parsed.type === 'change') {
    if (parsed.action === 'in') {
      if (role !== '主管') {
        await replyOrPush(event, { type: 'text', text: '您無法使用「入庫」' });
        return;
      }
      await replyOrPush(event, { type: 'text', text: '入庫請改用 App 進行；LINE 僅提供出庫' });
      return;
    }

    // ✅ 只有出庫才鎖 5 秒（同 user + branch）
    const lockKey = `${lineUserId || 'no_user'}::${branch}`;
    const now = Date.now();
    const until = OUT_LOCK.get(lockKey) || 0;
    if (now < until) {
      await replyOrPush(event, { type: 'text', text: '⚠️ 出庫處理中，請稍後再試一次（5 秒內）' });
      return;
    }
    OUT_LOCK.set(lockKey, now + OUT_LOCK_MS);

    try {
      const outBox = parsed.box || 0;
      const outPiece = parsed.piece || 0;
      if (outBox === 0 && outPiece === 0) return;

      const skuLast = getLastSku(lineUserId, branch);
      if (!skuLast) {
        await replyOrPush(event, { type: 'text', text: '請先用「查 商品」或「編號」選定「有庫存」商品後再出庫。' });
        return;
      }

      const whList = await getWarehouseStockBySku(branch, skuLast);
      if (!whList.length) {
        await replyOrPush(event, { type: 'text', text: '所有倉庫皆無庫存，無法出庫。' });
        return;
      }

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
          await replyOrPush(event, {
            type: 'text',
            text: '請選擇要出庫的倉庫',
            quickReply: buildQuickReplyForWarehouses('出', whList, outBox, outPiece),
          });
          return;
        }
        chosenWhCode = whList[0].warehouseCode;
      }

      LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(lastWhKey, chosenWhCode);

      // 出庫前 requery
      const snapBefore = await getWarehouseSnapshot(branch, skuLast, chosenWhCode);
      const curBox = snapBefore.box || 0;
      const curPiece = snapBefore.piece || 0;

      if (outBox > 0 && curBox < outBox) {
        await replyOrPush(event, {
          type: 'text',
          text: `庫存不足，無法出庫（倉別：${snapBefore.warehouseLabel}）\n目前庫存：${curBox}箱${curPiece}散`,
        });
        return;
      }
      if (outPiece > 0 && curPiece < outPiece) {
        await replyOrPush(event, {
          type: 'text',
          text: `庫存不足，無法出庫（倉別：${snapBefore.warehouseLabel}）\n目前庫存：${curBox}箱${curPiece}散`,
        });
        return;
      }

      // 出庫交易
      let result;
      try {
        result = await callOutOnceTx({
          branch,
          sku: skuLast,
          outBox,
          outPiece,
          warehouseCode: chosenWhCode,
          lineUserId,
        });
      } catch (err) {
        console.error('[fifo_out_and_log ERROR]', err);
        await replyOrPush(event, { type: 'text', text: `操作失敗：${err?.message || '未知錯誤'}` });
        return;
      }

      // 出庫後再查一次
      const snapAfter = await getWarehouseSnapshot(branch, skuLast, chosenWhCode);
      const whLabel = snapAfter.warehouseLabel;

      await replyOrPush(event, {
        type: 'text',
        text:
          `✅ 出庫成功\n編號：${skuDisplay(skuLast)}\n倉別：${whLabel}\n出庫：${Number(result.outBox || outBox)}箱 ${Number(
            result.outPiece || outPiece,
          )}件\n👉目前庫存：${snapAfter.box}箱${snapAfter.piece}散`,
      });

      // 推送 GAS（不影響回覆速度）
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
          warehouse: whLabel,
          warehouse_code: chosenWhCode,
          created_at: tpeNowISO(),
          bot_ver: BOT_VER,
          db_host: SUPA_HOST,
          biz_date_0500: getBizDate0500TPE(),
        };

        postInventoryToGAS(payload).catch((e) => console.warn('[GAS FIRE-AND-FORGET ERROR]', e?.message || e));
      } catch (e) {
        console.warn('[GAS PAYLOAD ERROR]', e?.message || e);
      }
    } finally {
      // lock 保留 5 秒自然過期，不主動解除（防重送/併發）
    }

    return;
  }
}

/* ======== Start server ======== */
app.listen(PORT, () => {
  console.log(`伺服器已啟動：${PORT} 版本=${BOT_VER} 資料庫主機=${SUPA_HOST}`);
});
