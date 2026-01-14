import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/**
 * =========================================================
 *  LINE Bot for Inventory（查庫存/出庫）
 *
 * ✅ 你指定的規則：
 *  - 只有「出庫」需要鎖：同一個人同一個品項同一個倉庫，5 秒內只允許一次
 *  - 其他（查詢 / 編號 / 模糊查 / 點選品項 / 點倉庫）一律不鎖
 *
 * ✅ 解卡策略：
 *  - 所有回覆「先 reply（有 replyToken 就用）」；reply 太慢/失敗 → 自動 fallback 改用 push
 *  - 查詢、點選品項、點倉庫都走同一套 sendMsg（reply→push）
 *
 *  - 查詢：只在「當日有庫存（約 200 筆）」內做關鍵字比對（快取 3 秒）
 *  - 快照：public.get_business_day_stock
 *  - 出庫：fifo_out_and_log（單一交易）
 *  - biz_date：台北 05:00 切日
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

const BOT_VER = 'V2026-01-14_OUT_LOCK_5S_ONLY_REPLY_PUSH_FALLBACK';
const SUPA_TIMEOUT_MS = 8000; // 不中斷，只是避免 await 卡太久，超時就丟錯讓上層 fallback

/* ======== App / Supabase ======== */
const app = express(); // ⚠️ webhook 前不可掛 body parser
const START_MS = Date.now();

app.use((req, _res, next) => {
  const up = ((Date.now() - START_MS) / 1000).toFixed(1);
  console.log(
    `[請求] ${req.method} ${req.path} up=${up}s ua=${req.headers['user-agent'] || ''} x-line-signature=${
      req.headers['x-line-signature'] ? 'yes' : 'no'
    }`,
  );
  next();
});

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
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
const LAST_SKU_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> sku(lower)

const WH_LABEL_CACHE = new Map(); // key: kind_id 或 kind_name → kind_name（中文）
const WH_CODE_CACHE = new Map(); // key: kind_name（中文） → kind_id（代碼）

/* ✅ 查詢快取：當天有庫存清單（200筆） */
const STOCK_LIST_CACHE = new Map(); // key=`${branch}::${bizDate}` -> { ts, rows }

/* ✅ 只有出庫要鎖：5 秒 */
const OUT_LOCK = new Map(); // key -> lockUntilMs

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

function nowUpStr() {
  const up = ((Date.now() - START_MS) / 1000).toFixed(1);
  return `up=${up}s`;
}

function getDestinationFromEvent(event) {
  const src = event?.source || {};
  if (src.type === 'group' && src.groupId) return { to: src.groupId, toType: `group:${src.groupId.slice(0, 6)}...` };
  if (src.type === 'room' && src.roomId) return { to: src.roomId, toType: `room:${src.roomId.slice(0, 6)}...` };
  if (src.userId) return { to: src.userId, toType: `user:${src.userId.slice(0, 6)}...` };
  return { to: null, toType: 'unknown' };
}

function withTimeout(promise, ms, label = 'timeout') {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(label)), ms)),
  ]);
}

/**
 * ✅ 核心送訊息：優先 reply（快），reply 超時/失敗 -> fallback push
 * - 這就是你現在「常常沒回」的解法：不要死等 replyToken
 */
async function sendMsg(event, msg, opt = {}) {
  const { preferReply = true, replyTimeoutMs = 1200 } = opt;
  const token = event?.replyToken || null;
  const { to, toType } = getDestinationFromEvent(event);

  // 沒有目的地就放棄（理論上不會）
  if (!token && !to) return;

  // 先試 reply（但最多等 1.2 秒）
  if (preferReply && token) {
    try {
      const t0 = Date.now();
      await withTimeout(client.replyMessage(token, msg), replyTimeoutMs, 'reply_timeout');
      console.log(`[線路回覆] ok ms=${Date.now() - t0} ${nowUpStr()}`);
      return;
    } catch (e) {
      console.warn(`[線路回覆] fail (${e?.message || e}) -> fallback push ${toType} ${nowUpStr()}`);
    }
  }

  // fallback push
  if (to) {
    try {
      const t0 = Date.now();
      await client.pushMessage(to, msg);
      console.log(`[LINE PUSH] ok ms=${Date.now() - t0} to=${toType} ${nowUpStr()}`);
    } catch (e2) {
      console.error(`[LINE PUSH] fail to=${toType} err=${e2?.message || e2} ${nowUpStr()}`);
    }
  }
}

async function supaRpc(name, args) {
  const t0 = Date.now();
  const p = supabase.rpc(name, args);
  const { data, error } = await withTimeout(p, SUPA_TIMEOUT_MS, `supa_${name}_timeout`);
  const ms = Date.now() - t0;
  if (error) {
    console.warn(`[RPC] ${name} error ms=${ms} msg=${error.message}`);
    throw error;
  }
  console.log(`[RPC] ${name} ok ms=${ms}`);
  return data;
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

  // code 直接回（含 main / withdraw / swap / unspecified）
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

  // 中文→code cache
  if (WH_CODE_CACHE.has(label)) return WH_CODE_CACHE.get(label);

  // 固定表 reverse
  for (const [code, name] of FIX_CODE_TO_NAME.entries()) {
    if (name === label) {
      WH_CODE_CACHE.set(name, code);
      return code;
    }
  }

  // DB 查詢（kind_name -> kind_id）
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

/* ======== 業務日結存：單一 SKU（快照） ======== */
async function getWarehouseStockBySku(branch, sku) {
  const group = String(branch || '').trim().toLowerCase();
  const s = skuKey(sku);
  if (!group || !s) return [];

  const bizDate = getBizDate0500TPE();

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER}`);
  console.log(`[庫存 RPC] group=${group} bizDate=${bizDate} sku=${s} stage=before`);

  const data = await supaRpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: null,
  });

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
    `[庫存 RPC] stage=after rows=${rows.length} kept=${kept.length} wh=${kept.map((x) => `${x.warehouseCode}:${x.box}/${x.piece}`).join(',')}`,
  );

  return kept;
}

async function getWarehouseSnapshot(branch, sku, warehouseCodeOrLabel) {
  const group = String(branch || '').trim().toLowerCase();
  const s = skuKey(sku);
  const whCode = await getWarehouseCodeForLabel(warehouseCodeOrLabel || 'unspecified');
  const bizDate = getBizDate0500TPE();

  console.log(`[DB] host=${SUPA_HOST} ver=${BOT_VER}`);

  const data = await supaRpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: whCode,
  });

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
  const data = await supaRpc('daily_sheet_rows_full', {
    p_biz_date: bizDate,
    p_group: group,
  });

  const rows = Array.isArray(data) ? data : [];
  const kept = rows.filter((r) => pickNum(r['庫存箱數'] ?? 0, 0) > 0 || pickNum(r['庫存散數'] ?? 0, 0) > 0);

  STOCK_LIST_CACHE.set(key, { ts: Date.now(), rows: kept });

  console.log(`[庫存清單] rpc ok ms=${Date.now() - t0} rows=${rows.length} kept=${kept.length}`);
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
  const data = await supaRpc('fifo_out_and_log', args);

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

async function postInventoryToGAS(payload) {
  if (!GAS_URL_CACHE || !GAS_SECRET_CACHE) return;
  const cleanBaseUrl = GAS_URL_CACHE.replace(/\?.*$/, '');
  const callUrl = `${cleanBaseUrl}?secret=${encodeURIComponent(GAS_SECRET_CACHE)}`;
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
    console.warn('[GAS ERROR]', e?.message || e);
  }
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

/* ======== Utilities ======== */
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
    if (event?.type === 'postback') console.log(`[LINE POSTBACK] data=${event?.postback?.data || ''}`);
  } catch (e) {
    console.error('[LINE EVENT LOG ERROR]', e);
  }
}

/* ======== Server endpoints ======== */
const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };
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
        logEventSummary(ev);
        try {
          await handleEvent(ev);
        } catch (err) {
          console.error('[HANDLE EVENT ERROR]', err);
          // 這裡也用 fallback：避免 replyToken 失效造成你覺得「沒回」
          await sendMsg(ev, { type: 'text', text: `系統忙碌或發生錯誤：${err?.message || '未知錯誤'}` }, { preferReply: true });
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

/* ======== 出庫鎖（只鎖出庫，5 秒） ======== */
function outLockKey({ branch, lineUserId, sku, whCode }) {
  return `${String(branch || '').toLowerCase()}::${String(lineUserId || '')}::${skuKey(sku)}::${String(whCode || 'unspecified')}`;
}
function isOutLocked(key) {
  const until = OUT_LOCK.get(key) || 0;
  if (Date.now() < until) return true;
  OUT_LOCK.delete(key);
  return false;
}
function setOutLock(key, ms = 5000) {
  OUT_LOCK.set(key, Date.now() + ms);
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
    await sendMsg(event, { type: 'text', text: needBindMsg || '尚未綁定分店' }, { preferReply: true });
    return;
  }

  // ✅ db 指令
  if (event.type === 'message' && event.message.type === 'text') {
    const parsed0 = parseCommand(event.message.text || '');
    if (parsed0?.type === 'db') {
      const bizDate = getBizDate0500TPE();
      await sendMsg(
        event,
        { type: 'text', text: `BOT=${BOT_VER}\nDB_HOST=${SUPA_HOST}\nBIZ_DATE_0500=${bizDate}\nSUPA_TIMEOUT_MS=${SUPA_TIMEOUT_MS}\n${nowUpStr()}` },
        { preferReply: true },
      );
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
        await sendMsg(event, { type: 'text', text: '請先選商品（查/編號）再選倉庫' }, { preferReply: true });
        return;
      }

      const whCode = await getWarehouseCodeForLabel(pb.wh);
      LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whCode);

      const snap = await getWarehouseSnapshot(branch, sku, whCode);
      await sendMsg(
        event,
        { type: 'text', text: `編號：${skuDisplay(sku)}\n倉庫類別：${snap.warehouseLabel}\n庫存：${snap.box}箱${snap.piece}散` },
        { preferReply: true },
      );
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
      await sendMsg(event, { type: 'text', text: '請先選商品（查/編號）再選倉庫' }, { preferReply: true });
      return;
    }

    const whCode = await getWarehouseCodeForLabel(parsed.warehouse);
    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whCode);

    const snap = await getWarehouseSnapshot(branch, sku, whCode);
    await sendMsg(
      event,
      { type: 'text', text: `編號：${skuDisplay(sku)}\n倉庫類別：${snap.warehouseLabel}\n庫存：${snap.box}箱${snap.piece}散` },
      { preferReply: true },
    );
    return;
  }

  // 查詢共用
  const doQueryCommon = async (p) => {
    const sku = skuKey(p.sku);
    const whList = await getWarehouseStockBySku(branch, sku);
    if (!whList.length) {
      await sendMsg(event, { type: 'text', text: '無此商品庫存' }, { preferReply: true });
      return;
    }

    setLastSku(lineUserId, branch, sku);

    if (whList.length >= 2) {
      await sendMsg(
        event,
        {
          type: 'text',
          text: `名稱：${p.name}\n編號：${skuDisplay(sku)}\n👉請選擇倉庫`,
          quickReply: buildQuickReplyForWarehousesForQuery(whList),
        },
        { preferReply: true },
      );
      return;
    }

    const chosen = whList[0];
    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, chosen.warehouseCode);

    await sendMsg(
      event,
      {
        type: 'text',
        text: `名稱：${p.name}\n編號：${skuDisplay(sku)}\n箱入數：${p.unitsPerBox}\n單價：${p.price}\n倉庫類別：${chosen.warehouseLabel}\n庫存：${chosen.box}箱${chosen.piece}散`,
      },
      { preferReply: true },
    );
  };

  // 查 關鍵字
  if (parsed.type === 'query') {
    const list = await searchByNameInStock(parsed.keyword, branch);
    if (!list.length) {
      await sendMsg(event, { type: 'text', text: '無此商品庫存' }, { preferReply: true });
      return;
    }

    if (list.length > 1) {
      await sendMsg(
        event,
        { type: 'text', text: `找到以下與「${parsed.keyword}」相關的庫存品項`, quickReply: buildQuickReplyForProducts(list) },
        { preferReply: true },
      );
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }

  // 編號 / #
  if (parsed.type === 'sku') {
    const list = await searchBySkuInStock(parsed.sku, branch);
    if (!list.length) {
      await sendMsg(event, { type: 'text', text: '無此商品庫存' }, { preferReply: true });
      return;
    }

    if (list.length > 1) {
      await sendMsg(
        event,
        { type: 'text', text: `找到以下與「${parsed.sku}」相關的庫存品項`, quickReply: buildQuickReplyForProducts(list) },
        { preferReply: true },
      );
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }

  // 入/出庫
  if (parsed.type === 'change') {
    if (parsed.action === 'in') {
      if (role !== '主管') {
        await sendMsg(event, { type: 'text', text: '您無法使用「入庫」' }, { preferReply: true });
        return;
      }
      await sendMsg(event, { type: 'text', text: '入庫請改用 App 進行；LINE 僅提供出庫' }, { preferReply: true });
      return;
    }

    const outBox = parsed.box || 0;
    const outPiece = parsed.piece || 0;
    if (outBox === 0 && outPiece === 0) return;

    const skuLast = getLastSku(lineUserId, branch);
    if (!skuLast) {
      await sendMsg(event, { type: 'text', text: '請先用「查 商品」或「編號」選定「有庫存」商品後再出庫。' }, { preferReply: true });
      return;
    }

    const whList = await getWarehouseStockBySku(branch, skuLast);
    if (!whList.length) {
      await sendMsg(event, { type: 'text', text: '所有倉庫皆無庫存，無法出庫。' }, { preferReply: true });
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
        await sendMsg(
          event,
          { type: 'text', text: '請選擇要出庫的倉庫', quickReply: buildQuickReplyForWarehouses('出', whList, outBox, outPiece) },
          { preferReply: true },
        );
        return;
      }
      chosenWhCode = whList[0].warehouseCode;
    }

    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(lastWhKey, chosenWhCode);

    // ✅ 只有出庫鎖：5 秒（同人+同分店+同sku+同倉）
    const lockKey = outLockKey({ branch, lineUserId, sku: skuLast, whCode: chosenWhCode });
    if (isOutLocked(lockKey)) {
      await sendMsg(event, { type: 'text', text: '出庫處理中，請 5 秒後再試一次（避免重複扣庫）' }, { preferReply: true });
      return;
    }
    setOutLock(lockKey, 5000);

    try {
      // 出庫前 requery
      const snapBefore = await getWarehouseSnapshot(branch, skuLast, chosenWhCode);
      const curBox = snapBefore.box || 0;
      const curPiece = snapBefore.piece || 0;

      if (outBox > 0 && curBox < outBox) {
        await sendMsg(
          event,
          { type: 'text', text: `庫存不足，無法出庫（倉別：${snapBefore.warehouseLabel}）\n目前庫存：${curBox}箱${curPiece}散` },
          { preferReply: true },
        );
        return;
      }
      if (outPiece > 0 && curPiece < outPiece) {
        await sendMsg(
          event,
          { type: 'text', text: `庫存不足，無法出庫（倉別：${snapBefore.warehouseLabel}）\n目前庫存：${curBox}箱${curPiece}散` },
          { preferReply: true },
        );
        return;
      }

      const result = await callOutOnceTx({
        branch,
        sku: skuLast,
        outBox,
        outPiece,
        warehouseCode: chosenWhCode,
        lineUserId,
      });

      // 出庫後再查一次
      const snapAfter = await getWarehouseSnapshot(branch, skuLast, chosenWhCode);
      const whLabel = snapAfter.warehouseLabel;

      await sendMsg(
        event,
        {
          type: 'text',
          text: `✅ 出庫成功\n編號：${skuDisplay(skuLast)}\n倉別：${whLabel}\n出庫：${Number(result.outBox || outBox)}箱 ${Number(
            result.outPiece || outPiece,
          )}件\n👉目前庫存：${snapAfter.box}箱${snapAfter.piece}散`,
        },
        { preferReply: true },
      );

      // 推送 GAS（fire-and-forget）
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

        postInventoryToGAS(payload).catch(() => {});
      } catch {}
    } finally {
      // 不主動解鎖：讓它自然 5 秒到期（你指定）
    }

    return;
  }
}

/* ======== Start server ======== */
app.listen(PORT, () => {
  console.log(`server up :${PORT} ver=${BOT_VER} db_host=${SUPA_HOST}`);
});
