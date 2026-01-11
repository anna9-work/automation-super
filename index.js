import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/**
 * =========================================================
 *  LINE Bot for Inventory（即時庫存：public.get_business_day_stock）
 *  - 查詢：只顯示「有庫存」商品（RPC 優先 / fallback）
 *  - 查庫存：public.get_business_day_stock（吃 daily_sheet_rows_full）
 *  - 出庫：public.fifo_out_and_log（單一交易）
 *
 *  修正：
 *  A) 群組狀態一律綁 groupId（避免查A出B、扣不到）
 *  B) 多倉：即使 quick reply 不出，也會提示「倉 main / 倉 withdraw」
 *  C) 出庫後回覆：再查一次該倉庫庫存，保證是最新
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

/* ======== App / Supabase ======== */
const app = express(); // ⚠️ webhook 前不可掛 body parser
app.use((req, _res, next) => {
  console.log(
    `[請求] ${req.method} ${req.path} ua=${req.headers['user-agent'] || ''} x-line-signature=${
      req.headers['x-line-signature'] ? 'yes' : 'no'
    }`,
  );
  next();
});

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

/* ======== Runtime caches ======== */
const LAST_WAREHOUSE_TOKEN_BY_ACTOR_BRANCH = new Map(); // key=`${actorKey}::${branch}` -> warehouse_code
const LAST_SKU_BY_ACTOR_BRANCH = new Map(); // key=`${actorKey}::${branch}` -> sku(lower)
const WH_LABEL_CACHE = new Map(); // key: code/id/name -> 中文名
const WH_CODE_CACHE = new Map(); // key: 中文名 -> code/id

/** 去重：避免 LINE webhook 超時重送造成重複處理 */
const DEDUPE_CACHE = new Map(); // key -> ts(ms)
const DEDUPE_TTL_MS = 10 * 60 * 1000;
function makeDedupeKey(ev) {
  const src = ev?.source || {};
  const msg = ev?.message || {};
  const actor = src.groupId ? `g:${src.groupId}` : src.userId ? `u:${src.userId}` : src.roomId ? `r:${src.roomId}` : 'unknown';
  const mid = msg?.id || '';
  if (mid) return `mid:${actor}:${mid}`;
  const rt = ev?.replyToken || '';
  if (rt) return `rt:${actor}:${rt}`;
  return null;
}
function isDuplicateAndMark(ev) {
  const key = makeDedupeKey(ev);
  if (!key) return false;
  const now = Date.now();
  const prev = DEDUPE_CACHE.get(key);
  if (prev && now - prev < DEDUPE_TTL_MS) return true;
  DEDUPE_CACHE.set(key, now);
  return false;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, ts] of DEDUPE_CACHE.entries()) {
    if (now - ts >= DEDUPE_TTL_MS) DEDUPE_CACHE.delete(k);
  }
}, 60 * 1000).unref?.();

/* ======== Fixed warehouse labels (code -> 中文) ======== */
/** ✅ 你的總倉代碼用 main */
const FIX_CODE_TO_NAME = new Map([
  ['main', '總倉'],
  ['main_warehouse', '總倉'], // 相容舊資料
  ['withdraw', '撤台'],
  ['swap', '夾換品'],
  ['prize', '代夾物'],
  ['unspecified', '未指定'],
]);

/* ======== Helpers ======== */
const skuKey = (s) => String(s || '').trim().toLowerCase();
const skuDisplay = (s) => {
  const t = String(s || '').trim();
  return t ? t.slice(0, 1).toUpperCase() + t.slice(1).toLowerCase() : '';
};

/* 業務日：台北 05:00 分界，回傳 'YYYY-MM-DD'（本地） */
function getBizDateTodayTPE() {
  const now = new Date();
  const tpe = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(now); // yyyy-mm-dd HH:mm:ss
  const [d, hms] = tpe.split(' ');
  const hh = parseInt(hms.split(':')[0], 10);
  if (hh < 5) {
    const dt = new Date(d + 'T00:00:00+08:00');
    dt.setDate(dt.getDate() - 1);
    return dt.toISOString().slice(0, 10);
  }
  return d;
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

/** ✅ 關鍵修正：群組一定用 groupId 綁狀態（不再用 userId） */
function getActorKey(event) {
  const src = event?.source || {};
  if (src.type === 'group' && src.groupId) return `g:${src.groupId}`;
  if (src.type === 'room' && src.roomId) return `r:${src.roomId}`;
  if (src.userId) return `u:${src.userId}`;
  return 'unknown';
}

function setLastSku(actorKey, branch, sku) {
  if (!actorKey || !branch || !sku) return;
  LAST_SKU_BY_ACTOR_BRANCH.set(`${actorKey}::${branch}`, skuKey(sku));
}
function getLastSku(actorKey, branch) {
  return LAST_SKU_BY_ACTOR_BRANCH.get(`${actorKey}::${branch}`) || null;
}
function setLastWarehouseToken(actorKey, branch, token) {
  if (!actorKey || !branch || !token) return;
  LAST_WAREHOUSE_TOKEN_BY_ACTOR_BRANCH.set(`${actorKey}::${branch}`, String(token).trim());
}
function getLastWarehouseToken(actorKey, branch) {
  return LAST_WAREHOUSE_TOKEN_BY_ACTOR_BRANCH.get(`${actorKey}::${branch}`) || null;
}

/* ======== Warehouse resolvers ======== */
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

async function getWarehouseCodeForLabel(displayName) {
  const label = String(displayName || '').trim();
  if (!label) return 'unspecified';
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
    const { data: u2 } = await supabase.from('users').select('群組').eq('user_id', userId).maybeSingle();
    return { branch: u2?.群組 || null, role, blocked, needBindMsg: '此使用者尚未綁定分店，請管理員設定' };
  }
}

async function autoRegisterUser(lineUserId) {
  if (!lineUserId) return;
  const { data } = await supabase.from('users').select('user_id').eq('user_id', lineUserId).maybeSingle();
  if (!data)
    await supabase.from('users').insert({ user_id: lineUserId, 群組: DEFAULT_GROUP, 角色: 'user', 黑名單: false });
}

/* ======== 查庫存（只回有庫存倉） ======== */
async function getWarehouseStockBySku(branch, sku) {
  const group = String(branch || '').trim().toLowerCase();
  const s = String(sku || '').trim().toLowerCase();
  if (!group || !s) return [];
  const bizDate = getBizDateTodayTPE();

  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: null,
  });
  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  return rows
    .map((r) => ({
      warehouseCode: String(r.warehouse_code || 'unspecified'),
      warehouseName: String(r.warehouse_name || r.warehouse_code || '未指定'),
      box: Number(r.box || 0),
      piece: Number(r.piece || 0),
      unitsPerBox: Number(r.units_per_box || 1),
      unitPricePiece: Number(r.unit_price_piece || 0),
    }))
    .filter((w) => w.box > 0 || w.piece > 0);
}

async function getWarehouseSnapshot(branch, sku, warehouseCode) {
  const group = String(branch || '').trim().toLowerCase();
  const s = String(sku || '').trim().toLowerCase();
  const whCode = String(warehouseCode || 'unspecified').trim();
  const bizDate = getBizDateTodayTPE();

  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: whCode,
  });
  if (error) throw error;

  const row = Array.isArray(data) ? data[0] : data;
  if (!row)
    return {
      box: 0,
      piece: 0,
      unitsPerBox: 1,
      unitPricePiece: 0,
      stockAmount: 0,
      warehouseCode: whCode,
    };

  const box = Number(row.box || 0);
  const piece = Number(row.piece || 0);
  const unitsPerBox = Number(row.units_per_box || 1);
  const unitPricePiece = Number(row.unit_price_piece || 0);
  const stockAmount = (box * unitsPerBox + piece) * unitPricePiece;

  return { box, piece, unitsPerBox, unitPricePiece, stockAmount, warehouseCode: String(row.warehouse_code || whCode) };
}

/* ======== 搜尋：只顯示「有庫存」商品 ======== */
async function searchStockInBiz(branch, keyword, limit = 20) {
  const group = String(branch || '').trim().toLowerCase();
  const k = String(keyword || '').trim();
  if (!group || !k) return [];
  const bizDate = getBizDateTodayTPE();

  try {
    const { data, error } = await supabase.rpc('search_stock_sku_inbiz', {
      p_group: group,
      p_biz_date: bizDate,
      p_keyword: k,
      p_limit: limit,
    });
    if (error) throw error;
    return Array.isArray(data) ? data : [];
  } catch (e) {
    console.warn('[searchStockInBiz fallback]', e?.message || e);

    const { data: prod, error: pe } = await supabase
      .from('products')
      .select('貨品名稱, 貨品編號, 條碼')
      .or(`貨品名稱.ilike.%${k}%,貨品編號.ilike.%${k}%`)
      .limit(50);
    if (pe) throw pe;
    const list = Array.isArray(prod) ? prod : [];
    if (!list.length) return [];

    const out = [];
    for (const p of list) {
      const sku = skuKey(p['貨品編號']);
      if (!sku) continue;
      const wh = await getWarehouseStockBySku(branch, sku);
      if (wh.length) {
        out.push({ product_sku: sku, 貨品名稱: p['貨品名稱'] || sku, 條碼: p['條碼'] || null });
        if (out.length >= limit) break;
      }
    }
    return out;
  }
}

function mapSearchRowsToProducts(rows) {
  return (rows || []).map((r) => ({
    貨品編號: String(r.product_sku || r.product_sku_raw || '').trim().toLowerCase(),
    貨品名稱: String(r['貨品名稱'] || r.product_name || '').trim(),
  }));
}

async function searchByName(keyword, _role, branch) {
  const rows = await searchStockInBiz(branch, keyword, 20);
  return mapSearchRowsToProducts(rows).slice(0, 10);
}
async function searchBySku(sku, _role, branch) {
  const rows = await searchStockInBiz(branch, sku, 20);
  return mapSearchRowsToProducts(rows).slice(0, 10);
}
async function searchByBarcode(barcode, _role, branch) {
  const rows = await searchStockInBiz(branch, barcode, 20);
  return mapSearchRowsToProducts(rows).slice(0, 10);
}

/* ======== Quick Replies ======== */
function buildQuickReplyForProducts(products) {
  const items = products.slice(0, 12).map((p) => ({
    type: 'action',
    action: { type: 'message', label: `${p['貨品名稱']}`.slice(0, 20), text: `編號 ${p['貨品編號']}` },
  }));
  return { items };
}
function buildQuickReplyForWarehousesForQuery(warehouseList) {
  const items = warehouseList.slice(0, 12).map((w) => ({
    type: 'action',
    action: {
      type: 'message',
      label: `${w.warehouseName}（${w.box}箱/${w.piece}件）`.slice(0, 20),
      text: `倉 ${w.warehouseCode}`,
    },
  }));
  return { items };
}
function buildQuickReplyForWarehouses(baseText, warehouseList, wantBox, wantPiece) {
  const items = warehouseList.slice(0, 12).map((w) => {
    const label = `${w.warehouseName}（${w.box}箱/${w.piece}散）`.slice(0, 20);
    const text = `${baseText} ${wantBox > 0 ? `${wantBox}箱 ` : ''}${wantPiece > 0 ? `${wantPiece}件 ` : ''}@${w.warehouseCode}`
      .trim();
    return { type: 'action', action: { type: 'message', label, text } };
  });
  return { items };
}

/* ======== Command parser ======== */
function parseCommand(text) {
  const t = (text || '').trim();
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
    /^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散|件))?(?:\s*(\d+))?(?:\s*(?:@|（?\(?倉庫[:：=]\s*)([^)）]+)\)?)?\s*$/,
  );
  if (mChange) {
    const box = mChange[2] ? parseInt(mChange[2], 10) : 0;
    const pieceLabeled = mChange[3] ? parseInt(mChange[3], 10) : 0;
    const pieceTail = mChange[4] ? parseInt(mChange[4], 10) : 0;
    const warehouse = (mChange[5] || '').trim();
    return {
      type: 'change',
      action: /入/.test(mChange[1]) ? 'in' : 'out',
      box,
      piece: pieceLabeled || pieceTail,
      warehouse: warehouse || null,
    };
  }
  return null;
}

/* ======== 出庫（RPC：fifo_out_and_log） ======== */
async function callOutOnceTx({ branch, sku, outBox, outPiece, warehouseLabel, lineUserId }) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error(`找不到對應的使用者，請先在後台綁定帳號。`);

  const { data, error } = await supabase.rpc('fifo_out_and_log', {
    p_group: String(branch || '').trim().toLowerCase(),
    p_sku: skuKey(sku),
    p_warehouse_name: String(warehouseLabel || '未指定').trim(),
    p_out_box: String(outBox ?? ''),
    p_out_piece: String(outPiece ?? ''),
    p_user_id: authUuid,
    p_source: 'LINE',
    p_at: new Date().toISOString(),
  });
  if (error) throw error;
  return Array.isArray(data) ? data[0] : data;
}

/* ======== GAS (optional) ======== */
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
    await fetch(callUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch {}
}

/* ======== Logging ======== */
function logEventSummary(event) {
  try {
    const src = event?.source || {};
    const msg = event?.message || {};
    console.log(
      `[LINE EVENT] type=${event?.type} msgType=${msg?.type} source=${src.type || '-'} userId=${src.userId || '-'} groupId=${
        src.groupId || '-'
      } text="${msg?.type === 'text' ? msg.text : ''}"`,
    );
  } catch {}
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

/* ======== Main Handler ======== */
async function lineHandler(req, res) {
  // 先回 200，降低重送
  res.status(200).send('OK');

  setImmediate(() => {
    const events = req.body?.events || [];
    const tasks = events.map(async (ev) => {
      logEventSummary(ev);
      if (isDuplicateAndMark(ev)) return;

      try {
        await handleEvent(ev);
      } catch (err) {
        console.error('[HANDLE EVENT ERROR]', err);
        const token = ev.replyToken;
        if (token) {
          try {
            await client.replyMessage(token, { type: 'text', text: `系統忙碌或發生錯誤：${err?.message || '未知錯誤'}` });
          } catch {}
        }
      }
    });

    Promise.allSettled(tasks).catch(() => {});
  });
}

async function handleEvent(event) {
  if (event.type !== 'message' || event.message.type !== 'text') return;

  const text = event.message.text || '';
  const parsed = parseCommand(text);
  if (!parsed) return;

  const source = event.source || {};
  const lineUserId = source.userId || null;
  const actorKey = getActorKey(event);

  if (lineUserId && source.type !== 'group') await autoRegisterUser(lineUserId);

  const { branch, role, blocked, needBindMsg } = await resolveBranchAndRole(event);
  if (blocked) return;

  const reply = (msg) => client.replyMessage(event.replyToken, msg);
  const replyText = (s) => reply({ type: 'text', text: s });

  if (!branch) return replyText(needBindMsg || '此使用者尚未綁定分店，請管理員設定');

  // ========== 倉庫選擇 ==========
  if (parsed.type === 'wh_select') {
    const sku = getLastSku(actorKey, branch);
    if (!sku) return replyText('請先選商品（查/條碼/編號）再選倉庫');

    let whToken = parsed.warehouse;
    if (!/^[a-z0-9_]+$/i.test(whToken)) whToken = await getWarehouseCodeForLabel(whToken);

    setLastWarehouseToken(actorKey, branch, whToken);

    const whLabel = await resolveWarehouseLabel(whToken);
    const snap = await getWarehouseSnapshot(branch, sku, whToken);

    const { data: prodRow } = await supabase
      .from('products')
      .select('貨品名稱, 箱入數, 單價')
      .ilike('貨品編號', sku)
      .maybeSingle();

    const name = prodRow?.['貨品名稱'] || skuDisplay(sku);
    const unitsPerBox = Number(prodRow?.['箱入數'] || 1) || 1;
    const price = Number(prodRow?.['單價'] || 0);

    await replyText(
      `名稱：${name}
編號：${skuDisplay(sku)}
箱入數：${unitsPerBox}
單價：${price}
倉庫類別：${whLabel}（${whToken}）
庫存：${snap.box}箱${snap.piece}散`,
    );
    return;
  }

  // ========== 查詢共用 ==========
  const doQueryCommon = async (p) => {
    const sku = skuKey(p['貨品編號']);
    if (!sku) return replyText('無此商品庫存');

    setLastSku(actorKey, branch, sku);

    const whList = await getWarehouseStockBySku(branch, sku);
    if (!whList.length) return replyText('無此商品庫存');

    const { data: prodRow } = await supabase
      .from('products')
      .select('貨品名稱, 箱入數, 單價')
      .ilike('貨品編號', sku)
      .maybeSingle();

    const name = prodRow?.['貨品名稱'] || p['貨品名稱'] || skuDisplay(sku);
    const unitsPerBox = Number(prodRow?.['箱入數'] || 1) || 1;
    const price = Number(prodRow?.['單價'] || 0);

    const whLines = await Promise.all(
      whList.map(async (w) => {
        const label = await resolveWarehouseLabel(w.warehouseCode);
        return `- ${label}（${w.warehouseCode}）：${w.box}箱${w.piece}散`;
      }),
    );

    if (whList.length >= 2) {
      // ✅ 就算 quick reply 沒跳，也會提示可用指令
      const hint = whList
        .slice(0, 6)
        .map((w) => `倉 ${w.warehouseCode}`)
        .join(' / ');

      await reply({
        type: 'text',
        text: `名稱：${name}
編號：${skuDisplay(sku)}
箱入數：${unitsPerBox}
單價：${price}
👉此商品有多個倉庫，請選擇倉庫（可直接輸入：${hint}）
${whLines.join('\n')}`,
        quickReply: buildQuickReplyForWarehousesForQuery(whList),
      });
      return;
    }

    const chosen = whList[0];
    setLastWarehouseToken(actorKey, branch, chosen.warehouseCode);
    const chosenLabel = await resolveWarehouseLabel(chosen.warehouseCode);

    await replyText(
      `名稱：${name}
編號：${skuDisplay(sku)}
箱入數：${unitsPerBox}
單價：${price}
倉庫類別：${chosenLabel}（${chosen.warehouseCode}）
庫存：${chosen.box}箱${chosen.piece}散`,
    );
  };

  if (parsed.type === 'query') {
    const list = await searchByName(parsed.keyword, role, branch);
    if (!list.length) return replyText('無此商品庫存');
    if (list.length > 1)
      return reply({ type: 'text', text: `找到以下與「${parsed.keyword}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
    return doQueryCommon(list[0]);
  }
  if (parsed.type === 'barcode') {
    const list = await searchByBarcode(parsed.barcode, role, branch);
    if (!list.length) return replyText('無此商品庫存');
    if (list.length > 1)
      return reply({ type: 'text', text: `找到以下與「${parsed.barcode}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
    return doQueryCommon(list[0]);
  }
  if (parsed.type === 'sku') {
    const list = await searchBySku(parsed.sku, role, branch);
    if (!list.length) return replyText('無此商品庫存');
    if (list.length > 1)
      return reply({ type: 'text', text: `找到以下與「${parsed.sku}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
    return doQueryCommon(list[0]);
  }

  // ========== 出庫 ==========
  if (parsed.type === 'change') {
    if (parsed.action === 'in' && role !== '主管') return replyText('您無法使用「入庫」');
    if (parsed.action !== 'out') return replyText('入庫請改用 App 進行；LINE 僅提供出庫');

    if (parsed.box === 0 && parsed.piece === 0) return;

    const skuLast = getLastSku(actorKey, branch);
    if (!skuLast) return replyText('請先用「查 商品」或「條碼/編號」選定「有庫存」商品後再出庫。');

    if (!lineUserId) return replyText('此聊天環境無法取得使用者 ID，為避免扣錯庫存，暫不允許出庫。');

    const outBox = parsed.box || 0;
    const outPiece = parsed.piece || 0;

    const whList = await getWarehouseStockBySku(branch, skuLast);
    if (!whList.length) return replyText('所有倉庫皆無庫存，無法出庫。');

    let whToken = parsed.warehouse || null;

    // 支援：出 1件 @main
    if (whToken && whToken.startsWith('@')) whToken = whToken.slice(1).trim();

    // 支援：出 1件 @總倉
    if (whToken && !/^[a-z0-9_]+$/i.test(whToken)) whToken = await getWarehouseCodeForLabel(whToken);

    if (!whToken) whToken = getLastWarehouseToken(actorKey, branch) || null;

    if (!whToken) {
      if (whList.length >= 2) {
        return reply({
          type: 'text',
          text: '請選擇要出庫的倉庫（或輸入：倉 main / 倉 withdraw）',
          quickReply: buildQuickReplyForWarehouses('出', whList, outBox, outPiece),
        });
      }
      whToken = whList[0].warehouseCode;
    }

    setLastWarehouseToken(actorKey, branch, whToken);
    const whLabel = await resolveWarehouseLabel(whToken);

    const snapBefore = await getWarehouseSnapshot(branch, skuLast, whToken);
    if (outBox > 0 && snapBefore.box < outBox) return replyText(`庫存不足（${whLabel}）目前：${snapBefore.box}箱${snapBefore.piece}散`);
    if (outPiece > 0 && snapBefore.piece < outPiece) return replyText(`庫存不足（${whLabel}）目前：${snapBefore.box}箱${snapBefore.piece}散`);

    let row;
    try {
      row = await callOutOnceTx({
        branch,
        sku: skuLast,
        outBox,
        outPiece,
        warehouseLabel: whLabel, // ✅ 中文
        lineUserId,
      });
    } catch (err) {
      console.error('[fifo_out_and_log ERROR]', err);
      return replyText(`操作失敗：${err?.message || '未知錯誤'}`);
    }

    // ✅ 出庫後再查一次（真正即時）
    const snapAfter = await getWarehouseSnapshot(branch, skuLast, whToken);
    const productName = String(row?.product_name || row?.貨品名稱 || '').trim() || skuDisplay(skuLast);

    await replyText(
      `✅ 出庫成功
品名：${productName}
編號：${skuDisplay(skuLast)}
倉別：${whLabel}（${whToken}）
出庫：${outBox}箱 ${outPiece}件
👉目前庫存：${snapAfter.box}箱${snapAfter.piece}散`,
    );

    // GAS
    try {
      const payload = {
        type: 'log',
        group: String(branch || '').trim().toLowerCase(),
        sku: skuDisplay(skuLast),
        name: productName,
        units_per_box: snapAfter.unitsPerBox,
        unit_price: Number(snapAfter.unitPricePiece || 0),
        in_box: 0,
        in_piece: 0,
        out_box: Number(outBox || 0),
        out_piece: Number(outPiece || 0),
        stock_box: Number(snapAfter.box || 0),
        stock_piece: Number(snapAfter.piece || 0),
        stock_amount: Number(snapAfter.stockAmount || 0),
        warehouse: whLabel,
        created_at: tpeNowISO(),
      };
      postInventoryToGAS(payload).catch(() => {});
    } catch {}

    return;
  }
}

/* ======== Start server ======== */
app.listen(PORT, () => {
  console.log(`伺服器運行在${PORT}端口`);
});
