import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/**
 * =========================================================
 *  LINE Bot for Inventory（查庫存走 public.get_business_day_stock）
 *  - 出庫：fifo_out_and_log（單一交易）
 *  - 查庫存/快照：public.get_business_day_stock（05:00 分界、與試算表一致）
 *  - 倉庫字典：warehouse_kinds(kind_id, kind_name)
 *  - 箱對箱、散對散；不做單位換算
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
// ⚠️ 永遠用 warehouse_code 當唯一識別
const LAST_WAREHOUSE_CODE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> warehouse_code

// ✅ 修「點倉庫跳錯 SKU」：last sku 記憶體快取（DB 失敗也不影響）
const LAST_SKU_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> sku(lower)

const WH_LABEL_CACHE = new Map(); // key: kind_id 或 kind_name → kind_name（中文）
const WH_CODE_CACHE = new Map(); // key: kind_name（中文） → kind_id（代碼）

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
    return label;
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

/* ======== 業務日結存：統一查詢 RPC ======== */
async function getWarehouseStockBySku(branch, sku) {
  const group = String(branch || '').trim().toLowerCase();
  const s = skuKey(sku);
  if (!group || !s) return [];

  const bizDate = getBizDateTodayTPE();

  console.log(`[STOCK RPC] ver=V2026-01-12_ALLFIX group=${group} bizDate=${bizDate} sku=${s} stage=before`);

  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: null,
  });

  if (error) {
    console.log(
      `[STOCK RPC] ver=V2026-01-12_ALLFIX group=${group} bizDate=${bizDate} sku=${s} stage=error msg=${error.message}`,
    );
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
    `[STOCK RPC] ver=V2026-01-12_ALLFIX group=${group} bizDate=${bizDate} sku=${s} stage=after rows=${rows.length} kept=${kept.length} wh=${kept
      .map((x) => `${x.warehouseCode}:${x.box}/${x.piece}`)
      .join(',')}`,
  );

  return kept;
}

async function getWarehouseSnapshot(branch, sku, warehouseCodeOrLabel) {
  const group = String(branch || '').trim().toLowerCase();
  const s = skuKey(sku);
  const whCode = await getWarehouseCodeForLabel(warehouseCodeOrLabel || 'unspecified');
  const bizDate = getBizDateTodayTPE();

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

/* ======== Product search (products + 業務日結存) ======== */
async function searchByName(keyword, _role, branch) {
  const k = String(keyword || '').trim();
  if (!k) return [];
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品名稱', `%${k}%`)
    .limit(30);
  if (error) throw error;
  const list = Array.isArray(data) ? data : [];
  if (!list.length) return [];

  const checks = await Promise.all(
    list.map(async (p) => {
      try {
        const warehouses = await getWarehouseStockBySku(branch, p['貨品編號']);
        return { p, hasStock: warehouses.length > 0 };
      } catch {
        return { p, hasStock: false };
      }
    }),
  );

  const filtered = [];
  for (const { p, hasStock } of checks) {
    if (hasStock) filtered.push(p);
    if (filtered.length >= 10) break;
  }
  return filtered;
}

async function searchByBarcode(barcode, _role, branch) {
  const b = String(barcode || '').trim();
  if (!b) return [];
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .eq('條碼', b)
    .maybeSingle();
  if (error) throw error;
  if (!data) return [];
  const warehouses = await getWarehouseStockBySku(branch, data['貨品編號']);
  return warehouses.length ? [data] : [];
}

async function searchBySku(sku, _role, branch) {
  const s = String(sku || '').trim();
  if (!s) return [];

  const { data: exact, error: e1 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品編號', s)
    .maybeSingle();
  if (e1) throw e1;
  if (exact) {
    const warehouses = await getWarehouseStockBySku(branch, exact['貨品編號']);
    if (warehouses.length) return [exact];
  }

  const { data: like, error: e2 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品編號', `%${s}%`)
    .limit(30);
  if (e2) throw e2;
  const list = Array.isArray(like) ? like : [];
  if (!list.length) return [];

  const checks = await Promise.all(
    list.map(async (p) => {
      try {
        const warehouses = await getWarehouseStockBySku(branch, p['貨品編號']);
        return { p, hasStock: warehouses.length > 0 };
      } catch {
        return { p, hasStock: false };
      }
    }),
  );

  const filtered = [];
  for (const { p, hasStock } of checks) {
    if (hasStock) filtered.push(p);
    if (filtered.length >= 10) break;
  }
  return filtered;
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
      label: `${w.warehouseLabel}（${w.box}箱/${w.piece}件）`.slice(0, 20),
      text: `倉 ${w.warehouseCode}`, // 永遠用 code
    },
  }));
  return { items };
}

function buildQuickReplyForWarehouses(baseText, warehouseList, wantBox, wantPiece) {
  const items = warehouseList.slice(0, 12).map((w) => {
    const label = `${w.warehouseLabel}（${w.box}箱/${w.piece}散）`.slice(0, 20);
    const text = `${baseText} ${wantBox > 0 ? `${wantBox}箱 ` : ''}${wantPiece > 0 ? `${wantPiece}件 ` : ''}@${w.warehouseCode}`.trim();
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

  // ✅ 支援：出 3箱2件、出3箱、出3件、出1（預設=出1件）、並支援 @main/@撤台/倉庫:總倉
  const mChange = t.match(
    /^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散|件))?\s*(?:(\d+))?(?:\s*(?:@|（?\(?倉庫[:：=]\s*)([^)）]+)\)?)?\s*$/,
  );
  if (mChange) {
    const box = mChange[2] ? parseInt(mChange[2], 10) : 0;
    const pieceLabeled = mChange[3] ? parseInt(mChange[3], 10) : 0;
    const pieceTail = mChange[4] ? parseInt(mChange[4], 10) : 0;

    // ✅ 沒寫單位、只寫數字：當 piece（例：出1）
    const rawHasDigit = /\d+/.test(t);
    const hasBoxOrPieceUnit = /箱|個|散|件/.test(t);
    const piece =
      pieceLabeled || pieceTail || (!hasBoxOrPieceUnit && rawHasDigit && box === 0 ? parseInt(t.replace(/[^\d]/g, ''), 10) || 0 : 0);

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

/* ======== 單一交易出庫（RPC：fifo_out_and_log） ======== */
async function callOutOnceTx({ branch, sku, outBox, outPiece, warehouseCode, lineUserId }) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error(`找不到對應的使用者，請先在後台綁定帳號。`);

  const args = {
    p_group: String(branch || '').trim().toLowerCase(),
    p_sku: skuKey(sku),
    p_warehouse_name: String(warehouseCode || 'unspecified').trim(), // 這個參數名叫 name，但塞 code
    p_out_box: String(outBox ?? ''),
    p_out_piece: String(outPiece ?? ''),
    p_user_id: authUuid,
    p_source: 'LINE',
    p_at: new Date().toISOString(),
  };

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

/* ======== Last product helpers ======== */
async function upsertUserLastProduct(lineUserId, branch, sku) {
  if (!lineUserId) return;

  // ✅ 先寫入記憶體：保證「點倉庫」一定拿到剛剛查到的 sku
  LAST_SKU_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, skuKey(sku));

  // DB 備援：失敗也不影響流程
  try {
    const now = new Date().toISOString();
    const { data, error: e0 } = await supabase
      .from('user_last_product')
      .select('id')
      .eq('user_id', lineUserId)
      .eq('群組', branch)
      .maybeSingle();
    if (e0) {
      console.warn('[user_last_product select warn]', e0?.message || e0);
      return;
    }

    if (data) {
      const { error: e1 } = await supabase
        .from('user_last_product')
        .update({ 貨品編號: skuKey(sku), 建立時間: now })
        .eq('user_id', lineUserId)
        .eq('群組', branch);
      if (e1) console.warn('[user_last_product update warn]', e1?.message || e1);
    } else {
      const { error: e2 } = await supabase
        .from('user_last_product')
        .insert({ user_id: lineUserId, 群組: branch, 貨品編號: skuKey(sku), 建立時間: now });
      if (e2) console.warn('[user_last_product insert warn]', e2?.message || e2);
    }
  } catch (e) {
    console.warn('[user_last_product upsert warn]', e?.message || e);
  }
}

async function getLastSku(lineUserId, branch) {
  if (!lineUserId) return null;

  const mem = LAST_SKU_BY_USER_BRANCH.get(`${lineUserId}::${branch}`);
  if (mem) return skuKey(mem);

  const { data, error } = await supabase
    .from('user_last_product')
    .select('貨品編號')
    .eq('user_id', lineUserId)
    .eq('群組', branch)
    .order('建立時間', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;

  const sku = data?.['貨品編號'] ? skuKey(data['貨品編號']) : null;
  if (sku) LAST_SKU_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, sku);
  return sku;
}

/* ======== Utilities ======== */
function logEventSummary(event) {
  try {
    const src = event?.source || {};
    const msg = event?.message || {};
    const isGroup = src.type === 'group';
    const isRoom = src.type === 'room';
    console.log(
      `[LINE EVENT] type=${event?.type} msgType=${msg?.type} source=${src.type || '-'} groupId=${
        isGroup ? src.groupId : '-'
      } roomId=${isRoom ? src.roomId : '-'} userId=${src.userId || '-'} text="${msg?.type === 'text' ? msg.text : ''}"`,
    );
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

/* ======== Main Handler ======== */
async function lineHandler(req, res) {
  try {
    const events = req.body?.events || [];

    const tasks = events.map(async (ev) => {
      logEventSummary(ev);
      try {
        await handleEvent(ev);
      } catch (err) {
        console.error('[HANDLE EVENT ERROR]', err);
        const token = ev.replyToken;
        if (token) {
          try {
            await client.replyMessage(token, {
              type: 'text',
              text: `系統忙碌或發生錯誤：${err?.message || '未知錯誤'}`,
            });
          } catch (e2) {
            console.error('[HANDLE EVENT REPLY ERROR]', e2);
          }
        }
      }
    });

    await Promise.all(tasks);
    return res.status(200).send('OK');
  } catch (e) {
    console.error('[WEBHOOK ERROR]', e);
    return res.status(500).send('ERR');
  }
}

async function handleEvent(event) {
  if (event.type !== 'message' || event.message.type !== 'text') return;
  const text = event.message.text || '';
  const parsed = parseCommand(text);
  if (!parsed) return;

  const source = event.source || {};
  const isGroup = source.type === 'group';
  const lineUserId = source.userId || null;
  if (!isGroup && lineUserId) await autoRegisterUser(lineUserId);

  const { branch, role, blocked, needBindMsg } = await resolveBranchAndRole(event);
  if (blocked) return;
  if (!branch) {
    await client.replyMessage(event.replyToken, { type: 'text', text: needBindMsg || '此使用者尚未綁定分店，請管理員設定' });
    return;
  }

  const reply = (msg) => client.replyMessage(event.replyToken, msg);
  const replyText = (s) => reply({ type: 'text', text: s });

  // ========== 倉庫選擇 ==========
  if (parsed.type === 'wh_select') {
    const sku = await getLastSku(lineUserId, branch);
    if (!sku) {
      await replyText('請先選商品（查/條碼/編號）再選倉庫');
      return;
    }

    const whCode = await getWarehouseCodeForLabel(parsed.warehouse);
    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whCode);

    const snap = await getWarehouseSnapshot(branch, sku, whCode);

    const { data: prodRow } = await supabase
      .from('products')
      .select('貨品名稱, 箱入數, 單價')
      .ilike('貨品編號', sku)
      .maybeSingle();

    const name = prodRow?.['貨品名稱'] || sku;
    const unitsPerBox = Number(prodRow?.['箱入數'] || 1) || 1;
    const price = Number(prodRow?.['單價'] || 0);

    await replyText(
      `品名：${name}
編號：${skuDisplay(sku)}
箱入數：${unitsPerBox}
單價：${price}
倉庫類別：${snap.warehouseLabel}
庫存：${snap.box}箱${snap.piece}散`,
    );
    return;
  }

  // ========== 查詢共用 ==========
  const doQueryCommon = async (p) => {
    const sku = skuKey(p['貨品編號']);
    const whList = await getWarehouseStockBySku(branch, sku);
    if (!whList.length) {
      await replyText('無此商品庫存');
      return;
    }

    await upsertUserLastProduct(lineUserId, branch, sku);

    if (whList.length >= 2) {
      await reply({
        type: 'text',
        text: `名稱：${p['貨品名稱']}
編號：${skuDisplay(sku)}
👉請選擇倉庫`,
        quickReply: buildQuickReplyForWarehousesForQuery(whList),
      });
      return;
    }

    const chosen = whList[0];
    LAST_WAREHOUSE_CODE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, chosen.warehouseCode);

    const name = p['貨品名稱'] || sku;
    const unitsPerBox = Number(p['箱入數'] || 1) || 1;
    const price = Number(p['單價'] || 0);

    await replyText(
      `名稱：${name}
編號：${skuDisplay(sku)}
箱入數：${unitsPerBox}
單價：${price}
倉庫類別：${chosen.warehouseLabel}
庫存：${chosen.box}箱${chosen.piece}散`,
    );
  };

  if (parsed.type === 'query') {
    const list = await searchByName(parsed.keyword, role, branch);
    if (!list.length) return replyText('無此商品庫存');
    if (list.length > 1) {
      await reply({ type: 'text', text: `找到以下與「${parsed.keyword}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }

  if (parsed.type === 'barcode') {
    const list = await searchByBarcode(parsed.barcode, role, branch);
    if (!list.length) return replyText('無此商品庫存');
    await doQueryCommon(list[0]);
    return;
  }

  if (parsed.type === 'sku') {
    const list = await searchBySku(parsed.sku, role, branch);
    if (!list.length) return replyText('無此商品庫存');
    if (list.length > 1) {
      await reply({ type: 'text', text: `找到以下與「${parsed.sku}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }

  // ========== 入/出庫 ==========
  if (parsed.type === 'change') {
    // 入庫仍不開放（照你原規則）
    if (parsed.action === 'in') {
      if (role !== '主管') {
        await replyText('您無法使用「入庫」');
        return;
      }
      await replyText('入庫請改用 App 進行；LINE 僅提供出庫');
      return;
    }

    // ====== 出庫 ======
    const outBox = parsed.box || 0;
    const outPiece = parsed.piece || 0;

    if (outBox === 0 && outPiece === 0) return;

    const skuLast = await getLastSku(lineUserId, branch);
    if (!skuLast) {
      await replyText('請先用「查 商品」或「條碼/編號」選定「有庫存」商品後再出庫。');
      return;
    }

    const whList = await getWarehouseStockBySku(branch, skuLast);
    if (!whList.length) {
      await replyText('所有倉庫皆無庫存，無法出庫。');
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
        await reply({
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
      await replyText(`庫存不足，無法出庫（倉別：${snapBefore.warehouseLabel}）
目前庫存：${curBox}箱${curPiece}散`);
      return;
    }
    if (outPiece > 0 && curPiece < outPiece) {
      await replyText(`庫存不足，無法出庫（倉別：${snapBefore.warehouseLabel}）
目前庫存：${curBox}箱${curPiece}散`);
      return;
    }

    // 交易出庫
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
      await replyText(`操作失敗：${err?.message || '未知錯誤'}`);
      return;
    }

    // 出庫後再查一次（解「回覆未扣」）
    const snapAfter = await getWarehouseSnapshot(branch, skuLast, chosenWhCode);

    const whLabel = snapAfter.warehouseLabel;

    await replyText(
      `✅ 出庫成功
品名：${result.productName}
編號：${skuDisplay(skuLast)}
倉別：${whLabel}
出庫：${Number(result.outBox || outBox)}箱 ${Number(result.outPiece || outPiece)}件
👉目前庫存：${snapAfter.box}箱${snapAfter.piece}散`,
    );

    // 推送 GAS（用中文倉名；避免試算表倉庫別辨識不到）
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
        庫存金額: Number(snapAfter.stockAmount || 0),
        warehouse: whLabel, // ✅ 中文（總倉/撤台）
        warehouse_code: chosenWhCode, // ✅ 同步送 code，給你後續排查/轉換用
        created_at: tpeNowISO(),
      };

      postInventoryToGAS(payload).catch((e) => console.warn('[GAS FIRE-AND-FORGET ERROR]', e));
    } catch (e) {
      console.warn('[GAS PAYLOAD ERROR]', e);
    }

    return;
  }
}

/* ======== Start server ======== */
app.listen(PORT, () => {
  console.log(`伺服器運行在${PORT}端口 ver=V2026-01-12_ALLFIX`);
});
