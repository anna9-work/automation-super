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
const LAST_WAREHOUSE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> 中文倉名
const WH_LABEL_CACHE = new Map(); // key: kind_id 或 kind_name → kind_name（中文）
const WH_CODE_CACHE = new Map(); // key: kind_name（中文） → kind_id（代碼）

/* ======== Fixed warehouse labels (code -> 中文) ======== */
const FIX_CODE_TO_NAME = new Map([
  ['main_warehouse', '總倉'],
  ['prize', '代夾物'],
  ['swap', '夾換品'],
  ['unspecified', '未指定'],
  ['withdraw', '撤台'],
]);

/* ======== Helpers ======== */
const skuKey = (s) => String(s || '').trim();
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
    // 05:00 前算前一天
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
  // 1) 固定表
  if (FIX_CODE_TO_NAME.has(key)) {
    const name = FIX_CODE_TO_NAME.get(key);
    WH_LABEL_CACHE.set(key, name);
    WH_CODE_CACHE.set(name, key);
    return name;
  }
  // 2) DB 查詢（kind_id or kind_name）
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
  // 找不到就原字串
  WH_LABEL_CACHE.set(key, key);
  return key;
}
async function getWarehouseCodeForLabel(displayName) {
  const label = String(displayName || '').trim();
  if (!label) return 'unspecified';
  if (WH_CODE_CACHE.has(label)) return WH_CODE_CACHE.get(label);
  // 固定表 reverse
  for (const [code, name] of FIX_CODE_TO_NAME.entries()) {
    if (name === label) {
      WH_CODE_CACHE.set(name, code);
      return code;
    }
  }
  // DB 查詢
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

/* ======== 業務日結存：統一查詢 RPC（與試算表一致） ======== */
async function getWarehouseStockBySku(branch, sku) {
  const group = String(branch || '').trim().toLowerCase();
  const s = String(sku || '').trim();
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
      warehouse: String(r.warehouse_name || '未指定'),
      box: Number(r.box || 0),
      piece: Number(r.piece || 0),
      unitsPerBox: Number(r.units_per_box || 1),
      unitPricePiece: Number(r.unit_price_piece || 0),
    }))
    .filter((w) => w.box > 0 || w.piece > 0); // 只保留有庫存的倉
}

async function getWarehouseSnapshot(branch, sku, warehouseDisplayName) {
  const group = String(branch || '').trim().toLowerCase();
  const s = String(sku || '').trim();
  const whCode = await getWarehouseCodeForLabel(warehouseDisplayName || '未指定');
  const bizDate = getBizDateTodayTPE();

  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: whCode,
  });
  if (error) throw error;

  const row = Array.isArray(data) ? data[0] : data;
  if (!row) return { box: 0, piece: 0, unitsPerBox: 1, unitPricePiece: 0, stockAmount: 0 };

  const box = Number(row.box || 0);
  const piece = Number(row.piece || 0);
  const unitsPerBox = Number(row.units_per_box || 1);
  const unitPricePiece = Number(row.unit_price_piece || 0);
  const stockAmount = (box * unitsPerBox + piece) * unitPricePiece;

  return { box, piece, unitsPerBox, unitPricePiece, stockAmount };
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
      label: `${w.warehouse}（${w.box}箱/${w.piece}件）`.slice(0, 20),
      text: `倉 ${w.warehouse}`,
    },
  }));
  return { items };
}
function buildQuickReplyForWarehouses(baseText, warehouseList, wantBox, wantPiece) {
  const items = warehouseList.slice(0, 12).map((w) => {
    const label = `${w.warehouse}（${w.box}箱/${w.piece}散）`.slice(0, 20);
    const text = `${baseText} ${wantBox > 0 ? `${wantBox}箱 ` : ''}${wantPiece > 0 ? `${wantPiece}件 ` : ''}@${
      w.warehouse
    }`.trim();
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

/* ======== 單一交易出庫（RPC：fifo_out_and_log） ======== */
async function callOutOnceTx({ branch, sku, outBox, outPiece, warehouseLabel, lineUserId }) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error(`找不到對應的使用者（${lineUserId}）。請先在 line_user_map 建立對應。`);

  const args = {
    p_group: String(branch || '').trim().toLowerCase(),
    p_sku: skuKey(sku),
    p_warehouse_name: String(warehouseLabel || '未指定').trim(),
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
    outBox: Number(row?.out_box || 0),
    outPiece: Number(row?.out_piece || 0),
    afterBox: Number(row?.after_box || 0),
    afterPiece: Number(row?.after_piece || 0),
    warehouseName: String(row?.warehouse_name || warehouseLabel || '未指定'),
    stockAmount: Number(row?.stock_amount || 0),
  };
}

/* ======== GAS Webhook (optional, idempotent) ======== */
let GAS_URL_CACHE = (ENV_GAS_URL || '').trim();
let GAS_SECRET_CACHE = (ENV_GAS_SECRET || '').trim();
let GAS_LOADED_ONCE = false;
async function loadGasConfigFromDBIfNeeded() {
  if (GAS_URL_CACHE && GAS_SECRET_CACHE) {
    GAS_LOADED_ONCE = true;
    return;
  }
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
  } catch (e) {
    GAS_LOADED_ONCE = true;
    console.warn('⚠️ 載入 GAS 設定失敗（RPC get_app_settings）：', e?.message || e);
  }
}
async function getGasConfig() {
  if (!GAS_LOADED_ONCE || !GAS_URL_CACHE || !GAS_SECRET_CACHE) await loadGasConfigFromDBIfNeeded();
  return { url: GAS_URL_CACHE, secret: GAS_SECRET_CACHE };
}
async function postInventoryToGAS(payload) {
  const { url, secret } = await getGasConfig();
  if (!url || !secret) {
    console.warn('⚠️ GAS 未設定（略過推送）');
    return;
  }
  const callUrl = `${url.replace(/\?+.*/, '')}?secret=${encodeURIComponent(secret)}`;
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

/* ======== Utilities ======== */
function logEventSummary(event) {
  try {
    const src = event?.source || {};
    const msg = event?.message || {};
    const isGroup = src.type === 'group';
    const isRoom = src.type === 'room';
    console.log(
      `[LINE EVENT] type=${event?.type} source=${src.type || '-'} groupId=${
        isGroup ? src.groupId : '-'
      } roomId=${isRoom ? src.roomId : '-'} userId=${src.userId || '-'} text="${
        msg?.type === 'text' ? msg.text : ''
      }"`,
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

/* ======== Last product helpers ======== */
async function upsertUserLastProduct(lineUserId, branch, sku) {
  if (!lineUserId) return;
  const now = new Date().toISOString();
  const { data } = await supabase
    .from('user_last_product')
    .select('id')
    .eq('user_id', lineUserId)
    .eq('群組', branch)
    .maybeSingle();
  if (data) {
    await supabase
      .from('user_last_product')
      .update({ 貨品編號: sku, 建立時間: now })
      .eq('user_id', lineUserId)
      .eq('群組', branch);
  } else {
    await supabase
      .from('user_last_product')
      .insert({ user_id: lineUserId, 群組: branch, 貨品編號: sku, 建立時間: now });
  }
}
async function getLastSku(lineUserId, branch) {
  const { data, error } = await supabase
    .from('user_last_product')
    .select('貨品編號')
    .eq('user_id', lineUserId)
    .eq('群組', branch)
    .order('建立時間', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data?.['貨品編號'] || null;
}

/* ======== Main Handler ======== */
async function lineHandler(req, res) {
  try {
    const events = req.body?.events || [];
    for (const ev of events) {
      logEventSummary(ev);
      try {
        await handleEvent(ev);
      } catch (err) {
        console.error('[HANDLE EVENT ERROR]', err);
      }
    }
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
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: needBindMsg || '此使用者尚未綁定分店，請管理員設定',
    });
    return;
  }

  const reply = (msg) => client.replyMessage(event.replyToken, msg);
  const replyText = (s) => reply({ type: 'text', text: s });

  // ========== 倉庫選擇（使用者輸入「倉XXX」） ==========
  if (parsed.type === 'wh_select') {
    const sku = await getLastSku(lineUserId, branch);
    if (!sku) {
      await replyText('請先選商品（查/條碼/編號）再選倉庫');
      return;
    }
    const wh = await resolveWarehouseLabel(parsed.warehouse);
    LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);

    const snap = await getWarehouseSnapshot(branch, sku, wh);
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
編號：${sku}
箱入數：${unitsPerBox}
單價：${price}
庫存：${snap.box}箱${snap.piece}散`,
    );
    return;
  }

  // ========== 查詢（products + 業務日結存）共用邏輯 ==========
  const doQueryCommon = async (p) => {
    const sku = p['貨品編號'];
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
編號：${sku}
👉請選擇倉庫`,
        quickReply: buildQuickReplyForWarehousesForQuery(whList),
      });
      return;
    }

    const chosen = whList[0];
    LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, chosen.warehouse);

    const name = p['貨品名稱'] || sku;
    const unitsPerBox = Number(p['箱入數'] || 1) || 1;
    const price = Number(p['單價'] || 0);

    await replyText(
      `名稱：${name}
編號：${sku}
箱入數：${unitsPerBox}
單價：${price}
倉庫類別：${chosen.warehouse}
庫存：${chosen.box}箱${chosen.piece}散`,
    );
  };

  // ========== 查詢：關鍵字 ==========
  if (parsed.type === 'query') {
    const list = await searchByName(parsed.keyword, role, branch);
    if (!list.length) {
      await replyText('無此商品庫存');
      return;
    }
    if (list.length > 1) {
      await reply({
        type: 'text',
        text: `找到以下與「${parsed.keyword}」相關的選項`,
        quickReply: buildQuickReplyForProducts(list),
      });
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }
  // ========== 查詢：條碼 ==========
  if (parsed.type === 'barcode') {
    const list = await searchByBarcode(parsed.barcode, role, branch);
    if (!list.length) {
      await replyText('無此商品庫存');
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }
  // ========== 查詢：貨品編號 ==========
  if (parsed.type === 'sku') {
    const list = await searchBySku(parsed.sku, role, branch);
    if (!list.length) {
      await replyText('無此商品庫存');
      return;
    }
    if (list.length > 1) {
      await reply({
        type: 'text',
        text: `找到以下與「${parsed.sku}」相關的選項`,
        quickReply: buildQuickReplyForProducts(list),
      });
      return;
    }
    await doQueryCommon(list[0]);
    return;
  }

  // ========== 入/出庫 ==========
  if (parsed.type === 'change') {
    if (parsed.action === 'in' && role !== '主管') {
      await replyText('您無法使用「入庫」');
      return;
    }
    if (parsed.box === 0 && parsed.piece === 0) return;

    const skuLast = await getLastSku(lineUserId, branch);
    if (!skuLast) {
      await replyText('請先用「查 商品」或「條碼/編號」選定「有庫存」商品後再入/出庫。');
      return;
    }

    if (parsed.action === 'out') {
      const outBox = parsed.box || 0;
      const outPiece = parsed.piece || 0;

      const whList = await getWarehouseStockBySku(branch, skuLast);
      if (!whList.length) {
        await replyText('所有倉庫皆無庫存，無法出庫。');
        return;
      }

      const lastWhKey = `${lineUserId || ''}::${branch}`;
      const lastWhLabel = LAST_WAREHOUSE_BY_USER_BRANCH.get(lastWhKey) || null;

      if (!parsed.warehouse) {
        if (lastWhLabel) {
          const matched = whList.find((w) => w.warehouse === lastWhLabel);
          if (matched) {
            parsed.warehouse = lastWhLabel;
          }
        }

        if (!parsed.warehouse) {
          if (whList.length >= 2) {
            await reply({
              type: 'text',
              text: '請選擇要出庫的倉庫',
              quickReply: buildQuickReplyForWarehouses('出', whList, outBox, outPiece),
            });
            return;
          }
          parsed.warehouse = whList[0].warehouse;
        }
      }

      const wh = await resolveWarehouseLabel(parsed.warehouse || '未指定');
      LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);

      const snap = await getWarehouseSnapshot(branch, skuLast, wh);
      const curBox = snap.box || 0;
      const curPiece = snap.piece || 0;

      if (outBox > 0 && curBox < outBox) {
        await replyText(`庫存不足，無法出庫（倉別：${wh}）
目前庫存：${curBox}箱${curPiece}散`);
        return;
      }
      if (outPiece > 0 && curPiece < outPiece) {
        await replyText(`庫存不足，無法出庫（倉別：${wh}）
目前庫存：${curBox}箱${curPiece}散`);
        return;
      }

      // 只把 fifo_out_and_log 包在 try 裡，失敗就直接回錯誤訊息
      let result;
      try {
        result = await callOutOnceTx({
          branch,
          sku: skuLast,
          outBox,
          outPiece,
          warehouseLabel: wh,
          lineUserId,
        });
      } catch (err) {
        console.error('[fifo_out_and_log ERROR]', err);
        await replyText(`操作失敗：${err?.message || '未知錯誤'}`);
        return;
      }

      if (result.afterBox < 0 || result.afterPiece < 0) {
        console.error('[FATAL] 負庫存異常，出庫交易結果：', result);
        await replyText('庫存異常，操作取消，請聯絡管理員。');
        return;
      }

      // 回覆 LINE：如果這裡失敗就只記 log，不再重試，避免把錯誤吃掉
      try {
        await replyText(
          `✅ 出庫成功
` +
            `品名：${result.productName}
` +
            `編號：${skuLast}
` +
            `倉別：${result.warehouseName}
` +
            `出庫：${result.outBox}箱 ${result.outPiece}件
` +
            `👉目前庫存：${result.afterBox}箱${result.afterPiece}散`,
        );
      } catch (e) {
        console.error('[REPLY ERROR]', e);
        return;
      }

      // 推 GAS：改成 fire-and-forget，不影響使用者回覆
      try {
        const outAmountForGas =
          (Number(result.outBox || 0) * result.unitsPerBox + Number(result.outPiece || 0)) *
          Number(result.unitPricePiece || 0);

        const payload = {
          type: 'log',
          group: String(branch || '').trim().toLowerCase(),
          sku: skuDisplay(skuLast),
          name: result.productName,
          units_per_box: result.unitsPerBox,
          unit_price: Number(result.unitPricePiece || 0),
          in_box: 0,
          in_piece: 0,
          out_box: Number(result.outBox || 0),
          out_piece: Number(result.outPiece || 0),
          stock_box: Number(result.afterBox || 0),
          stock_piece: Number(result.afterPiece || 0),
          out_amount: outAmountForGas,
          stock_amount: Number(result.stockAmount || 0),
          庫存金額: Number(result.stockAmount || 0),
          warehouse: result.warehouseName,
          created_at: tpeNowISO(),
        };

        // 不 await，只記錄錯誤
        postInventoryToGAS(payload).catch((e) => console.warn('[GAS FIRE-AND-FORGET ERROR]', e));
      } catch (e) {
        console.warn('[GAS PAYLOAD ERROR]', e);
      }

      return;
    }

    // ========= 入庫（目前不開放 LINE 操作） =========
    await replyText('入庫請改用 App 進行；LINE 僅提供出庫');
    return;
  }
}

/* ======== Start server ======== */
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
