// index.js — LINE 出庫（事件流水=inventory_logs、FIFO=inventory_lots），可直接覆蓋使用
import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

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
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) console.error('缺少 Supabase 環境變數');

const app = express();
// ⚠️ 不要在 webhook 前面加任何 body parser！(express.json/express.raw 皆不行)
app.use((req, _res, next) => {
  console.log(`[請求] ${req.method} ${req.path} ua=${req.headers['user-agent'] || ''} x-line-signature=${req.headers['x-line-signature'] ? 'yes' : 'no'}`);
  next();
});

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

/** ===== 進程記憶 ===== */
const LAST_WAREHOUSE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> warehouseName(中文)
const WAREHOUSE_NAME_CACHE = new Map();          // key=codeOrName -> 中文name

/** 固定倉庫映射：code → 中文 */
const FIX_WH_LABEL = new Map([
  ['swap', '夾換品'],
  ['agency', '代夾物'],
  ['main', '總倉'],
  ['withdraw', '撤台'],
  ['unspecified', '未指定'],
]);

/** SKU 規一顯示 */
function skuKey(s) { return String(s || '').trim().toUpperCase(); }
function skuDisplay(s) {
  const t = String(s || '').trim();
  if (!t) return '';
  return t.slice(0,1).toUpperCase() + t.slice(1).toLowerCase();
}

/** 倉庫代碼/名稱 → 中文顯示名（先固定映射，再查 DB） */
async function resolveWarehouseLabel(codeOrName) {
  const key = String(codeOrName || '').trim();
  if (!key) return '未指定';
  if (FIX_WH_LABEL.has(key)) return FIX_WH_LABEL.get(key);
  if (WAREHOUSE_NAME_CACHE.has(key)) return WAREHOUSE_NAME_CACHE.get(key);
  try {
    let label = key;
    {
      const { data } = await supabase
        .from('inventory_warehouses')
        .select('code,name')
        .or(`code.eq.${key},name.eq.${key}`)
        .limit(1)
        .maybeSingle();
      if (data?.name) label = data.name;
    }
    if (label === key) {
      const { data } = await supabase
        .from('warehouse_kinds')
        .select('code,name')
        .or(`code.eq.${key},name.eq.${key}`)
        .limit(1)
        .maybeSingle();
      if (data?.name) label = data.name;
    }
    WAREHOUSE_NAME_CACHE.set(key, label);
    return label;
  } catch {
    return key;
  }
}

/** 中文顯示名 → code（找不到回 'unspecified'） */
async function getWarehouseCodeForLabel(displayName) {
  const label = String(displayName || '').trim();
  for (const [code, cn] of FIX_WH_LABEL.entries()) if (cn === label) return code;
  const { data } = await supabase
    .from('inventory_warehouses')
    .select('code,name')
    .or(`name.eq.${label},code.eq.${label}`)
    .limit(1)
    .maybeSingle();
  if (data?.code) return data.code;
  return 'unspecified';
}

/** 只查 line_user_map，把 LINE userId 轉成 auth.users.id (uuid) */
async function resolveAuthUuidFromLineUserId(lineUserId) {
  if (!lineUserId) return null;
  const { data, error } = await supabase
    .from('line_user_map')
    .select('auth_user_id')
    .eq('line_user_id', lineUserId)
    .maybeSingle();
  if (error) { console.warn('[resolveAuthUuid] line_user_map error:', error); return null; }
  return data?.auth_user_id || null;
}

/** 取得 branches.id（大小寫不敏感） */
async function getBranchIdByGroupCode(groupCode) {
  const key = String(groupCode || '').trim();
  if (!key) return null;
  const { data, error } = await supabase
    .from('branches')
    .select('id')
    .ilike('分店代號', key)
    .maybeSingle();
  if (error) throw error;
  return data?.id ?? null;
}

/** 指令解析 */
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
    /^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散|件))?(?:\s*(\d+))?(?:\s*(?:@|（?\(?倉庫[:：=]\s*)([^)）]+)\)?)?\s*$/
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
      warehouse: warehouse || null
    };
  }
  return null;
}

/** 解析分店與角色（保持原樣大小寫，不強制 lower） */
async function resolveBranchAndRole(event) {
  const source = event.source || {};
  const userId = source.userId || null;
  const isGroup = source.type === 'group';
  const groupId = isGroup ? source.groupId : null;

  let role = 'user';
  let blocked = false;
  if (userId) {
    const { data: u } = await supabase.from('users').select('角色, 黑名單, 群組').eq('user_id', userId).maybeSingle();
    role = u?.角色 || 'user';
    blocked = !!u?.黑名單;
  }

  if (isGroup) {
    const { data: lg } = await supabase.from('line_groups').select('群組').eq('line_group_id', groupId).maybeSingle();
    const branch = lg?.群組 || null;
    return { branch, role, blocked, needBindMsg: '此群組尚未綁定分店，請管理員設定' };
  } else {
    const { data: u2 } = await supabase.from('users').select('群組').eq('user_id', userId).maybeSingle();
    const branch = u2?.群組 || null;
    return { branch, role, blocked, needBindMsg: '此使用者尚未綁定分店，請管理員設定' };
  }
}

async function autoRegisterUser(lineUserId) {
  if (!lineUserId) return;
  const { data } = await supabase.from('users').select('user_id').eq('user_id', lineUserId).maybeSingle();
  if (!data) {
    await supabase.from('users').insert({ user_id: lineUserId, 群組: DEFAULT_GROUP, 角色: 'user', 黑名單: false });
  }
}

/** 最後選取的貨號（DB 不改 schema） */
async function upsertUserLastProduct(lineUserId, branch, sku) {
  if (!lineUserId) return;
  const now = new Date().toISOString();
  const { data } = await supabase.from('user_last_product').select('id').eq('user_id', lineUserId).eq('群組', branch).maybeSingle();
  if (data) {
    await supabase.from('user_last_product').update({ '貨品編號': sku, '建立時間': now }).eq('user_id', lineUserId).eq('群組', branch);
  } else {
    await supabase.from('user_last_product').insert({ user_id: lineUserId, 群組: branch, '貨品編號': sku, '建立時間': now });
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

/** ================== 查詢與庫存：改用 lots，不分大小寫 ================== */
async function getInStockSkuSet(branch) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return new Set();
  const { data, error } = await supabase
    .from('inventory_lots')
    .select('product_sku, qty_left')
    .eq('branch_id', branchId)
    .gt('qty_left', 0);
  if (error) throw error;
  const set = new Set();
  (data || []).forEach(r => { if (Number(r.qty_left || 0) > 0) set.add(String(r.product_sku || '').toUpperCase()); });
  return set;
}

async function getStockByGroupSku(branch, sku) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box: 0, piece: 0 };
  const { data, error } = await supabase
    .from('inventory_lots')
    .select('uom,qty_left')
    .eq('branch_id', branchId)
    .ilike('product_sku', sku);
  if (error) throw error;
  let box = 0, piece = 0;
  (data || []).forEach(r => {
    const u = String(r.uom || '').toLowerCase();
    const q = Number(r.qty_left || 0);
    if (u === 'box') box += q;
    if (u === 'piece') piece += q;
  });
  return { box, piece };
}

async function getWarehouseStockBySku(branch, sku) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return [];
  const { data, error } = await supabase
    .from('inventory_lots')
    .select('warehouse_name, uom, qty_left')
    .eq('branch_id', branchId)
    .ilike('product_sku', sku);
  if (error) throw error;

  const map = new Map();
  (data || []).forEach(r => {
    const raw = String(r.warehouse_name || '未指定');
    const u = String(r.uom || '').toLowerCase();
    const q = Number(r.qty_left || 0);
    if (!map.has(raw)) map.set(raw, { box:0, piece:0 });
    const obj = map.get(raw);
    if (u === 'box') obj.box += q; else if (u === 'piece') obj.piece += q;
  });

  const list = [];
  for (const [raw, v] of map.entries()) {
    if (v.box > 0 || v.piece > 0) {
      const label = await resolveWarehouseLabel(raw);
      list.push({ warehouse: label, box: v.box, piece: v.piece, _raw: raw });
    }
  }
  return list;
}

async function warehouseCandidates(warehouseDisplayName) {
  const label = await resolveWarehouseLabel(warehouseDisplayName);
  const back = new Set([label]);
  for (const [code, cn] of FIX_WH_LABEL.entries()) if (cn === label) back.add(code);
  const { data } = await supabase.from('inventory_warehouses').select('code,name').or(`name.eq.${label},code.eq.${label}`).limit(1).maybeSingle();
  if (data?.code) back.add(data.code);
  if (data?.name) back.add(data.name);
  return Array.from(back);
}

async function getWarehouseStockForSku(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box: 0, piece: 0 };
  const candidates = await warehouseCandidates(warehouseDisplayName);
  const { data, error } = await supabase
    .from('inventory_lots')
    .select('warehouse_name, uom, qty_left')
    .eq('branch_id', branchId)
    .ilike('product_sku', sku)
    .in('warehouse_name', candidates);
  if (error) throw error;

  let box = 0, piece = 0;
  (data || []).forEach(r => {
    const u = String(r.uom || '').toLowerCase();
    const q = Number(r.qty_left || 0);
    if (u === 'box') box += q;
    else if (u === 'piece') piece += q;
  });
  return { box, piece };
}

/** lots 快照（庫存箱/件、庫存總額=lots分層加總、顯示單價=最新仍有量(散)、箱入數、品名） */
async function getWarehouseSnapshotFromLots(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box:0, piece:0, stockAmount:0, displayUnitCost:0, unitsPerBox:1, productName: sku };

  const candidates = await warehouseCandidates(warehouseDisplayName);

  const { data: prod } = await supabase
    .from('products')
    .select('貨品名稱, 箱入數')
    .ilike('貨品編號', sku)
    .maybeSingle();
  const unitsPerBox = Number(prod?.['箱入數'] || 1) || 1;

  const { data, error } = await supabase
    .from('inventory_lots')
    .select('uom,qty_left,unit_cost,created_at,warehouse_name,product_sku')
    .eq('branch_id', branchId)
    .ilike('product_sku', sku)
    .in('warehouse_name', candidates);
  if (error) throw error;

  let box=0, piece=0, amount=0;
  let displayUnitCost = 0, latestTs = 0;
  (data || []).forEach(r=>{
    const u = String(r.uom||'').toLowerCase();
    const q = Number(r.qty_left||0);
    const c = Number(r.unit_cost||0);
    const ts = new Date(r.created_at || 0).getTime();
    if (u === 'box') box += q; else if (u === 'piece') piece += q;
    const pieces = (u === 'box') ? (q * unitsPerBox) : q;
    amount += pieces * c;
    if (q > 0 && ts >= latestTs) { latestTs = ts; displayUnitCost = c; }
  });

  return {
    box, piece,
    stockAmount: Math.max(0, amount),
    displayUnitCost,
    unitsPerBox,
    productName: prod?.['貨品名稱'] || sku
  };
}

async function getWarehouseDisplayUnitCost(branch, sku, warehouseDisplayName) {
  const snap = await getWarehouseSnapshotFromLots(branch, sku, warehouseDisplayName);
  return snap.displayUnitCost || 0;
}

/** FIFO 出庫（回 consumed 與 cost 單價(散)） */
async function callFifoOutLots(branch, sku, uom, qty, warehouseDisplayName, lineUserId) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error(`找不到對應使用者（${lineUserId}）`);
  if (qty <= 0) return { consumed: 0, cost: 0 };

  const candidates = await warehouseCandidates(warehouseDisplayName);
  const whRaw = candidates[0] || warehouseDisplayName;

  const { data, error } = await supabase.rpc('fifo_out_lots', {
    p_group: String(branch || '').trim(),
    p_sku: skuKey(sku),
    p_uom: uom,                 // 'box' | 'piece'
    p_qty: qty,
    p_warehouse_name: whRaw || '',
    p_user_id: authUuid,
    p_source: 'LINE',
    p_now: new Date().toISOString()
  });
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  return { consumed: Number(row?.consumed || 0), cost: Number(row?.cost || 0) };
}

/** ===== GAS Webhook 設定 ===== */
let GAS_URL_CACHE = (ENV_GAS_URL || '').trim();
let GAS_SECRET_CACHE = (ENV_GAS_SECRET || '').trim();
let GAS_LOADED_ONCE = false;

async function loadGasConfigFromDBIfNeeded() {
  if (GAS_URL_CACHE && GAS_SECRET_CACHE) { GAS_LOADED_ONCE = true; return; }
  try {
    const { data, error } = await supabase.rpc('get_app_settings', { keys: ['gas_webhook_url', 'gas_webhook_secret'] });
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
    console.warn('⚠️ 載入 GAS 設定失敗：', e?.message || e);
  }
}
async function getGasConfig() {
  if (!GAS_LOADED_ONCE || !GAS_URL_CACHE || !GAS_SECRET_CACHE) await loadGasConfigFromDBIfNeeded();
  return { url: GAS_URL_CACHE, secret: GAS_SECRET_CACHE };
}

/** 台北時間 ISO */
function formatTpeIso(date = new Date()) {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
  }).format(date);
  return s.replace(' ', 'T') + '+08:00';
}

/** 推送 GAS（type=log → GAS 端以 RPC 覆蓋當日分頁） */
let GAS_WARNED_MISSING = false;
async function postInventoryToGAS(payload) {
  const { url, secret } = await getGasConfig();
  if (!url || !secret) {
    if (!GAS_WARNED_MISSING) { console.warn('⚠️ 未設定 GAS_WEBHOOK_URL/SECRET（略過推送）'); GAS_WARNED_MISSING = true; }
    return;
  }
  const callUrl = `${url.replace(/\?+.*/, '')}?secret=${encodeURIComponent(secret)}`;
  try {
    const res = await fetch(callUrl, { method: 'POST', headers: { 'Content-Type':'application/json' }, body: JSON.stringify(payload) });
    if (!res.ok) console.warn('[GAS PUSH WARN]', res.status, await res.text().catch(()=> ''));
  } catch (e) { console.warn('[GAS PUSH ERROR]', e); }
}

/** 產品 quick reply */
function buildQuickReplyForProducts(products) {
  const items = products.slice(0, 12).map(p => ({
    type: 'action',
    action: { type: 'message', label: `${p['貨品名稱']}`.slice(0, 20), text: `編號 ${p['貨品編號']}` }
  }));
  return { items };
}
function buildQuickReplyForWarehousesForQuery(warehouseList) {
  const items = warehouseList.slice(0, 12).map(w => {
    const label = `${w.warehouse}（${w.box}箱/${w.piece}件）`.slice(0, 20);
    const text = `倉 ${w.warehouse}`;
    return { type: 'action', action: { type: 'message', label, text } };
  });
  return { items };
}
function buildQuickReplyForWarehouses(baseText, warehouseList, wantBox, wantPiece) {
  const items = warehouseList.slice(0, 12).map(w => {
    const label = `${w.warehouse}（${w.box}箱/${w.piece}散）`.slice(0, 20);
    const text = `${baseText} ${wantBox>0?`${wantBox}箱 `:''}${wantPiece>0?`${wantPiece}件 `:''}@${w.warehouse}`;
    return { type: 'action', action: { type: 'message', label, text: text.trim() } };
  });
  return { items };
}

function logEventSummary(event) {
  try {
    const src = event && event.source ? event.source : {};
    const msg = event && event.message ? event.message : {};
    const isGroup = src.type === 'group';
    const isRoom = src.type === 'room';
    const groupId = isGroup ? src.groupId : null;
    const roomId = isRoom ? src.roomId : null;
    const userId = src.userId || null;
    const text = msg && msg.type === 'text' ? msg.text : '';
    console.log(`[LINE EVENT] type=${event && event.type} source=${src.type || '-'} groupId=${groupId || '-'} roomId=${roomId || '-'} userId=${userId || '-'} text="${text}"`);
  } catch (e) {
    console.error('[LINE EVENT LOG ERROR]', e);
  }
}

/** ====== 寫入 logs（出庫事件；箱對箱/散對散；金額/庫存即時） ====== */
async function recordLogOut({
  branch, sku, warehouseLabel,
  qtyBox = 0, qtyPiece = 0,
  unitPricePiece = 0,
  stockBoxAfter = 0, stockPieceAfter = 0, stockAmountAfter = 0,
  createdBy = 'linebot', refTable = 'linebot', refId = null, lineUserId = null
}) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) throw new Error('recordLogOut: 無法解析分店 ID');
  const warehouseCode = await getWarehouseCodeForLabel(warehouseLabel);

  const { data: prod } = await supabase.from('products').select('貨品名稱, 箱入數').ilike('貨品編號', sku).maybeSingle();
  const prodName = (prod && prod['貨品名稱']) || sku;
  const unitsPerBox = Number((prod && prod['箱入數']) || 1) || 1;

  const piecesOut = (qtyBox * unitsPerBox) + qtyPiece;
  const outAmount = Math.max(0, piecesOut * Number(unitPricePiece || 0));

  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);

  const row = {
    '貨品編號': skuKey(sku),
    '貨品名稱': prodName,
    '入庫箱數': '0',
    '入庫散數': 0,
    '出庫箱數': String(qtyBox || 0),
    '出庫散數': String(qtyPiece || 0),
    '庫存箱數': String(stockBoxAfter || 0),
    '庫存散數': String(stockPieceAfter || 0),
    '出庫金額': String(outAmount || 0),
    '入庫金額': '0',
    '庫存金額': String(Math.max(0, stockAmountAfter || 0)),
    '建立時間': new Date().toISOString(),
    '群組': String(branch || '').trim(),
    '操作來源': createdBy,
    'user_id': authUuid || null,
    '倉庫別': warehouseLabel,
    '倉庫代碼': warehouseCode
  };

  const { error } = await supabase.from('inventory_logs').insert([row]);
  if (error) throw error;

  return { outAmount, unitsPerBox, prodName };
}

/** ====== 搜尋（名稱 / 條碼 / 指定貨號） ====== */
async function searchByName(keyword, _role, branch, _unused) {
  const k = String(keyword || '').trim();
  const { data, error } = await supabase.from('products').select('貨品名稱, 貨品編號, 箱入數, 單價').ilike('貨品名稱', `%${k}%`).limit(20);
  if (error) throw error;
  const set = await getInStockSkuSet(branch);
  return (data || []).filter(p => set.has(String(p['貨品編號']).toUpperCase())).slice(0, 10);
}
async function searchByBarcode(barcode, _role, branch, _unused) {
  const b = String(barcode || '').trim();
  const { data, error } = await supabase.from('products').select('貨品名稱, 貨品編號, 箱入數, 單價').eq('條碼', b).maybeSingle();
  if (error) throw error;
  if (!data) return [];
  const set = await getInStockSkuSet(branch);
  if (!set.has(String(data['貨品編號']).toUpperCase())) return [];
  return [data];
}
async function searchBySku(sku, _role, branch, _unused) {
  const s = String(sku || '').trim();
  const set = await getInStockSkuSet(branch);

  const { data: exact, error: e1 } = await supabase.from('products').select('貨品名稱, 貨品編號, 箱入數, 單價').ilike('貨品編號', s).maybeSingle();
  if (e1) throw e1;
  if (exact && set.has(String(exact['貨品編號']).toUpperCase())) return [exact];

  const { data: like, error: e2 } = await supabase.from('products').select('貨品名稱, 貨品編號, 箱入數, 單價').ilike('貨品編號', `%${s}%`).limit(20);
  if (e2) throw e2;
  const list = (like || []).filter(p => set.has(String(p['貨品編號']).toUpperCase()));
  return list.slice(0, 10);
}

/** ====== 指令主處理 ====== */
async function handleEvent(event) {
  if (!event || event.type !== 'message' || !event.message || event.message.type !== 'text') return;
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
    await client.replyMessage(event.replyToken, { type: 'text', text: needBindMsg || '尚未分店綁定，請管理員設定' });
    return;
  }

  const reply = (messageObj) => client.replyMessage(event.replyToken, messageObj);
  const replyText = (textStr) => reply({ type: 'text', text: textStr });

  const inStockSet = await getInStockSkuSet(branch);

  if (parsed.type === 'wh_select') {
    const sku = await getLastSku(lineUserId, branch);
    if (!sku || !inStockSet.has(String(sku).toUpperCase())) { await replyText('請先選擇商品（查 / 條碼 / 編號）後再選倉庫'); return; }

    const wh = await resolveWarehouseLabel(parsed.warehouse);
    LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);

    const { data: prodRow } = await supabase.from('products').select('貨品名稱, 箱入數').ilike('貨品編號', sku).maybeSingle();
    const prodName = (prodRow && prodRow['貨品名稱']) || sku;
    const boxSize = (prodRow && prodRow['箱入數']) ?? '-';
    const unitPrice = await getWarehouseDisplayUnitCost(branch, sku, wh);
    const { box, piece } = await getWarehouseStockForSku(branch, sku, wh);

    await replyText(`品名：${prodName}\n編號：${sku}\n箱入數：${boxSize}\n單價：${unitPrice}\n庫存：${box}箱${piece}散`);
    return;
  }

  if (parsed.type === 'query') {
    const list = await searchByName(parsed.keyword, role, branch, inStockSet);
    if (!list.length) { await replyText('無此商品庫存'); return; }
    if (list.length > 1) {
      const whList = await getWarehouseStockBySku(branch, list[0]['貨品編號']);
      await reply({ type: 'text', text: `找到以下與「${parsed.keyword}」相關的選項`, quickReply: buildQuickReplyForWarehousesForQuery(whList) });
      return;
    }
    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (s.box === 0 && s.piece === 0) { await replyText('無此商品庫存'); return; }
    await upsertUserLastProduct(lineUserId, branch, sku);

    const whList = await getWarehouseStockBySku(branch, sku);
    if (whList.length >= 2) {
      await reply({ type: 'text', text: `名稱：${p['貨品名稱']}\n編號：${sku}\n👉請選擇倉庫`, quickReply: buildQuickReplyForWarehousesForQuery(whList) });
      return;
    }
    if (whList.length === 1) {
      const wh = whList[0].warehouse;
      LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);
      const unitPrice = await getWarehouseDisplayUnitCost(branch, sku, wh);
      const { box, piece } = await getWarehouseStockForSku(branch, sku, wh);
      const boxSize = p['箱入數'] ?? '-';
      await replyText(`名稱：${p['貨品名稱']}\n編號：${sku}\n箱入數：${boxSize}\n單價：${unitPrice}\n倉庫類別：${wh}\n庫存：${box}箱${piece}散`);
      return;
    }
    await replyText('無此商品庫存');
    return;
  }

  if (parsed.type === 'barcode') {
    const list = await searchByBarcode(parsed.barcode, role, branch, inStockSet);
    if (!list.length) { await replyText('無此商品庫存'); return; }
    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (s.box === 0 && s.piece === 0) { await replyText('無此商品庫存'); return; }
    await upsertUserLastProduct(lineUserId, branch, sku);

    const whList = await getWarehouseStockBySku(branch, sku);
    if (whList.length >= 2) {
      await reply({ type: 'text', text: `名稱：${p['貨品名稱']}\n編號：${sku}\n👉請選擇倉庫`, quickReply: buildQuickReplyForWarehousesForQuery(whList) });
      return;
    }
    if (whList.length === 1) {
      const wh = whList[0].warehouse;
      LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);
      const unitPrice = await getWarehouseDisplayUnitCost(branch, sku, wh);
      const { box, piece } = await getWarehouseStockForSku(branch, sku, wh);
      const boxSize = p['箱入數'] ?? '-';
      await replyText(`名稱：${p['貨品名稱']}\n編號：${sku}\n箱入數：${boxSize}\n單價：${unitPrice}\n倉庫類別：${wh}\n庫存：${box}箱${piece}散`);
      return;
    }
    await replyText('無此商品庫存');
    return;
  }

  if (parsed.type === 'sku') {
    const list = await searchBySku(parsed.sku, role, branch, inStockSet);
    if (!list.length) { await replyText('無此商品庫存'); return; }
    if (list.length > 1) {
      await reply({ type: 'text', text: `找到以下與「${parsed.sku}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
      return;
    }
    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (s.box === 0 && s.piece === 0) { await replyText('無此商品庫存'); return; }
    await upsertUserLastProduct(lineUserId, branch, sku);

    const whList = await getWarehouseStockBySku(branch, sku);
    if (whList.length >= 2) {
      await reply({ type: 'text', text: `🍀品名：${p['貨品名稱']}\n編號：${sku}\n👉請選擇倉庫`, quickReply: buildQuickReplyForWarehousesForQuery(whList) });
      return;
    }
    if (whList.length === 1) {
      const wh = whList[0].warehouse;
      LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);
      const unitPrice = await getWarehouseDisplayUnitCost(branch, sku, wh);
      const { box, piece } = await getWarehouseStockForSku(branch, sku, wh);
      const boxSize = p['箱入數'] ?? '-';
      await replyText(`🍀品名：${p['貨品名稱']}\n編號：${sku}\n箱入數：${boxSize}\n單價：${unitPrice}\n倉庫類別：${wh}\n庫存：${box}箱${piece}散`);
      return;
    }
    await replyText('無此商品庫存');
    return;
  }

  // 入/出庫
  if (parsed.type === 'change') {
    if (parsed.action === 'in' && role !== '主管') { await replyText('您無法使用「入庫」'); return; }
    if (parsed.box === 0 && parsed.piece === 0) return;

    const skuLast = await getLastSku(lineUserId, branch);
    if (!skuLast || !inStockSet.has(String(skuLast).toUpperCase())) {
      await replyText('請先用「查 商品」或「條碼123 / 編號ABC」選定「有庫存」商品後再入/出庫。');
      return;
    }

    if (parsed.action === 'out') {
      if (!parsed.warehouse) {
        const remembered = LAST_WAREHOUSE_BY_USER_BRANCH.get(`${lineUserId}::${branch}`) || null;
        if (remembered) parsed.warehouse = remembered;
        else {
          const list = await getWarehouseStockBySku(branch, skuLast);
          if (list.length >= 2) {
            const qr = buildQuickReplyForWarehouses('出', list, parsed.box, parsed.piece);
            await reply({ type: 'text', text: '請選擇要出庫的倉庫', quickReply: qr });
            return;
          }
          if (list.length === 1) parsed.warehouse = list[0].warehouse;
        }
      }
    }

    try {
      if (parsed.action === 'out') {
        const wh = await resolveWarehouseLabel(parsed.warehouse || '未指定');
        LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);

        // 出庫前防呆：庫存不足（以 lots 快照檢查）
        const beforeSnap = await getWarehouseSnapshotFromLots(branch, skuLast, wh);
        const needPieces = (parsed.box > 0 ? parsed.box * (beforeSnap.unitsPerBox || 1) : 0) + (parsed.piece || 0);
        const hasPieces = beforeSnap.box * (beforeSnap.unitsPerBox || 1) + beforeSnap.piece;
        if (needPieces > hasPieces) { await replyText(`庫存不足：該倉僅有 ${beforeSnap.box}箱${beforeSnap.piece}件`); return; }

        // FIFO 成本（箱與散各自扣）
        let fifoCostUnitPieceBox = 0, fifoConsumedBox = 0;
        let fifoCostUnitPiecePiece = 0, fifoConsumedPiece = 0;

        if (parsed.box > 0) {
          const rBox = await callFifoOutLots(branch, skuLast, 'box', parsed.box, wh, lineUserId);
          fifoCostUnitPieceBox = Number(rBox.cost || 0);
          fifoConsumedBox = Number(rBox.consumed || 0) * (beforeSnap.unitsPerBox || 1);
        }
        if (parsed.piece > 0) {
          const rPiece = await callFifoOutLots(branch, skuLast, 'piece', parsed.piece, wh, lineUserId);
          fifoCostUnitPiecePiece = Number(rPiece.cost || 0);
          fifoConsumedPiece = Number(rPiece.consumed || 0);
        }

        const displayUnitPrice = await getWarehouseDisplayUnitCost(branch, skuLast, wh);
        const afterSnap = await getWarehouseSnapshotFromLots(branch, skuLast, wh);

        const outAmount = fifoCostUnitPieceBox * fifoConsumedBox + fifoCostUnitPiecePiece * fifoConsumedPiece;

        await recordLogOut({
          branch,
          sku: skuLast,
          warehouseLabel: wh,
          qtyBox: parsed.box || 0,
          qtyPiece: parsed.piece || 0,
          unitPricePiece: displayUnitPrice, // 紀錄用；實際出庫金額 outAmount 已用 FIFO 單價計算
          stockBoxAfter: afterSnap.box,
          stockPieceAfter: afterSnap.piece,
          stockAmountAfter: afterSnap.stockAmount, // L 欄對應：lots 金額加總
          createdBy: 'LINE',
          refTable: 'linebot',
          refId: event.message && event.message.id ? event.message.id : null,
          lineUserId
        });

        const payload = {
          type: 'log',
          group: String(branch || '').trim().toLowerCase(),
          sku: skuDisplay(skuLast),
          name: afterSnap.productName || skuLast,
          units_per_box: afterSnap.unitsPerBox || 1,
          unit_price: displayUnitPrice,
          in_box: 0,
          in_piece: 0,
          out_box: parsed.box,
          out_piece: parsed.piece,
          stock_box: afterSnap.box,
          stock_piece: afterSnap.piece,
          out_amount: outAmount,               // K：出庫總額（FIFO）
          stock_amount: afterSnap.stockAmount, // L：庫存總額（lots 金額加總）
          warehouse: wh,
          created_at: formatTpeIso(new Date())
        };
        postInventoryToGAS(payload).catch(()=>{});

        await replyText(`✅ 出庫成功\n品名：${afterSnap.productName}\n倉別：${wh}\n出庫：${parsed.box || 0}箱 ${parsed.piece || 0}件\n👉目前庫存：${afterSnap.box}箱${afterSnap.piece}散`);
        return;
      }

      await replyText('入庫請改用 App 進行；LINE 目前僅提供出庫');
      return;

    } catch (err) {
      console.error('change error:', err);
      await replyText(`操作失敗：${err && err.message ? err.message : '未知錯誤'}`);
      return;
    }
  }
}

/** ===== LINE webhook 處理器（需放在 routes 掛載前）===== */
async function lineHandler(req, res) {
  try {
    const events = (req.body && req.body.events) ? req.body.events : [];
    for (const ev of events) {
      logEventSummary(ev);
      try { await handleEvent(ev); } catch (err) { console.error('[HANDLE EVENT ERROR]', err); }
    }
    return res.status(200).send('OK');
  } catch (e) {
    console.error('[WEBHOOK ERROR]', e);
    return res.status(500).send('ERR');
  }
}

/** ===== Routes（⚠️ 一定要在任何 parser 之前）===== */
const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));
app.get('/webhook', (_req, res) => res.status(200).send('OK'));
app.get('/line/webhook', (_req, res) => res.status(200).send('OK'));
app.post('/webhook',      line.middleware(lineConfig), lineHandler);
app.post('/line/webhook', line.middleware(lineConfig), lineHandler);

// （若未來要掛 JSON API，請放在 webhook 後面再加 parser）
// app.use('/api', express.json());
// app.post('/api/something', (req, res) => res.json({ ok: true }));

app.listen(PORT, () => { console.log(`Server running on port ${PORT}`); });
