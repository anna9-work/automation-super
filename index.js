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

if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) {
  console.error('缺少 LINE 環境變數');
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('缺少 Supabase 環境變數 (URL / SERVICE_ROLE_KEY)');
}

const app = express();
// ⚠️ 不要在 webhook 前面加任何 body parser！
app.use((req, _res, next) => {
  console.log(`[請求] ${req.method} ${req.path} ua=${req.headers['user-agent'] || ''} x-line-signature=${req.headers['x-line-signature'] ? 'yes' : 'no'}`);
  next();
});

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

/** ===== 常數 / 快取 ===== */
const LAST_WAREHOUSE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> 中文名
const WAREHOUSE_NAME_CACHE = new Map(); // codeOrName -> 中文名

// ★ 你確認的 lots 實際名稱對照（單一來源真相）
const LOTS_WAREHOUSE_MAP = {
  '總倉': 'main_warehouse',
  '撤台': 'withdraw',
  '代夾物': 'prize',
  '夾換品': 'swap',
  '未指定': 'unspecified',
};

/** SKU 規一 */
function skuKey(s) { return String(s || '').trim().toUpperCase(); }
function skuDisplay(s) { const t = String(s || '').trim(); return t ? t.slice(0,1).toUpperCase() + t.slice(1).toLowerCase() : ''; }

/** 台北時間 ISO */
function formatTpeIso(date = new Date()) {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(date);
  return s.replace(' ', 'T') + '+08:00';
}

/** 倉庫：顯示字串→中文名（先看對照表、再查 DB） */
async function resolveWarehouseLabel(codeOrName) {
  const key = String(codeOrName || '').trim();
  if (!key) return '未指定';

  // 直接命中中文
  if (LOTS_WAREHOUSE_MAP[key]) return key;

  // 反查：如果傳進來是 lots 名，找中文鍵
  for (const [cn, lotsName] of Object.entries(LOTS_WAREHOUSE_MAP)) {
    if (lotsName === key) return cn;
  }

  if (WAREHOUSE_NAME_CACHE.has(key)) return WAREHOUSE_NAME_CACHE.get(key);

  // 退回查表 inventory_warehouses 或 warehouse_kinds（容錯）
  try {
    let label = key;
    const { data: iw } = await supabase
      .from('inventory_warehouses').select('code,name')
      .or(`code.eq.${key},name.eq.${key}`).limit(1).maybeSingle();
    if (iw?.name) label = iw.name;
    WAREHOUSE_NAME_CACHE.set(key, label);
    return label;
  } catch {
    return key;
  }
}

/** 把「中文或任何代稱」→ lots 實際 warehouse_name */
function toLotsWarehouseName(displayOrCode) {
  const inStr = String(displayOrCode || '').trim();
  if (!inStr) return LOTS_WAREHOUSE_MAP['未指定'];

  // 若是中文鍵
  if (LOTS_WAREHOUSE_MAP[inStr]) return LOTS_WAREHOUSE_MAP[inStr];

  // 若本身就是 lots 實際名
  for (const v of Object.values(LOTS_WAREHOUSE_MAP)) {
    if (v === inStr) return v;
  }

  // 常見代稱容錯
  const alias = {
    main: 'main_warehouse',
    withdraw_warehouse: 'withdraw',
    agency: 'prize',
    agency_warehouse: 'prize',
    swap_warehouse: 'swap',
    unspecified: 'unspecified',
    '': 'unspecified'
  };
  if (alias[inStr]) return alias[inStr];

  // 找不到就不轉
  return inStr;
}

/** 只查 line_user_map，把 LINE userId 轉 auth.users.id (uuid) */
async function resolveAuthUuidFromLineUserId(lineUserId) {
  if (!lineUserId) return null;
  const { data, error } = await supabase
    .from('line_user_map')
    .select('auth_user_id')
    .eq('line_user_id', lineUserId)
    .maybeSingle();
  if (error) { console.warn('[resolveAuthUuid] error:', error); return null; }
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

/** 使用者與群組 */
async function resolveBranchAndRole(event) {
  const source = event.source || {};
  const userId = source.userId || null;
  const isGroup = source.type === 'group';
  const groupId = isGroup ? source.groupId : null;

  let role = 'user';
  let blocked = false;
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
      .eq('line_group_id', groupId)
      .maybeSingle();
    const branch = lg?.群組 || null;
    return { branch, role, blocked, needBindMsg: '此群組尚未綁定分店，請管理員設定' };
  } else {
    const { data: u2 } = await supabase
      .from('users')
      .select('群組')
      .eq('user_id', userId)
      .maybeSingle();
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

/** 使用者最近選的商品 */
async function upsertUserLastProduct(lineUserId, branch, sku) {
  if (!lineUserId) return;
  const now = new Date().toISOString();
  const { data } = await supabase
    .from('user_last_product').select('id')
    .eq('user_id', lineUserId).eq('群組', branch).maybeSingle();
  if (data) {
    await supabase.from('user_last_product')
      .update({ '貨品編號': sku, '建立時間': now })
      .eq('user_id', lineUserId).eq('群組', branch);
  } else {
    await supabase.from('user_last_product')
      .insert({ user_id: lineUserId, 群組: branch, '貨品編號': sku, '建立時間': now });
  }
}
async function getLastSku(lineUserId, branch) {
  const { data, error } = await supabase
    .from('user_last_product').select('貨品編號')
    .eq('user_id', lineUserId).eq('群組', branch)
    .order('建立時間', { ascending: false }).limit(1).maybeSingle();
  if (error) throw error;
  return data?.['貨品編號'] || null;
}

/** ===== 查庫存（沿用 inventory 彙總，不參與箱↔散換算） ===== */
async function getStockByGroupSku(branch, sku) {
  const { data, error } = await supabase
    .from('inventory').select('庫存箱數, 庫存散數')
    .eq('群組', branch).eq('貨品編號', sku).maybeSingle();
  if (error) throw error;
  return { box: Number(data?.['庫存箱數'] ?? 0), piece: Number(data?.['庫存散數'] ?? 0) };
}
async function getInStockSkuSet(branch) {
  const { data, error } = await supabase
    .from('inventory').select('貨品編號, 庫存箱數, 庫存散數')
    .eq('群組', branch);
  if (error) throw error;
  const set = new Set();
  (data || []).forEach(r => {
    const b = Number(r['庫存箱數'] || 0);
    const p = Number(r['庫存散數'] || 0);
    if (b > 0 || p > 0) set.add(r['貨品編號']);
  });
  return set;
}

/** 產品查詢（僅回有庫存品） */
async function searchByName(keyword, _role, _branch, inStockSet) {
  const k = String(keyword || '').trim();
  const { data, error } = await supabase
    .from('products').select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品名稱', `%${k}%`).limit(20);
  if (error) throw error;
  return (data || []).filter(p => inStockSet.has(p['貨品編號'])).slice(0, 10);
}
async function searchByBarcode(barcode, _role, _branch, inStockSet) {
  const b = String(barcode || '').trim();
  const { data, error } = await supabase
    .from('products').select('貨品名稱, 貨品編號, 箱入數, 單價')
    .eq('條碼', b).maybeSingle();
  if (error) throw error;
  if (!data) return [];
  return inStockSet.has(data['貨品編號']) ? [data] : [];
}
async function searchBySku(sku, _role, _branch, inStockSet) {
  const s = String(sku || '').trim().toUpperCase();
  const { data: exact } = await supabase
    .from('products').select('貨品名稱, 貨品編號, 箱入數, 單價')
    .eq('貨品編號', s).maybeSingle();
  if (exact && inStockSet.has(exact['貨品編號'])) return [exact];
  const { data: like } = await supabase
    .from('products').select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品編號', `%${s}%`).limit(20);
  return (like || []).filter(p => inStockSet.has(p['貨品編號'])).slice(0, 10);
}

/** 依 SKU 匯總各倉現量（lots）→ 回中文倉名 */
async function getWarehouseStockBySku(branch, sku) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return [];
  const { data, error } = await supabase
    .from('inventory_lots').select('warehouse_name, uom, qty_left')
    .eq('branch_id', branchId).eq('product_sku', sku);
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

/** 指定倉之現量（箱/件） */
async function getWarehouseStockForSku(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box: 0, piece: 0 };
  const lotsName = toLotsWarehouseName(warehouseDisplayName);
  const { data, error } = await supabase
    .from('inventory_lots').select('uom, qty_left')
    .eq('branch_id', branchId).eq('product_sku', sku).eq('warehouse_name', lotsName);
  if (error) throw error;
  let box = 0, piece = 0;
  (data || []).forEach(r => {
    const u = String(r.uom || '').toLowerCase();
    const q = Number(r.qty_left || 0);
    if (u === 'box') box += q; else if (u === 'piece') piece += q;
  });
  return { box, piece };
}

/** 查 lots 並「用 unit_cost 重算」：庫存箱/件、庫存總額、顯示單價、箱入數 */
async function getWarehouseSnapshotFromLots(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box:0, piece:0, stockAmount:0, displayUnitCost:0, unitsPerBox:1 };

  const lotsName = toLotsWarehouseName(warehouseDisplayName);

  const { data: prod } = await supabase
    .from('products').select('箱入數').eq('貨品編號', sku).maybeSingle();
  const unitsPerBox = Number(prod?.['箱入數'] || 1) || 1;

  const { data, error } = await supabase
    .from('inventory_lots').select('uom,qty_left,unit_cost,created_at')
    .eq('branch_id', branchId).eq('product_sku', sku).eq('warehouse_name', lotsName);
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

  return { box, piece, stockAmount: amount, displayUnitCost, unitsPerBox };
}

/** 指定倉庫 lots 顯示單價（最新且仍有量） */
async function getWarehouseDisplayUnitCost(branch, sku, warehouseDisplayName) {
  const snap = await getWarehouseSnapshotFromLots(branch, sku, warehouseDisplayName);
  return snap.displayUnitCost || 0;
}

/** Quick Reply */
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

/** FIFO 出庫（各自扣箱/扣散；不互轉） */
async function callFifoOutLots(branch, sku, uom, qty, warehouseDisplayName, lineUserId) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error('找不到對應的使用者（請先建 line_user_map）');
  if (qty <= 0) return { consumed: 0, cost: 0 };

  const lotsWhName = toLotsWarehouseName(warehouseDisplayName);

  const { data, error } = await supabase.rpc('fifo_out_lots', {
    p_group: branch,
    p_sku: skuKey(sku),
    p_uom: uom,                  // 'box' 或 'piece'
    p_qty: qty,
    p_warehouse_name: lotsWhName, // ★ lots 實際字串
    p_user_id: authUuid,
    p_source: 'LINE',
    p_now: new Date().toISOString()
  });
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  return { consumed: Number(row?.consumed || 0), cost: Number(row?.cost || 0) };
}

/** 聚合（群組+SKU） */
async function changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, source = 'LINE') {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error('找不到對應的使用者（請先建 line_user_map）');
  const { data, error } = await supabase.rpc('exec_change_inventory_by_group_sku', {
    p_group: branch,
    p_sku: sku,
    p_delta_box: deltaBox,
    p_delta_piece: deltaPiece,
    p_user_id: authUuid,
    p_source: source
  });
  if (error) throw error;
  return Array.isArray(data) ? data[0] : data;
}

/** ledger: OUT（箱/散各寫，不互轉） */
async function recordLedgerOut({
  branch, sku, warehouseLabel, qtyBox, qtyPiece,
  createdBy = 'linebot', refTable = 'linebot', refId = null
}) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) throw new Error('recordLedgerOut: 無法解析分店 ID');
  const warehouseKind = toLotsWarehouseName(warehouseLabel); // 直接寫 lots 名稱

  const row = {
    branch_id: Number(branchId),
    product_sku: skuKey(sku),
    warehouse_kind: warehouseKind,
    movement: 'OUT',
    qty_box: Number(qtyBox || 0),     // ★ 新欄位（你已建立）
    qty_piece: Number(qtyPiece || 0), // ★ 舊欄位
    unit_cost: null,                  // 成本由 fifo 決定，這裡不硬塞
    note: 'linebot out',
    ref_table: refTable,
    ref_id: refId,
    created_by: createdBy,
  };
  if (!row.branch_id || !row.product_sku || (row.qty_box <= 0 && row.qty_piece <= 0)) {
    throw new Error('recordLedgerOut: 參數不足或數量為 0');
  }
  const { error } = await supabase.from('inventory_ledger').insert([row]);
  if (error) throw error;
}

/** ===== GAS Webhook ===== */
let GAS_URL_CACHE = (ENV_GAS_URL || '').trim();
let GAS_SECRET_CACHE = (ENV_GAS_SECRET || '').trim();
let GAS_LOADED_ONCE = false;

async function loadGasConfigFromDBIfNeeded() {
  if (GAS_URL_CACHE && GAS_SECRET_CACHE) { GAS_LOADED_ONCE = true; return; }
  try {
    const { data, error } = await supabase
      .rpc('get_app_settings', { keys: ['gas_webhook_url', 'gas_webhook_secret'] });
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
  if (!GAS_LOADED_ONCE || !GAS_URL_CACHE || !GAS_SECRET_CACHE) {
    await loadGasConfigFromDBIfNeeded();
  }
  return { url: GAS_URL_CACHE, secret: GAS_SECRET_CACHE };
}
let GAS_WARNED_MISSING = false;
async function postInventoryToGAS(payload) {
  const { url, secret } = await getGasConfig();
  if (!url || !secret) {
    if (!GAS_WARNED_MISSING) {
      console.warn('⚠️ GAS_WEBHOOK_URL / GAS_WEBHOOK_SECRET 未設定（已略過推送到試算表）');
      GAS_WARNED_MISSING = true;
    }
    return;
  }
  const callUrl = `${url.replace(/\?+.*/, '')}?secret=${encodeURIComponent(secret)}`;
  try {
    const res = await fetch(callUrl, { method: 'POST', headers: { 'Content-Type':'application/json' }, body: JSON.stringify(payload) });
    if (!res.ok) {
      const txt = await res.text().catch(()=> '');
      console.warn('[GAS PUSH WARN]', res.status, txt);
    }
  } catch (e) {
    console.warn('[GAS PUSH ERROR]', e);
  }
}

/** 產品 quick reply */
function buildQuickReplyForProducts(products) {
  const items = products.slice(0, 12).map(p => ({
    type: 'action',
    action: { type: 'message', label: `${p['貨品名稱']}`.slice(0, 20), text: `編號 ${p['貨品編號']}` }
  }));
  return { items };
}

function logEventSummary(event) {
  try {
    const src = event?.source || {};
    const msg = event?.message || {};
    const isGroup = src.type === 'group';
    const isRoom = src.type === 'room';
    const groupId = isGroup ? src.groupId : null;
    const roomId = isRoom ? src.roomId : null;
    const userId = src.userId || null;
    const text = msg?.type === 'text' ? msg.text : '';
    console.log(`[LINE EVENT] type=${event?.type} source=${src.type || '-'} groupId=${groupId || '-'} roomId=${roomId || '-'} userId=${userId || '-'} text="${text}"`);
  } catch (e) { console.error('[LINE EVENT LOG ERROR]', e); }
}

app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));
const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };
app.get('/webhook', (_req, res) => res.status(200).send('OK'));
app.get('/line/webhook', (_req, res) => res.status(200).send('OK'));
app.post('/webhook',      line.middleware(lineConfig), lineHandler);
app.post('/line/webhook', line.middleware(lineConfig), lineHandler);

async function lineHandler(req, res) {
  try {
    const events = req.body?.events || [];
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

app.use((err, req, res, next) => {
  if (req.path === '/webhook' || req.path === '/line/webhook') {
    console.error('[LINE MIDDLEWARE ERROR]', err?.message || err);
    return res.status(400).end();
  }
  return next(err);
});

/** ====== 指令主處理 ====== */
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
    await client.replyMessage(event.replyToken, { type: 'text', text: needBindMsg || '尚未分店綁定，請管理員設定' });
    return;
  }

  const reply = (messageObj) => client.replyMessage(event.replyToken, messageObj);
  const replyText = (textStr) => reply({ type: 'text', text: textStr });

  // 一律只看有庫存品項
  const inStockSet = await getInStockSkuSet(branch);

  // === 倉庫選擇 ===
  if (parsed.type === 'wh_select') {
    const sku = await getLastSku(lineUserId, branch);
    if (!sku || !inStockSet.has(sku)) { await replyText('請先選擇商品（查 / 條碼 / 編號）後再選倉庫'); return; }

    const whLabel = await resolveWarehouseLabel(parsed.warehouse);
    LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whLabel);

    const { data: prodRow } = await supabase.from('products').select('貨品名稱, 箱入數').eq('貨品編號', sku).maybeSingle();
    const prodName = prodRow?.['貨品名稱'] || sku;
    const boxSize = prodRow?.['箱入數'] ?? '-';
    const unitPrice = await getWarehouseDisplayUnitCost(branch, sku, whLabel);
    const { box, piece } = await getWarehouseStockForSku(branch, sku, whLabel);

    await replyText(
      `品名：${prodName}\n` +
      `編號：${sku}\n` +
      `箱入數：${boxSize}\n` +
      `單價：${unitPrice}\n` +
      `庫存：${box}箱${piece}散`
    );
    return;
  }

  // === 查詢（名稱）===
  if (parsed.type === 'query') {
    const list = await searchByName(parsed.keyword, role, branch, inStockSet);
    if (!list.length) { await replyText('無此商品庫存'); return; }
    if (list.length > 1) {
      await reply({ type: 'text', text: `找到以下與「${parsed.keyword}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
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
      await replyText(
        `名稱：${p['貨品名稱']}\n` +
        `編號：${sku}\n` +
        `箱入數：${boxSize}\n` +
        `單價：${unitPrice}\n` +
        `倉庫類別：${wh}\n` +
        `庫存：${box}箱${piece}散`
      );
      return;
    }
    await replyText('無此商品庫存');
    return;
  }

  // === 條碼 ===
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
      await replyText(
        `名稱：${p['貨品名稱']}\n` +
        `編號：${sku}\n` +
        `箱入數：${boxSize}\n` +
        `單價：${unitPrice}\n` +
        `倉庫類別：${wh}\n` +
        `庫存：${box}箱${piece}散`
      );
      return;
    }
    await replyText('無此商品庫存');
    return;
  }

  // === 指定貨號 ===
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
      await replyText(
        `🍀品名：${p['貨品名稱']}\n` +
        `編號：${sku}\n` +
        `箱入數：${boxSize}\n` +
        `單價：${unitPrice}\n` +
        `倉庫類別：${wh}\n` +
        `庫存：${box}箱${piece}散`
      );
      return;
    }
    await replyText('無此商品庫存');
    return;
  }

  // === 入/出庫 ===
  if (parsed.type === 'change') {
    if (parsed.action === 'in' && role !== '主管') { await replyText('您無法使用「入庫」'); return; }
    if (parsed.box === 0 && parsed.piece === 0) return;

    const skuLast = await getLastSku(lineUserId, branch);
    if (!skuLast || !inStockSet.has(skuLast)) { await replyText('請先用「查 商品」或「條碼123 / 編號ABC」選定「有庫存」商品後再入/出庫。'); return; }

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
        const whLabel = await resolveWarehouseLabel(parsed.warehouse || '未指定');
        LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, whLabel);

        // 庫存檢查（不互轉）
        const beforeSnap = await getWarehouseSnapshotFromLots(branch, skuLast, whLabel);
        const unitsPerBox = beforeSnap.unitsPerBox || 1;
        const needPieces = (parsed.box > 0 ? parsed.box * unitsPerBox : 0) + (parsed.piece || 0);
        const hasPieces = beforeSnap.box * unitsPerBox + beforeSnap.piece;
        if (needPieces > hasPieces) {
          await replyText(`庫存不足：該倉僅有 ${beforeSnap.box}箱${beforeSnap.piece}件`);
          return;
        }

        // FIFO 成本（箱與散分開扣）
        let fifoCostTotal = 0;
        if (parsed.box > 0) {
          const rBox = await callFifoOutLots(branch, skuLast, 'box', parsed.box, whLabel, lineUserId);
          fifoCostTotal += Number(rBox.cost || 0);
        }
        if (parsed.piece > 0) {
          const rPiece = await callFifoOutLots(branch, skuLast, 'piece', parsed.piece, whLabel, lineUserId);
          fifoCostTotal += Number(rPiece.cost || 0);
        }

        // 聚合（群組+SKU）
        await changeInventoryByGroupSku(
          branch,
          skuLast,
          parsed.box > 0 ? -parsed.box : 0,
          parsed.piece > 0 ? -parsed.piece : 0,
          lineUserId,
          'LINE'
        );

        // 寫 ledger（箱/散各寫欄位，不互轉）
        await recordLedgerOut({
          branch,
          sku: skuLast,
          warehouseLabel: whLabel,
          qtyBox: parsed.box || 0,
          qtyPiece: parsed.piece || 0,
          createdBy: 'linebot',
          refTable: 'linebot',
          refId: event.message?.id || null,
        });

        // 出庫後快照
        const afterSnap = await getWarehouseSnapshotFromLots(branch, skuLast, whLabel);

        // 商品資訊
        const { data: prodRow } = await supabase.from('products').select('貨品名稱, 箱入數').eq('貨品編號', skuLast).maybeSingle();
        const prodName = prodRow?.['貨品名稱'] || skuLast;

        // 推 GAS（K=本次 FIFO 成本）
        const payload = {
          type: 'log',
          group: String(branch || '').trim().toLowerCase(),
          sku: skuDisplay(skuLast),
          name: prodName,
          units_per_box: afterSnap.unitsPerBox || 1,
          unit_price: afterSnap.displayUnitCost,
          in_box: 0,
          in_piece: 0,
          out_box: parsed.box,
          out_piece: parsed.piece,
          stock_box: afterSnap.box,
          stock_piece: afterSnap.piece,
          out_amount: fifoCostTotal,
          stock_amount: afterSnap.stockAmount,
          warehouse: whLabel,
          created_at: formatTpeIso(new Date())
        };
        postInventoryToGAS(payload).catch(()=>{});

        await replyText(
          `✅ 出庫成功\n` +
          `品名：${prodName}\n` +
          `倉別：${whLabel}\n` +
          `出庫：${parsed.box || 0}箱 ${parsed.piece || 0}件\n` +
          `👉目前庫存：${afterSnap.box}箱${afterSnap.piece}散`
        );
        return;
      }

      await replyText('入庫請改用 App 進行；LINE 僅提供出庫');
      return;

    } catch (err) {
      console.error('change error:', err);
      await replyText(`操作失敗：${err?.message || '未知錯誤'}`);
      return;
    }
  }
  return;
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
