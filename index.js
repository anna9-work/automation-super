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
// ⚠️ 不要在 webhook 前面加任何 body parser！(express.json/express.raw 皆不行)
app.use((req, _res, next) => {
  console.log(`[請求] ${req.method} ${req.path} ua=${req.headers['user-agent'] || ''} x-line-signature=${req.headers['x-line-signature'] ? 'yes' : 'no'}`);
  next();
});

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY);

/** ===== 進程記憶：最後選倉（不改 DB 結構） ===== */
const LAST_WAREHOUSE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> warehouseName(中文)

/** ===== 倉庫中文名快取 ===== */
const WAREHOUSE_NAME_CACHE = new Map(); // key=codeOrName -> 中文name

/** 固定倉庫映射：code → 中文 */
const FIX_WH_LABEL = new Map([
  ['swap', '夾換品'],
  ['agency', '代夾物'],
  ['main', '總倉'],
  ['withdraw', '撤台'],
  ['unspecified', '未指定'],
]);

/** SKU 規一：比對用大寫，顯示用首字大寫其餘小寫 */
function skuKey(s) { return String(s || '').trim().toUpperCase(); }
function skuDisplay(s) {
  const t = String(s || '').trim();
  if (!t) return '';
  return t.slice(0,1).toUpperCase() + t.slice(1).toLowerCase();
}

/** 倉庫代碼/名稱 → 中文顯示名（先固定映射，再查 inventory_warehouses/warehouse_kinds） */
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

/** 只查 line_user_map，把 LINE userId 轉成 auth.users.id (uuid) */
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

/** 取得 branches.id */
async function getBranchIdByGroupCode(groupCode) {
  const { data, error } = await supabase
    .from('branches')
    .select('id')
    .eq('分店代號', groupCode)
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

/** 解析分店與角色 */
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
    await supabase.from('users').insert({
      user_id: lineUserId,
      群組: DEFAULT_GROUP,
      角色: 'user',
      黑名單: false
    });
  }
}

/** 最後選取的貨號（DB 不改 schema） */
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
      .update({ '貨品編號': sku, '建立時間': now })
      .eq('user_id', lineUserId)
      .eq('群組', branch);
  } else {
    await supabase
      .from('user_last_product')
      .insert({ user_id: lineUserId, 群組: branch, '貨品編號': sku, '建立時間': now });
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

/** ✅ 聚合庫存（群組+SKU）— 改用 inventory_lots */
async function getStockByGroupSku(branch, sku) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box: 0, piece: 0 };
  const { data, error } = await supabase
    .from('inventory_lots')
    .select('uom, qty_left')
    .eq('branch_id', branchId)
    .eq('product_sku', skuKey(sku))
    .gt('qty_left', 0);
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

/** ✅ 取有庫存 SKU 集（一般使用者過濾）— 改用 inventory_lots（集合存 uppercase） */
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
  (data || []).forEach(r => {
    const s = skuKey(r.product_sku || '');
    if (s) set.add(s);
  });
  return set;
}

/** 產品搜尋（名稱/條碼/編號）——大小寫不敏感（使用者以 lots 集合過濾） */
async function searchByName(keyword, role, _branch, inStockSet) {
  const k = String(keyword || '').trim();
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品名稱', `%${k}%`)
    .limit(20);
  if (error) throw error;
  let list = data || [];
  if (role === 'user') list = list.filter(p => inStockSet.has(skuKey(p['貨品編號'])));
  return list.slice(0, 10);
}
async function searchByBarcode(barcode, role, _branch, inStockSet) {
  const b = String(barcode || '').trim();
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .eq('條碼', b)
    .maybeSingle();
  if (error) throw error;
  if (!data) return [];
  if (role === 'user' && !inStockSet.has(skuKey(data['貨品編號']))) return [];
  return [data];
}
async function searchBySku(sku, role, _branch, inStockSet) {
  const s = String(sku || '').trim();
  const { data: exact, error: e1 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .eq('貨品編號', s)
    .maybeSingle();
  if (e1) throw e1;
  if (exact && (role !== 'user' || inStockSet.has(skuKey(exact['貨品編號'])))) {
    return [exact];
  }
  const { data: like, error: e2 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品編號', `%${s}%`)
    .limit(20);
  if (e2) throw e2;
  let list = like || [];
  if (role === 'user') list = list.filter(p => inStockSet.has(skuKey(p['貨品編號'])));
  return list.slice(0, 10);
}

/** 依 SKU 匯總各倉現量（lots）→ 回傳中文倉名 */
async function getWarehouseStockBySku(branch, sku) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return [];
  const { data, error } = await supabase
    .from('inventory_lots')
    .select('warehouse_name, uom, qty_left')
    .eq('branch_id', branchId)
    .eq('product_sku', skuKey(sku));
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

/** 產生候選倉名（中文 + code） */
async function warehouseCandidates(warehouseDisplayName) {
  const label = await resolveWarehouseLabel(warehouseDisplayName);
  const back = new Set([label]);
  for (const [code, cn] of FIX_WH_LABEL.entries()) {
    if (cn === label) back.add(code);
  }
  const { data } = await supabase
    .from('inventory_warehouses')
    .select('code,name')
    .or(`name.eq.${label},code.eq.${label}`)
    .limit(1)
    .maybeSingle();
  if (data?.code) back.add(data.code);
  if (data?.name) back.add(data.name);
  return Array.from(back);
}

/** 指定倉之現量（箱/件） */
async function getWarehouseStockForSku(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box: 0, piece: 0 };

  const candidates = await warehouseCandidates(warehouseDisplayName);

  const { data, error } = await supabase
    .from('inventory_lots')
    .select('warehouse_name, uom, qty_left')
    .eq('branch_id', branchId)
    .eq('product_sku', skuKey(sku))
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

/** 查 lots 並「用 unit_cost 重算」：庫存箱/件、庫存總額、顯示單價、箱入數 */
async function getWarehouseSnapshotFromLots(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box:0, piece:0, stockAmount:0, displayUnitCost:0, unitsPerBox:1 };

  const candidates = await warehouseCandidates(warehouseDisplayName);

  const { data: prod } = await supabase
    .from('products')
    .select('箱入數')
    .eq('貨品編號', skuKey(sku))
    .maybeSingle();
  const unitsPerBox = Number(prod?.['箱入數'] || 1) || 1;

  const { data, error } = await supabase
    .from('inventory_lots')
    .select('uom,qty_left,unit_cost,created_at,warehouse_name')
    .eq('branch_id', branchId)
    .eq('product_sku', skuKey(sku))
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

  return { box, piece, stockAmount: amount, displayUnitCost, unitsPerBox };
}

/** 指定倉庫 lots 顯示單價（最新且仍有量），找不到則 0 */
async function getWarehouseDisplayUnitCost(branch, sku, warehouseDisplayName) {
  const snap = await getWarehouseSnapshotFromLots(branch, sku, warehouseDisplayName);
  return snap.displayUnitCost || 0;
}

/** 倉庫 quick reply（查詢用；中文名） */
function buildQuickReplyForWarehousesForQuery(warehouseList) {
  const items = warehouseList.slice(0, 12).map(w => {
    const label = `${w.warehouse}（${w.box}箱/${w.piece}件）`.slice(0, 20);
    const text = `倉 ${w.warehouse}`;
    return { type: 'action', action: { type: 'message', label, text } };
  });
  return { items };
}

/** 二層倉庫 quick reply（出庫用；中文名） */
function buildQuickReplyForWarehouses(baseText, warehouseList, wantBox, wantPiece) {
  const items = warehouseList.slice(0, 12).map(w => {
    const label = `${w.warehouse}（${w.box}箱/${w.piece}散）`.slice(0, 20);
    const text = `${baseText} ${wantBox>0?`${wantBox}箱 `:''}${wantPiece>0?`${wantPiece}件 `:''}@${w.warehouse}`;
    return { type: 'action', action: { type: 'message', label, text: text.trim() } };
  });
  return { items };
}

/** FIFO 出庫（回 consumed 與 cost） */
async function callFifoOutLots(branch, sku, uom, qty, warehouseDisplayName, lineUserId) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) {
    const hint = '此 LINE 使用者尚未對應到 auth.users。請先在 line_user_map 建立對應。';
    throw new Error(`找不到對應的使用者（${lineUserId}）。${hint}`);
  }
  if (qty <= 0) return { consumed: 0, cost: 0 };

  const candidates = await warehouseCandidates(warehouseDisplayName);
  const whRaw = candidates[0] || warehouseDisplayName;

  const { data, error } = await supabase.rpc('fifo_out_lots', {
    p_group: branch,
    p_sku: skuKey(sku),
    p_uom: uom,
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

/** 同步聚合 inventory（群組+SKU） */
async function changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, source = 'LINE') {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) {
    const hint = '此 LINE 使用者尚未對應到 auth.users。請先在 line_user_map 建立對應。';
    throw new Error(`找不到對應的使用者（${lineUserId}）。${hint}`);
  }
  const { data, error } = await supabase.rpc('exec_change_inventory_by_group_sku', {
    p_group: branch,
    p_sku: skuKey(sku),
    p_delta_box: deltaBox,
    p_delta_piece: deltaPiece,
    p_user_id: authUuid,
    p_source: source
  });
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  return row || { new_box: null, new_piece: null };
}

/** ===== GAS Webhook 設定 ===== */
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

/** 推送 GAS */
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
    const res = await fetch(callUrl, {
      method: 'POST',
      headers: { 'Content-Type':'application/json' },
      body: JSON.stringify(payload)
    });
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
  } catch (e) {
    console.error('[LINE EVENT LOG ERROR]', e);
  }
}

app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));

// ⚠️ webhook 路由一定要在任何 body parser 之前
const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };
app.get('/webhook', (_req, res) => res.status(200).send('OK'));
app.get('/line/webhook', (_req, res) => res.status(200).send('OK'));
app.post('/webhook',      line.middleware(lineConfig), lineHandler);
app.post('/line/webhook', line.middleware(lineConfig), lineHandler);

// 如果你未來還有 JSON API，要放在 webhook 後面再掛 parser：
// app.use('/api', express.json());
// app.post('/api/something', (req, res) => { res.json({ok:true}); });

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

  const inStockSet = role === 'user' ? await getInStockSkuSet(branch) : new Set();

  // === 倉庫選擇（查詢後 step2；記憶最後選倉） ===
  if (parsed.type === 'wh_select') {
    const sku = await getLastSku(lineUserId, branch);
    if (!sku) { await replyText('請先選擇商品（查 / 條碼 / 編號）後再選倉庫'); return; }

    const wh = await resolveWarehouseLabel(parsed.warehouse);
    LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);

    const { data: prodRow } = await supabase
      .from('products')
      .select('貨品名稱, 箱入數')
      .eq('貨品編號', skuKey(sku))
      .maybeSingle();
    const prodName = prodRow?.['貨品名稱'] || sku;
    const boxSize = prodRow?.['箱入數'] ?? '-';
    const unitPrice = await getWarehouseDisplayUnitCost(branch, sku, wh);
    const { box, piece } = await getWarehouseStockForSku(branch, sku, wh);

    await replyText(
      `名稱：${prodName}\n` +
      `編號：${skuDisplay(sku)}\n` +
      `箱入數：${box
