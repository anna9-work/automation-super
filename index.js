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
app.use((req, _res, next) => {
  console.log(`[請求] ${req.method} ${req.path} ua=${req.headers['user-agent'] || ''} x-line-signature=${req.headers['x-line-signature'] ? 'yes' : 'no'}`);
  next();
});
const jsonParser = express.json();

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY);

/** ====== 進程內記憶：最後選的倉（不改 DB 結構） ====== */
const LAST_WAREHOUSE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> warehouseDisplayName(中文)

/** ====== 倉庫中文名快取 ====== */
const WAREHOUSE_NAME_CACHE = new Map(); // key=codeOrName -> 中文name

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

/** 將倉庫代碼/名稱轉為中文顯示名（匹配 warehouse_kinds.code 或 name） */
async function resolveWarehouseLabel(codeOrName) {
  const key = String(codeOrName || '').trim();
  if (!key) return '未指定';
  if (WAREHOUSE_NAME_CACHE.has(key)) return WAREHOUSE_NAME_CACHE.get(key);
  try {
    const { data } = await supabase
      .from('warehouse_kinds')
      .select('code,name')
      .or(`code.eq.${key},name.eq.${key}`)
      .limit(1)
      .maybeSingle();
    const label = data?.name || key; // 找不到就原樣
    WAREHOUSE_NAME_CACHE.set(key, label);
    return label;
  } catch {
    return key;
  }
}

/** 指令解析：含「倉 XXX」選倉、@倉庫、尾數出1=1件 */
function parseCommand(text) {
  const t = (text || '').trim();
  if (!/^(查|查詢|條碼|編號|#|入庫|入|出庫|出|倉)/.test(t)) return null;

  // 倉庫選擇（由 quick reply 送出）
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

  // 入/出：支援「3箱」「2件/散/個」與尾數（出1=1件），可帶 @倉庫 / (倉庫=xxx)
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

/** 聚合庫存（群組+SKU）— 僅用於是否有庫存判斷/一般使用者過濾 */
async function getStockByGroupSku(branch, sku) {
  const { data, error } = await supabase
    .from('inventory')
    .select('庫存箱數, 庫存散數')
    .eq('群組', branch)
    .eq('貨品編號', sku)
    .maybeSingle();
  if (error) throw error;
  return {
    box: Number(data?.['庫存箱數'] ?? 0),
    piece: Number(data?.['庫存散數'] ?? 0)
  };
}

/** 產品搜尋 quick reply */
function buildQuickReplyForProducts(products) {
  const items = products.slice(0, 12).map(p => ({
    type: 'action',
    action: { type: 'message', label: `${p['貨品名稱']}`.slice(0, 20), text: `編號 ${p['貨品編號']}` }
  }));
  return { items };
}

/** 依 SKU 匯總各倉現量（lots）→ 回傳中文倉名 */
async function getWarehouseStockBySku(branch, sku) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return [];
  const { data, error } = await supabase
    .from('inventory_lots')
    .select('warehouse_name, uom, qty_left')
    .eq('branch_id', branchId)
    .eq('product_sku', sku);
  if (error) throw error;

  const map = new Map(); // key = rawNameOrCode, value = { box, piece }
  (data || []).forEach(r => {
    const raw = String(r.warehouse_name || '未指定');
    const u = String(r.uom || '').toLowerCase();
    const q = Number(r.qty_left || 0);
    if (!map.has(raw)) map.set(raw, { box:0, piece:0 });
    const obj = map.get(raw);
    if (u === 'box') obj.box += q; else if (u === 'piece') obj.piece += q;
  });

  // 轉中文顯示名
  const list = [];
  for (const [raw, v] of map.entries()) {
    if (v.box > 0 || v.piece > 0) {
      const label = await resolveWarehouseLabel(raw);
      list.push({ warehouse: label, box: v.box, piece: v.piece, _raw: raw });
    }
  }
  return list;
}

/** 指定倉之現量（箱/件）——輸出中文名不影響查詢，查詢用 raw 名稱候選 */
async function getWarehouseStockForSku(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box: 0, piece: 0 };

  let candidates = [warehouseDisplayName];
  try {
    const { data: kind } = await supabase
      .from('warehouse_kinds')
      .select('code,name')
      .or(`name.eq.${warehouseDisplayName},code.eq.${warehouseDisplayName}`)
      .limit(1)
      .maybeSingle();
    if (kind?.code && kind?.name) {
      candidates = [kind.name, kind.code];
    }
  } catch {}

  const { data, error } = await supabase
    .from('inventory_lots')
    .select('warehouse_name, uom, qty_left')
    .eq('branch_id', branchId)
    .eq('product_sku', sku)
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

/** 指定倉的顯示單價：取該倉「最近仍有量」lot 的 unit_cost；若無則用 products.單價 */
async function getWarehouseDisplayUnitCost(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return null;

  let candidates = [warehouseDisplayName];
  try {
    const { data: kind } = await supabase
      .from('warehouse_kinds')
      .select('code,name')
      .or(`name.eq.${warehouseDisplayName},code.eq.${warehouseDisplayName}`)
      .limit(1)
      .maybeSingle();
    if (kind?.code && kind?.name) {
      candidates = [kind.name, kind.code];
    }
  } catch {}

  const { data: lot, error: e1 } = await supabase
    .from('inventory_lots')
    .select('unit_cost, created_at, qty_left, warehouse_name')
    .eq('branch_id', branchId)
    .eq('product_sku', sku)
    .in('warehouse_name', candidates)
    .gt('qty_left', 0)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (e1) throw e1;
  if (lot && lot.unit_cost !== null && typeof lot.unit_cost !== 'undefined') {
    return Number(lot.unit_cost);
  }
  const { data: prod } = await supabase
    .from('products')
    .select('單價')
    .eq('貨品編號', sku)
    .maybeSingle();
  const price = Number(String(prod?.['單價'] ?? '0').replace(/[^0-9.]/g, '')) || 0;
  return price;
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

  // 反查 raw 名稱
  let whRaw = warehouseDisplayName;
  try {
    const { data: kind } = await supabase
      .from('warehouse_kinds')
      .select('code,name')
      .or(`name.eq.${warehouseDisplayName},code.eq.${warehouseDisplayName}`)
      .limit(1)
      .maybeSingle();
    if (kind?.code && kind?.name) {
      whRaw = kind.name || kind.code || warehouseDisplayName;
    }
  } catch {}

  const { data, error } = await supabase.rpc('fifo_out_lots', {
    p_group: branch,
    p_sku: sku,
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

/** 同步更新 inventory 聚合表（群組+SKU） */
async function changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, source = 'LINE') {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) {
    const hint = '此 LINE 使用者尚未對應到 auth.users。請先在 line_user_map 建立對應。';
    throw new Error(`找不到對應的使用者（${lineUserId}）。${hint}`);
  }
  const { data, error } = await supabase.rpc('exec_change_inventory_by_group_sku', {
    p_group: branch,
    p_sku: sku,
    p_delta_box: deltaBox,
    p_delta_piece: deltaPiece,
    p_user_id: authUuid,
    p_source: source
  });
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  return row || { new_box: null, new_piece: null };
}

/** ===== GAS Webhook 載入 ===== */
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
loadGasConfigFromDBIfNeeded().catch(() => {});
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
  const cfg = await getGasConfig();
  const url = (cfg.url || '').trim();
  const sec = (cfg.secret || '').trim();
  if (!url || !sec) {
    if (!GAS_WARNED_MISSING) {
      console.warn('⚠️ GAS_WEBHOOK_URL / GAS_WEBHOOK_SECRET 未設定（已略過推送到試算表）');
      GAS_WARNED_MISSING = true;
    }
    return;
  }
  const callUrl = `${url.replace(/\?+.*/, '')}?secret=${encodeURIComponent(sec)}`;
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

/** 產品搜尋 */
async function searchByName(keyword, role, _branch, inStockSet) {
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品名稱', `%${keyword}%`)
    .limit(20);
  if (error) throw error;
  let list = data || [];
  if (role === 'user') list = list.filter(p => inStockSet.has(p['貨品編號']));
  return list.slice(0, 10);
}
async function searchByBarcode(barcode, role, _branch, inStockSet) {
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .eq('條碼', barcode.trim())
    .maybeSingle();
  if (error) throw error;
  if (!data) return [];
  if (role === 'user' && !inStockSet.has(data['貨品編號'])) return [];
  return [data];
}
async function searchBySku(sku, role, _branch, inStockSet) {
  const { data: exact, error: e1 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .eq('貨品編號', sku.trim())
    .maybeSingle();
  if (e1) throw e1;
  if (exact && (role !== 'user' || inStockSet.has(exact['貨品編號']))) {
    return [exact];
  }
  const { data: like, error: e2 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品編號', `%${sku}%`)
    .limit(20);
  if (e2) throw e2;
  let list = like || [];
  if (role === 'user') list = list.filter(p => inStockSet.has(p['貨品編號']));
  return list.slice(0, 10);
}

/** 取有庫存 SKU 集（一般使用者過濾） */
async function getInStockSkuSet(branch) {
  const { data, error } = await supabase
    .from('inventory')
    .select('貨品編號, 庫存箱數, 庫存散數')
    .eq('群組', branch);
  if (error) throw error;
  const set = new Set();
  (data || []).forEach(row => {
    const box = Number(row['庫存箱數'] || 0);
    const piece = Number(row['庫存散數'] || 0);
    if (box > 0 || piece > 0) set.add(row['貨品編號']);
  });
  return set;
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
  the:
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

    // 商品基本 + 該倉的顯示單價與庫存
    const { data: prodRow } = await supabase
      .from('products')
      .select('貨品名稱, 箱入數')
      .eq('貨品編號', sku)
      .maybeSingle();
    const prodName = prodRow?.['貨品名稱'] || sku;
    const boxSize = prodRow?.['箱入數'] ?? '-';
    const unitPrice = await getWarehouseDisplayUnitCost(branch, sku, wh);
    const { box, piece } = await getWarehouseStockForSku(branch, sku, wh);

    await replyText(
      `名稱：${prodName}\n` +
      `編號：${sku}\n` +
      `箱入數：${boxSize}\n` +
      `單價：${unitPrice}\n` +
      `倉庫類別：${wh}\n` +
      `庫存：${box}箱${piece}散`
    );
    return;
  }

  // === 查詢 ===
  if (parsed.type === 'query') {
    const list = await searchByName(parsed.keyword, role, branch, inStockSet);
    if (!list.length) { await replyText(role === '主管' ? '查無此商品' : '無此商品庫存'); return; }
    if (list.length > 1) {
      await reply({ type: 'text', text: `找到以下與「${parsed.keyword}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
      return;
    }
    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (role === 'user' && s.box === 0 && s.piece === 0) { await replyText('無此商品庫存'); return; }
    await upsertUserLastProduct(lineUserId, branch, sku);

    const whList = await getWarehouseStockBySku(branch, sku); // 中文名
    if (whList.length >= 2) {
      await reply({
        type: 'text',
        text: `名稱：${p['貨品名稱']}\n編號：${sku}\n請選擇倉庫`,
        quickReply: buildQuickReplyForWarehousesForQuery(whList)
      });
      return;
    }
    if (whList.length === 1) {
      const wh = whList[0].warehouse;
      LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh); // 只有一倉也記憶
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
    if (!list.length) { await replyText(role === '主管' ? '查無此條碼商品' : '無此商品庫存'); return; }
    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (role === 'user' && s.box === 0 && s.piece === 0) { await replyText('無此商品庫存'); return; }
    await upsertUserLastProduct(lineUserId, branch, sku);

    const whList = await getWarehouseStockBySku(branch, sku);
    if (whList.length >= 2) {
      await reply({
        type: 'text',
        text: `名稱：${p['貨品名稱']}\n編號：${sku}\n請選擇倉庫`,
        quickReply: buildQuickReplyForWarehousesForQuery(whList)
      });
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
    if (!list.length) { await replyText(role === '主管' ? '查無此貨品編號' : '無此商品庫存'); return; }
    if (list.length > 1) {
      await reply({ type: 'text', text: `找到以下與「${parsed.sku}」相關的選項`, quickReply: buildQuickReplyForProducts(list) });
      return;
    }
    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (role === 'user' && s.box === 0 && s.piece === 0) { await replyText('無此商品庫存'); return; }
    await upsertUserLastProduct(lineUserId, branch, sku);

    const whList = await getWarehouseStockBySku(branch, sku);
    if (whList.length >= 2) {
      await reply({
        type: 'text',
        text: `名稱：${p['貨品名稱']}\n編號：${sku}\n請選擇倉庫`,
        quickReply: buildQuickReplyForWarehousesForQuery(whList)
      });
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

  // === 入/出庫 ===
  if (parsed.type === 'change') {
    if (parsed.action === 'in' && role !== '主管') { await replyText('您無法使用「入庫」'); return; }
    if (parsed.box === 0 && parsed.piece === 0) return;

    const sku = await getLastSku(lineUserId, branch);
    if (!sku) { await replyText('請先用「查 商品」或「條碼123 / 編號ABC」選定商品後再入/出庫。'); return; }

    // 出庫：若未帶倉，優先使用「最後選倉」；仍無則才要求選
    if (parsed.action === 'out') {
      if (!parsed.warehouse) {
        const remembered = LAST_WAREHOUSE_BY_USER_BRANCH.get(`${lineUserId}::${branch}`) || null;
        if (remembered) {
          parsed.warehouse = remembered; // ✅ 不再跳倉庫選單
        } else {
          const list = await getWarehouseStockBySku(branch, sku);
          if (list.length >= 2) {
            const qr = buildQuickReplyForWarehouses('出', list, parsed.box, parsed.piece);
            await reply({ type: 'text', text: `請選擇要出庫的倉庫`, quickReply: qr });
            return;
          }
          if (list.length === 1) parsed.warehouse = list[0].warehouse;
        }
      }
    }

    try {
      if (parsed.action === 'out') {
        const wh = await resolveWarehouseLabel(parsed.warehouse || '未指定');
        LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh); // 記憶

        // FIFO 成本合計（box / piece 分開扣）
        let fifoCostTotal = 0;
        if (parsed.box > 0) {
          const rBox = await callFifoOutLots(branch, sku, 'box', parsed.box, wh, lineUserId);
          fifoCostTotal += Number(rBox.cost || 0);
        }
        if (parsed.piece > 0) {
          const rPiece = await callFifoOutLots(branch, sku, 'piece', parsed.piece, wh, lineUserId);
          fifoCostTotal += Number(rPiece.cost || 0);
        }

        // 同步聚合（群組+SKU）
        await changeInventoryByGroupSku(
          branch,
          sku,
          parsed.box > 0 ? -parsed.box : 0,
          parsed.piece > 0 ? -parsed.piece : 0,
          lineUserId,
          'LINE'
        );

        // 顯示/推送（以該倉現量為準！）
        const { data: prodRow } = await supabase
          .from('products')
          .select('貨品名稱, 箱入數, 單價')
          .eq('貨品編號', sku)
          .maybeSingle();
        const prodName = prodRow?.['貨品名稱'] || sku;
        const unitsPerBox = Number(String(prodRow?.['箱入數'] ?? '1').replace(/[^\d]/g, '')) || 1;
        const sellPrice   = Number(String(prodRow?.['單價']   ?? '0').replace(/[^0-9.]/g, '')) || 0;

        const { box: whBox, piece: whPiece } = await getWarehouseStockForSku(branch, sku, wh);

        const payload = {
          type: 'log',
          group: String(branch || '').trim().toLowerCase(),
          sku,
          name: prodName,
          units_per_box: unitsPerBox,
          unit_price: sellPrice,                // 參考價（顯示用）
          in_box: 0,
          in_piece: 0,
          out_box: parsed.box,
          out_piece: parsed.piece,
          stock_box: whBox,                     // ★ 只填該倉
          stock_piece: whPiece,                 // ★ 只填該倉
          out_amount: fifoCostTotal,            // ★ FIFO 成本合計
          stock_amount: (whBox * unitsPerBox + whPiece) * sellPrice, // 該倉估值
          warehouse: wh,                        // ★ 中文名
          created_at: formatTpeIso(new Date())
        };
        postInventoryToGAS(payload).catch(()=>{});

        await replyText(
          `✅ 出庫成功\n` +
          `貨品名稱：${prodName}\n` +
          `倉庫類別：${wh}\n` +
          `出庫：${parsed.box || 0}箱 ${parsed.piece || 0}件\n` +
          `目前庫存：${whBox}箱${whPiece}散`
        );
        return;
      }

      // 入庫（聚合 inventory 維持既有；lots 入庫另案）
      const deltaBox = parsed.box;
      const deltaPiece = parsed.piece;
      const r = await changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, 'LINE');
      let nb = r?.new_box ?? null, np = r?.new_piece ?? null;
      if (nb === null || np === null) {
        const s2 = await getStockByGroupSku(branch, sku);
        nb = s2.box; np = s2.piece;
      }
      const { data: prodRow } = await supabase
        .from('products')
        .select('貨品名稱')
        .eq('貨品編號', sku)
        .maybeSingle();
      const prodName = prodRow?.['貨品名稱'] || sku;
      await replyText(`✅ 入庫成功\n貨品名稱：${prodName}\n目前庫存：${nb}箱${np}散`);
      return;

    } catch (err) {
      console.error('change error:', err);
      await replyText(`操作失敗：${err?.message || '未知錯誤'}`);
      return;
    }
  }
  return;
}

/** 你若還有其它自訂 API，要用 JSON，像下面這樣掛 parser： */
// app.post('/some/api', jsonParser, async (req, res) => { /* ... */ });

process.on('unhandledRejection', (reason) => {
  console.error('[UNHANDLED REJECTION]', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT EXCEPTION]', err);
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
