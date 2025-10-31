import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/* ======== 環境變數 ======== */
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
  console.log(`[請求] ${req.method} ${req.path} ua=${req.headers['user-agent']||''} x-line-signature=${req.headers['x-line-signature']?'yes':'no'}`);
  next();
});
const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

/* ======== 運行暫存 ======== */
const LAST_WAREHOUSE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` -> 中文倉名
const WAREHOUSE_NAME_CACHE = new Map();          // code/name -> 中文

/* ======== 固定倉庫映射（code → 中文） ======== */
const FIX_WH_LABEL = new Map([
  ['swap', '夾換品'],
  ['agency', '代夾物'],
  ['main', '總倉'],
  ['withdraw', '撤台'],
  ['unspecified', '未指定'],
]);

/* ======== 小工具 ======== */
const skuKey     = (s) => String(s||'').trim(); // 保留大小寫
const skuUpper   = (s) => String(s||'').trim().toUpperCase();
const skuDisplay = (s) => { const t=String(s||'').trim(); return t? (t.slice(0,1).toUpperCase()+t.slice(1).toLowerCase()):''; };
function tpeNowISO() {
  const s=new Intl.DateTimeFormat('sv-SE',{ timeZone:'Asia/Taipei', year:'numeric',month:'2-digit',day:'2-digit', hour:'2-digit',minute:'2-digit', second:'2-digit', hour12:false }).format(new Date());
  return s.replace(' ','T') + '+08:00';
}

/* ======== 解析倉庫 ======== */
async function resolveWarehouseLabel(codeOrName) {
  const key = String(codeOrName||'').trim();
  if (!key) return '未指定';
  if (FIX_WH_LABEL.has(key)) return FIX_WH_LABEL.get(key);
  if (WAREHOUSE_NAME_CACHE.has(key)) return WAREHOUSE_NAME_CACHE.get(key);
  try {
    let label = key;
    {
      const { data } = await supabase.from('inventory_warehouses').select('code,name').or(`code.eq.${key},name.eq.${key}`).limit(1).maybeSingle();
      if (data?.name) label = data.name;
    }
    if (label === key) {
      const { data } = await supabase.from('warehouse_kinds').select('code,name').or(`code.eq.${key},name.eq.${key}`).limit(1).maybeSingle();
      if (data?.name) label = data.name;
    }
    WAREHOUSE_NAME_CACHE.set(key, label);
    return label;
  } catch { return key; }
}
async function getWarehouseCodeForLabel(displayName) {
  const label = String(displayName||'').trim();
  for (const [code, cn] of FIX_WH_LABEL.entries()) if (cn === label) return code;
  const { data } = await supabase.from('inventory_warehouses').select('code,name').or(`name.eq.${label},code.eq.${label}`).limit(1).maybeSingle();
  if (data?.code) return data.code;
  return 'unspecified';
}

/* ======== 使用者/分店 ======== */
async function resolveAuthUuidFromLineUserId(lineUserId) {
  if (!lineUserId) return null;
  const { data, error } = await supabase.from('line_user_map').select('auth_user_id').eq('line_user_id', lineUserId).maybeSingle();
  if (error) { console.warn('[resolveAuthUuid] line_user_map error:', error); return null; }
  return data?.auth_user_id || null;
}
async function getBranchIdByGroupCode(groupCode) {
  const key = String(groupCode||'').trim();
  if (!key) return null;
  const { data, error } = await supabase.from('branches').select('id').ilike('分店代號', key).maybeSingle();
  if (error) throw error;
  return data?.id ?? null;
}
async function resolveBranchAndRole(event) {
  const src = event.source || {};
  const userId = src.userId || null;
  const isGroup = src.type === 'group';
  let role = 'user', blocked = false;
  if (userId) {
    const { data:u } = await supabase.from('users').select('角色, 黑名單, 群組').eq('user_id', userId).maybeSingle();
    role = u?.角色 || 'user'; blocked = !!u?.黑名單;
  }
  if (isGroup) {
    const { data:lg } = await supabase.from('line_groups').select('群組').eq('line_group_id', src.groupId).maybeSingle();
    return { branch: lg?.群組 || null, role, blocked, needBindMsg:'此群組尚未綁定分店，請管理員設定' };
  } else {
    const { data:u2 } = await supabase.from('users').select('群組').eq('user_id', userId).maybeSingle();
    return { branch: u2?.群組 || null, role, blocked, needBindMsg:'此使用者尚未綁定分店，請管理員設定' };
  }
}
async function autoRegisterUser(lineUserId) {
  if (!lineUserId) return;
  const { data } = await supabase.from('users').select('user_id').eq('user_id', lineUserId).maybeSingle();
  if (!data) await supabase.from('users').insert({ user_id: lineUserId, 群組: DEFAULT_GROUP, 角色:'user', 黑名單:false });
}

/* ======== lots 為準：判斷是否有庫存 / 取得倉庫分佈 / 快照 ======== */
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
  (data||[]).forEach(r => {
    if (Number(r.qty_left||0) > 0) set.add(skuUpper(r.product_sku));
  });
  return set;
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
  (data||[]).forEach(r => {
    const name = String(r.warehouse_name||'未指定');
    const u = String(r.uom||'').toLowerCase();
    const q = Number(r.qty_left||0);
    if (!map.has(name)) map.set(name, { warehouse: name, box:0, piece:0 });
    const obj = map.get(name);
    if (u==='box') obj.box += q;
    else if (u==='piece') obj.piece += q;
  });
  return Array.from(map.values()).filter(w => w.box>0 || w.piece>0);
}
async function getWarehouseSnapshotFromLots(branch, sku, warehouseDisplayName) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return { box:0, piece:0, stockAmount:0, displayUnitCost:0, unitsPerBox:1 };
  const label = await resolveWarehouseLabel(warehouseDisplayName);
  const { data:prod } = await supabase.from('products').select('箱入數').ilike('貨品編號', sku).maybeSingle();
  const unitsPerBox = Number(prod?.['箱入數']||1) || 1;

  const { data, error } = await supabase
    .from('inventory_lots')
    .select('uom, qty_left, unit_cost, created_at, warehouse_name, product_sku')
    .eq('branch_id', branchId)
    .ilike('product_sku', sku)
    .eq('warehouse_name', label);
  if (error) throw error;

  let box=0, piece=0, amount=0, displayUnitCost=0, latestTs=0;
  (data||[]).forEach(r=>{
    const u=String(r.uom||'').toLowerCase(); const q=Number(r.qty_left||0); const c=Number(r.unit_cost||0);
    const ts=new Date(r.created_at||0).getTime();
    if(u==='box') box+=q; else if(u==='piece') piece+=q;
    const pieces=(u==='box') ? (q*unitsPerBox) : q;
    amount += pieces * c;
    if(q>0 && ts>=latestTs){ latestTs=ts; displayUnitCost=c; }
  });
  return { box, piece, stockAmount: amount, displayUnitCost, unitsPerBox };
}

/* ======== 產品搜尋（products + lots 過濾「有庫存」） ======== */
async function searchByName(keyword, _role, branch) {
  const k = String(keyword||'').trim();
  if (!k) return [];
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品名稱', `%${k}%`)
    .limit(30);
  if (error) throw error;
  const set = await getInStockSkuSet(branch);
  return (data||[]).filter(p => set.has(String(p['貨品編號']).toUpperCase())).slice(0, 10);
}
async function searchByBarcode(barcode, _role, branch) {
  const b = String(barcode||'').trim();
  if (!b) return [];
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .eq('條碼', b)
    .maybeSingle();
  if (error) throw error;
  if (!data) return [];
  const set = await getInStockSkuSet(branch);
  return set.has(String(data['貨品編號']).toUpperCase()) ? [data] : [];
}
async function searchBySku(sku, _role, branch) {
  const s = String(sku||'').trim();
  if (!s) return [];
  const { data: exact, error: e1 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品編號', s)
    .maybeSingle();
  if (e1) throw e1;
  const set = await getInStockSkuSet(branch);
  if (exact && set.has(String(exact['貨品編號']).toUpperCase())) return [exact];
  const { data: like, error: e2 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 箱入數, 單價')
    .ilike('貨品編號', `%${s}%`)
    .limit(30);
  if (e2) throw e2;
  return (like||[]).filter(p => set.has(String(p['貨品編號']).toUpperCase())).slice(0, 10);
}

/* ======== Quick Reply（唯一版本；避免重複宣告） ======== */
function buildQuickReplyForProducts(products){
  const items = products.slice(0,12).map(p=>({ type:'action', action:{ type:'message', label:`${p['貨品名稱']}`.slice(0,20), text:`編號 ${p['貨品編號']}` }}));
  return { items };
}
function buildQuickReplyForWarehousesForQuery(warehouseList){
  const items = warehouseList.slice(0,12).map(w=>({ type:'action', action:{ type:'message', label:`${w.warehouse}（${w.box}箱/${w.piece}件）`.slice(0,20), text:`倉 ${w.warehouse}` }}));
  return { items };
}
function buildQuickReplyForWarehouses(baseText, warehouseList, wantBox, wantPiece){
  const items = warehouseList.slice(0,12).map(w=>{
    const label = `${w.warehouse}（${w.box}箱/${w.piece}散）`.slice(0,20);
    const text  = `${baseText} ${wantBox>0?`${wantBox}箱 `:''}${wantPiece>0?`${wantPiece}件 `:''}@${w.warehouse}`.trim();
    return { type:'action', action:{ type:'message', label, text } };
  });
  return { items };
}

/* ======== 解析指令 ======== */
function parseCommand(text) {
  const t = (text||'').trim();
  if (!/^(查|查詢|條碼|編號|#|入庫|入|出庫|出|倉)/.test(t)) return null;
  const mWhSel = t.match(/^倉(?:庫)?\s*(.+)$/);
  if (mWhSel) return { type:'wh_select', warehouse: mWhSel[1].trim() };
  const mBarcode = t.match(/^條碼[:：]?\s*(.+)$/);
  if (mBarcode) return { type:'barcode', barcode: mBarcode[1].trim() };
  const mSkuHash = t.match(/^#\s*(.+)$/);
  if (mSkuHash) return { type:'sku', sku: mSkuHash[1].trim() };
  const mSku = t.match(/^編號[:：]?\s*(.+)$/);
  if (mSku) return { type:'sku', sku: mSku[1].trim() };
  const mQuery = t.match(/^查(?:詢)?\s*(.+)$/);
  if (mQuery) return { type:'query', keyword: mQuery[1].trim() };
  const mChange = t.match(/^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散|件))?(?:\s*(\d+))?(?:\s*(?:@|（?\(?倉庫[:：=]\s*)([^)）]+)\)?)?\s*$/);
  if (mChange) {
    const box = mChange[2] ? parseInt(mChange[2],10) : 0;
    const pieceLabeled = mChange[3] ? parseInt(mChange[3],10) : 0;
    const pieceTail = mChange[4] ? parseInt(mChange[4],10) : 0;
    const warehouse = (mChange[5]||'').trim();
    return { type:'change', action:/入/.test(mChange[1])?'in':'out', box, piece:pieceLabeled||pieceTail, warehouse: warehouse||null };
  }
  return null;
}

/* ======== FIFO 出庫（inventory_lots） ======== */
async function callFifoOutLots(branch, sku, uom, qty, warehouseDisplayName, lineUserId) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error(`找不到對應的使用者（${lineUserId}）。請先在 line_user_map 建立對應。`);
  if (qty <= 0) return { consumed: 0, cost: 0 };

  const whRaw = await resolveWarehouseLabel(warehouseDisplayName);
  const { data, error } = await supabase.rpc('fifo_out_lots', {
    p_group: String(branch||'').trim().toLowerCase(),
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
  return { consumed: Number(row?.consumed||0), cost: Number(row?.cost||0) }; // cost = 單價(散)
}

/* ======== （可留）舊彙總 RPC ======== */
async function changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, source='LINE') {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error(`找不到對應的使用者（${lineUserId}）。請先在 line_user_map 建立對應。`);
  const { data, error } = await supabase.rpc('exec_change_inventory_by_group_sku', {
    p_group: branch, p_sku: skuKey(sku), p_delta_box: deltaBox, p_delta_piece: deltaPiece, p_user_id: authUuid, p_source: source
  });
  if (error) throw error;
  return (Array.isArray(data)?data[0]:data) || { new_box:null, new_piece:null };
}

/* ======== 寫入 inventory_logs（出庫） ======== */
async function insertInventoryLogOut({ branch, sku, warehouseLabel, unitPricePiece, qtyBox, qtyPiece, userId, refTable, refId, afterBox, afterPiece }) {
  const { data: prod } = await supabase.from('products').select('貨品名稱,"箱入數"').ilike('貨品編號', sku).maybeSingle();
  const name = prod?.['貨品名稱'] || sku;
  const unitsPerBox = Number(prod?.['箱入數'] || 1) || 1;

  const totalPiecesOut = (Number(qtyBox||0)*unitsPerBox) + Number(qtyPiece||0);
  const outAmount = totalPiecesOut * Number(unitPricePiece||0);
  const stockAmount = ((Number(afterBox||0) * unitsPerBox) + Number(afterPiece||0)) * Number(unitPricePiece||0);

  const nowIso = new Date().toISOString();
  const warehouseCode = await getWarehouseCodeForLabel(warehouseLabel);

  const row = {
    '貨品編號': skuKey(sku),
    '貨品名稱': name,
    '入庫箱數': '0',
    '入庫散數': 0,
    '出庫箱數': String(Number(qtyBox||0)),
    '出庫散數': String(Number(qtyPiece||0)),
    '出庫金額': String(outAmount),
    '入庫金額': '0',
    '庫存金額': String(stockAmount),
    '建立時間': nowIso,
    '群組': String(branch||'').trim().toLowerCase(),
    '操作來源': 'LINE',
    'user_id': userId || null,
    '倉庫別': warehouseLabel,
    '倉庫代碼': warehouseCode
  };

  const { error } = await supabase.from('inventory_logs').insert([row]);
  if (error) throw error;
}

/* ======== Quick Reply builder（logging） ======== */
function logEventSummary(event){
  try{
    const src=event?.source||{}; const msg=event?.message||{}; const isGroup=src.type==='group'; const isRoom=src.type==='room';
    console.log(`[LINE EVENT] type=${event?.type} source=${src.type||'-'} groupId=${isGroup?src.groupId:'-'} roomId=${isRoom?src.roomId:'-'} userId=${src.userId||'-'} text="${msg?.type==='text'?msg.text:''}"`);
  }catch(e){ console.error('[LINE EVENT LOG ERROR]', e); }
}

/* ======== 主流程 ======== */
const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };
app.get('/health', (_req,res)=>res.status(200).send('OK'));
app.get('/',       (_req,res)=>res.status(200).send('RUNNING'));
app.get('/webhook',      (_req,res)=>res.status(200).send('OK'));
app.get('/line/webhook', (_req,res)=>res.status(200).send('OK'));
app.post('/webhook',      line.middleware(lineConfig), lineHandler);
app.post('/line/webhook', line.middleware(lineConfig), lineHandler);
app.use((err, req, res, next) => { if (req.path==='/webhook' || req.path==='/line/webhook'){ console.error('[LINE MIDDLEWARE ERROR]', err?.message||err); return res.status(400).end(); } return next(err); });

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
  const { data, error } = await supabase.from('user_last_product').select('貨品編號').eq('user_id', lineUserId).eq('群組', branch).order('建立時間',{ascending:false}).limit(1).maybeSingle();
  if (error) throw error;
  return data?.['貨品編號'] || null;
}

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

async function handleEvent(event){
  if (event.type!=='message' || event.message.type!=='text') return;
  const text = event.message.text || '';
  const parsed = parseCommand(text);
  if (!parsed) return;

  const source = event.source || {};
  const isGroup = source.type === 'group';
  const lineUserId = source.userId || null;
  if (!isGroup && lineUserId) await autoRegisterUser(lineUserId);

  const { branch, role, blocked, needBindMsg } = await resolveBranchAndRole(event);
  if (blocked) return;
  if (!branch) { await client.replyMessage(event.replyToken, { type:'text', text: needBindMsg || '尚未分店綁定，請管理員設定' }); return; }

  const reply = (msg) => client.replyMessage(event.replyToken, msg);
  const replyText = (s) => reply({ type:'text', text:s });

  // 倉庫選擇
  if (parsed.type === 'wh_select') {
    const sku = await getLastSku(lineUserId, branch);
    if (!sku) { await replyText('請先選商品（查/條碼/編號）再選倉庫'); return; }
    const wh = await resolveWarehouseLabel(parsed.warehouse);
    LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);

    const snap = await getWarehouseSnapshotFromLots(branch, sku, wh);
    const { data: prodRow } = await supabase.from('products').select('貨品名稱, 箱入數, 單價').ilike('貨品編號', sku).maybeSingle();
    const name = prodRow?.['貨品名稱'] || sku;
    const unitsPerBox = Number(prodRow?.['箱入數']||1) || 1;
    const price = Number(prodRow?.['單價']||0);
    await replyText(`品名：${name}\n編號：${sku}\n箱入數：${unitsPerBox}\n單價：${price}\n庫存：${snap.box}箱${snap.piece}散`);
    return;
  }

  // 查詢（用 products + lots）
  const doQueryCommon = async (p) => {
    const sku = p['貨品編號'];
    const whList = await getWarehouseStockBySku(branch, sku);
    if (!whList.length) { await replyText('無此商品庫存'); return; }
    await upsertUserLastProduct(lineUserId, branch, sku);

    if (whList.length >= 2) {
      await reply({ type:'text', text:`名稱：${p['貨品名稱']}\n編號：${sku}\n👉請選擇倉庫`, quickReply: buildQuickReplyForWarehousesForQuery(whList) });
      return;
    }
    const chosen = whList[0];
    const { data: prodRow } = await supabase.from('products').select('貨品名稱, 箱入數, 單價').ilike('貨品編號', sku).maybeSingle();
    const name = prodRow?.['貨品名稱'] || sku;
    const unitsPerBox = Number(prodRow?.['箱入數']||1) || 1;
    const price = Number(prodRow?.['單價']||0);
    await replyText(`名稱：${name}\n編號：${sku}\n箱入數：${unitsPerBox}\n單價：${price}\n倉庫類別：${chosen.warehouse}\n庫存：${chosen.box}箱${chosen.piece}散`);
  };

  if (parsed.type==='query') {
    const list = await searchByName(parsed.keyword, role, branch);
    if (!list.length) { await replyText('無此商品庫存'); return; }
    if (list.length>1) { await reply({ type:'text', text:`找到以下與「${parsed.keyword}」相關的選項`, quickReply: buildQuickReplyForProducts(list) }); return; }
    await doQueryCommon(list[0]); return;
  }
  if (parsed.type==='barcode') {
    const list = await searchByBarcode(parsed.barcode, role, branch);
    if (!list.length) { await replyText('無此商品庫存'); return; }
    await doQueryCommon(list[0]); return;
  }
  if (parsed.type==='sku') {
    const list = await searchBySku(parsed.sku, role, branch);
    if (!list.length) { await replyText('無此商品庫存'); return; }
    if (list.length>1) { await reply({ type:'text', text:`找到以下與「${parsed.sku}」相關的選項`, quickReply: buildQuickReplyForProducts(list) }); return; }
    await doQueryCommon(list[0]); return;
  }

  // 入/出庫
  if (parsed.type==='change') {
    if (parsed.action==='in' && role!=='主管') { await replyText('您無法使用「入庫」'); return; }
    if (parsed.box===0 && parsed.piece===0) return;

    const skuLast = await getLastSku(lineUserId, branch);
    if (!skuLast) { await replyText('請先用「查 商品」或「條碼/編號」選定「有庫存」商品後再入/出庫。'); return; }

    if (parsed.action==='out') {
      if (!parsed.warehouse) {
        const list = await getWarehouseStockBySku(branch, skuLast);
        if (list.length >= 2) { await reply({ type:'text', text:'請選擇要出庫的倉庫', quickReply: buildQuickReplyForWarehouses('出', list, parsed.box, parsed.piece) }); return; }
        if (list.length === 1) parsed.warehouse = list[0].warehouse;
      }

      try {
        const wh = await resolveWarehouseLabel(parsed.warehouse || '未指定');
        LAST_WAREHOUSE_BY_USER_BRANCH.set(`${lineUserId}::${branch}`, wh);

        // 出庫前快照（以 lots 為準）
        const beforeSnap = await getWarehouseSnapshotFromLots(branch, skuLast, wh);
        const unitsPerBoxForCalc = beforeSnap.unitsPerBox || 1;

        // FIFO 扣庫（箱/散分別，箱對箱、散對散）
        let fifoUnitPieceCosts = [];
        if (parsed.box>0) {
          const rBox = await callFifoOutLots(branch, skuLast, 'box',   parsed.box,   wh, lineUserId);
          fifoUnitPieceCosts.push({ uom:'box',   consumed:rBox.consumed, unitCost:rBox.cost });
        }
        if (parsed.piece>0) {
          const rPiece = await callFifoOutLots(branch, skuLast, 'piece', parsed.piece, wh, lineUserId);
          fifoUnitPieceCosts.push({ uom:'piece', consumed:rPiece.consumed, unitCost:rPiece.cost });
        }

        // 舊彙總（可留）
        await changeInventoryByGroupSku(
          branch,
          skuLast,
          parsed.box>0 ? -parsed.box : 0,
          parsed.piece>0 ? -parsed.piece : 0,
          lineUserId,
          'LINE'
        ).catch(()=>{});

        // 出庫單價(散)：若同時有箱/散，以散的 FIFO 單價優先；否則取前快照顯示單價
        const unitPricePiece =
          (fifoUnitPieceCosts.find(x=>x.uom==='piece')?.unitCost)
          ?? (fifoUnitPieceCosts.find(x=>x.uom==='box')?.unitCost)
          ?? Number(beforeSnap.displayUnitCost||0);

        // 不重新查：以「快照 - 本次出庫」計算當下回覆
        const afterBox   = Math.max(0, (beforeSnap.box||0)   - (parsed.box||0));
        const afterPiece = Math.max(0, (beforeSnap.piece||0) - (parsed.piece||0));

        // logs（回寫當下金額與數量）
        const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
        await insertInventoryLogOut({
          branch,
          sku: skuLast,
          warehouseLabel: wh,
          unitPricePiece,
          qtyBox: parsed.box||0,
          qtyPiece: parsed.piece||0,
          userId: authUuid,
          refTable: 'linebot',
          refId: event.message?.id || null,
          afterBox,
          afterPiece
        });

        await replyText(
          `✅ 出庫成功\n` +
          `品名：${skuLast}\n` +
          `倉別：${wh}\n` +
          `出庫：${parsed.box||0}箱 ${parsed.piece||0}件\n` +
          `👉目前庫存：${afterBox}箱${afterPiece}散`
        );
        return;
      } catch (err) {
        console.error('change error:', err);
        await replyText(`操作失敗：${err?.message || '未知錯誤'}`);
        return;
      }
    }

    await replyText('入庫請改用 App 進行；LINE 僅提供出庫');
    return;
  }
}

/* ======== 啟動 ======== */
app.listen(PORT, () => { console.log(`Server running on port ${PORT}`); });
