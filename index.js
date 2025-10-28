// index.js（整合修正版）
import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/* ========= Env ========= */
const {
  PORT = 3000,
  LINE_CHANNEL_ACCESS_TOKEN,
  LINE_CHANNEL_SECRET,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  DEFAULT_GROUP = 'default',
  GAS_WEBHOOK_URL: ENV_GAS_URL,
  GAS_WEBHOOK_SECRET: ENV_GAS_SECRET,
  NODE_ENV = 'development',
} = process.env;

if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) {
  console.error('❌ 缺少 LINE 環境變數');
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ 缺少 Supabase 環境變數 (URL / SERVICE_ROLE_KEY)');
}

/* ========= App ========= */
const app = express();
// LINE 中介軟體需要原始 body，故不要先掛 express.json()
app.use((req, _res, next) => {
  console.log(`[請求] ${req.method} ${req.path} ua=${req.headers['user-agent'] || ''} sig=${req.headers['x-line-signature'] ? 'yes' : 'no'}`);
  next();
});

// 如果你還有自訂 REST API 要收 JSON，再在那些路由「之後」掛 parser
const jsonParser = express.json();

/* ========= Clients ========= */
const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY);

/* ========= Utils ========= */
// Node 18+ 內建 fetch；若是 Node 16，用 node-fetch
async function safeFetch(url, opt) {
  if (typeof fetch === 'function') return fetch(url, opt);
  const { default: nodeFetch } = await import('node-fetch');
  return nodeFetch(url, opt);
}

function formatTpeIso(date = new Date()) {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(date);
  return s.replace(' ', 'T') + '+08:00';
}

function logEventSummary(event) {
  try {
    const src = event?.source || {};
    const msg = event?.message || {};
    const isGroup = src.type === 'group';
    const isRoom  = src.type === 'room';
    const groupId = isGroup ? src.groupId : null;
    const roomId  = isRoom ? src.roomId : null;
    const userId  = src.userId || null;
    const text    = msg?.type === 'text' ? msg.text : '';
    console.log(`[LINE EVENT] type=${event?.type} src=${src.type || '-'} gid=${groupId || '-'} rid=${roomId || '-'} uid=${userId || '-'} text="${text}"`);
  } catch (e) {
    console.error('[LINE EVENT LOG ERROR]', e);
  }
}

/* ========= Supabase helpers ========= */
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

async function getBranchIdByGroupCode(groupCode) {
  const { data, error } = await supabase
    .from('branches')
    .select('id')
    .eq('分店代號', groupCode)
    .maybeSingle();
  if (error) throw error;
  return data?.id ?? null;
}

async function changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, source = 'LINE') {
  // 這是舊「群組彙總」路線，沿用你原本的 RPC 名稱（若沒有，請告訴我）
  const { data, error } = await supabase.rpc('exec_change_inventory_by_group_sku', {
    p_group: branch,
    p_sku: sku,
    p_boxes: deltaBox,
    p_pieces: deltaPiece,
    p_user_id: lineUserId || 'LINE',
    p_source: source || 'LINE'
  });
  if (error) throw error;
  // 預期回傳 new_box/new_piece；若沒有，呼叫者會再查一次 inventory
  return Array.isArray(data) ? data[0] : data;
}

/* ========= 指令解析 ========= */
// 支援：
// 查/查詢 文字、條碼 123、編號 AG030、#AG030、出 2箱1件、入3件、出5箱 @櫃倉、出2件(倉庫=總倉)
function parseCommand(text) {
  const t = (text || '').trim();
  if (!/^(查|查詢|條碼|編號|#|入庫|入|出庫|出)/.test(t)) return null;

  const mBarcode = t.match(/^條碼[:：]?\s*(.+)$/);
  if (mBarcode) return { type: 'barcode', barcode: mBarcode[1].trim() };

  const mSkuHash = t.match(/^#\s*(.+)$/);
  if (mSkuHash) return { type: 'sku', sku: mSkuHash[1].trim() };
  const mSku = t.match(/^編號[:：]?\s*(.+)$/);
  if (mSku) return { type: 'sku', sku: mSku[1].trim() };

  const mQuery = t.match(/^查(?:詢)?\s*(.+)$/);
  if (mQuery) return { type: 'query', keyword: mQuery[1].trim() };

  // 出/入 指令（含可選倉庫）
  const mChange = t.match(/^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散|件))?(?:\s*(?:@|（?\(?倉庫[:：=]\s*)([^)）]+)\)?)?\s*$/);
  if (mChange) {
    const box = mChange[2] ? parseInt(mChange[2], 10) : 0;
    const piece = mChange[3] ? parseInt(mChange[3], 10) : 0;
    const warehouse = (mChange[4] || '').trim();
    return {
      type: 'change',
      action: /入/.test(mChange[1]) ? 'in' : 'out',
      box,
      piece,
      warehouse: warehouse || null
    };
  }
  return null;
}

/* ========= 使用者/分店解析 ========= */
async function resolveBranchAndRole(event) {
  const source = event.source || {};
  const userId = source.userId || null;
  const isGroup = source.type === 'group';
  const groupId = isGroup ? source.groupId : null;

  let role = 'user';
  let blocked = false;
  try {
    if (userId) {
      const { data: u } = await supabase
        .from('users')
        .select('角色, 黑名單, 群組')
        .eq('user_id', userId)
        .maybeSingle();
      role = u?.角色 || 'user';
      blocked = !!u?.黑名單;
    }
  } catch (_) {}

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

/* ========= 查詢輔助 ========= */
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

/* ========= GAS 近即時推送 ========= */
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
    const res = await safeFetch(callUrl, {
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

/* ========= 倉庫分層 ========= */
async function getWarehouseStockBySku(branch, sku) {
  const branchId = await getBranchIdByGroupCode(branch);
  if (!branchId) return [];
  const { data, error } = await supabase
    .from('inventory_lots')
    .select('warehouse_name, uom, qty_left')
    .eq('branch_id', branchId)
    .eq('product_sku', sku);
  if (error) throw error;
  const map = new Map(); // key = warehouse_name
  (data || []).forEach(r => {
    const w = String(r.warehouse_name || '未指定');
    const u = String(r.uom || '').toLowerCase();
    const q = Number(r.qty_left || 0);
    if (!map.has(w)) map.set(w, { box:0, piece:0 });
    const obj = map.get(w);
    if (u === 'box') obj.box += q;
    else if (u === 'piece') obj.piece += q;
  });
  return Array.from(map.entries()).map(([warehouse, v]) => ({ warehouse, ...v }))
    .filter(x => x.box > 0 || x.piece > 0);
}

function buildQuickReplyForProducts(products) {
  const items = products.slice(0, 12).map(p => ({
    type: 'action',
    action: { type: 'message', label: `${p['貨品名稱']}`.slice(0, 20), text: `編號 ${p['貨品編號']}` }
  }));
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

/* ========= FIFO 出庫（lots） ========= */
async function callFifoOutLots(branch, sku, uom, qty, warehouseName, lineUserId) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) {
    const hint = '此 LINE 使用者尚未對應到 auth.users。請先在 line_user_map 建立對應。';
    throw new Error(`找不到對應的使用者（${lineUserId}）。${hint}`);
  }
  if (qty <= 0) return { consumed: 0, cost: null };
  const { data, error } = await supabase.rpc('fifo_out_lots', {
    p_group: branch,
    p_sku: sku,
    p_uom: uom,
    p_qty: qty,
    p_warehouse_name: warehouseName || '',
    p_user_id: authUuid,
    p_source: 'LINE',
    p_now: new Date().toISOString() // UTC；GAS 會做 +08:00 視窗
  });
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  return { consumed: Number(row?.consumed || 0), cost: row?.cost ?? null };
}

/* ========= Health ========= */
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));

/* ========= LINE Webhook ========= */
const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };
app.get('/webhook', (_req, res) => res.status(200).send('OK'));
app.get('/line/webhook', (_req, res) => res.status(200).send('OK'));
app.post('/webhook',      line.middleware(lineConfig), lineHandler);
app.post('/line/webhook', line.middleware(lineConfig), lineHandler);

// 其餘自訂 API 再掛 jsonParser
// app.post('/some/api', jsonParser, async (req, res) => { /* ... */ });

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

/* ========= 主流程 ========= */
async function handleEvent(event) {
  if (event.type !== 'message' || event.message.type !== 'text') return;
  const text = event.message.text || '';
  const parsed = parseCommand(text);
  if (!parsed) return;

  const source = event.source || {};
  const isGroup = source.type === 'group';
  const lineUserId = source.userId || null;

  try {
    if (!isGroup && lineUserId) await autoRegisterUser(lineUserId);

    const { branch, role, blocked, needBindMsg } = await resolveBranchAndRole(event);
    if (blocked) return;

    const reply = (messageObj) => client.replyMessage(event.replyToken, messageObj);
    const replyText = (textStr) => reply({ type: 'text', text: textStr });

    if (!branch) {
      await replyText(needBindMsg || '此使用者/群組尚未綁定分店，請管理員設定');
      return;
    }

    const inStockSet = role === 'user' ? await getInStockSkuSet(branch) : new Set();

    /* ===== 查詢 ===== */
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
      const boxSize = p['箱入數'] ?? '-';
      const price = p['單價'] ?? '-';
      await replyText(`名稱：${p['貨品名稱']}\n編號：${sku}\n箱入數：${boxSize}\n單價：${price}\n庫存：${s.box}箱${s.piece}散`);
      return;
    }

    if (parsed.type === 'barcode') {
      const list = await searchByBarcode(parsed.barcode, role, branch, inStockSet);
      if (!list.length) { await replyText(role === '主管' ? '查無此條碼商品' : '無此商品庫存'); return; }
      const p = list[0];
      const sku = p['貨品編號'];
      const s = await getStockByGroupSku(branch, sku);
      if (role === 'user' && s.box === 0 && s.piece === 0) { await replyText('無此商品庫存'); return; }
      await upsertUserLastProduct(lineUserId, branch, sku);
      const boxSize = p['箱入數'] ?? '-';
      const price = p['單價'] ?? '-';
      await replyText(`名稱：${p['貨品名稱']}\n編號：${sku}\n箱入數：${boxSize}\n單價：${price}\n庫存：${s.box}箱${s.piece}散`);
      return;
    }

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
      const boxSize = p['箱入數'] ?? '-';
      const price = p['單價'] ?? '-';
      await replyText(`名稱：${p['貨品名稱']}\n編號：${sku}\n箱入數：${boxSize}\n單價：${price}\n庫存：${s.box}箱${s.piece}散`);
      return;
    }

    /* ===== 入/出 庫 ===== */
    if (parsed.type === 'change') {
      if (parsed.action === 'in' && role !== '主管') { await replyText('您無法使用「入庫」'); return; }
      if (parsed.box === 0 && parsed.piece === 0) { await replyText('請輸入數量'); return; }

      const sku = await getLastSku(lineUserId, branch);
      if (!sku) { await replyText('請先用「查 商品」或「條碼123 / 編號ABC」選定商品後再入/出庫。'); return; }

      // 出庫需決定倉庫
      if (parsed.action === 'out' && !parsed.warehouse) {
        const list = await getWarehouseStockBySku(branch, sku);
        if (list.length >= 2) {
          const qr = buildQuickReplyForWarehouses('出', list, parsed.box, parsed.piece);
          await reply({ type: 'text', text: `請選擇要出庫的倉庫`, quickReply: qr });
          return;
        }
        if (list.length === 1) {
          parsed.warehouse = list[0].warehouse;
        }
      }

      try {
        if (parsed.action === 'out') {
          const wh = parsed.warehouse || '未指定';

          // 箱
          if (parsed.box > 0) {
            await callFifoOutLots(branch, sku, 'box', parsed.box, wh, lineUserId);
          }
          // 散
          if (parsed.piece > 0) {
            await callFifoOutLots(branch, sku, 'piece', parsed.piece, wh, lineUserId);
          }

          const { data: prodRow } = await supabase
            .from('products')
            .select('貨品名稱, 箱入數, 單價')
            .eq('貨品編號', sku)
            .maybeSingle();
          const prodName = prodRow?.['貨品名稱'] || sku;
          const s = await getStockByGroupSku(branch, sku);

          // 推到 GAS（僅顯示用途）
          const unitsPerBox = Number(String(prodRow?.['箱入數'] ?? '1').replace(/[^\d]/g, '')) || 1;
          const unitPrice   = Number(String(prodRow?.['單價']   ?? '0').replace(/[^0-9.]/g, '')) || 0;
          const outPiecesAbs = (parsed.box * unitsPerBox) + parsed.piece;
          const payload = {
            type: 'log',
            group: String(branch || '').trim().toLowerCase(),
            sku,
            name: prodName,
            units_per_box: unitsPerBox,
            unit_price: unitPrice,
            in_box: 0, in_piece: 0,
            out_box: parsed.box, out_piece: parsed.piece,
            stock_box: s.box, stock_piece: s.piece,
            out_amount: outPiecesAbs * unitPrice,
            stock_amount: (s.box * unitsPerBox + s.piece) * unitPrice,
            warehouse: wh,
            created_at: formatTpeIso(new Date())
          };
          postInventoryToGAS(payload).catch(()=>{});

          await replyText(`✅ 出庫成功（倉庫：${wh}）\n貨品：${prodName}\n出庫：${parsed.box || 0}箱 ${parsed.piece || 0}件\n目前庫存：${s.box}箱${s.piece}散`);
          return;
        }

        // 入庫（仍走群組彙總舊路；若要改 lots 入庫，告訴我你要帶的欄位）
        const deltaBox = parsed.box;
        const deltaPiece = parsed.piece;
        const r = await changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, 'LINE');
        let nb = null, np = null;
        if (r && typeof r.new_box === 'number') nb = r.new_box;
        if (r && typeof r.new_piece === 'number') np = r.new_piece;
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
  } catch (e) {
    // 最外層保底：避免「沒反應」
    try {
      await client.replyMessage(event.replyToken, { type: 'text', text: `處理失敗：${e?.message || e || '未知錯誤'}` });
    } catch (_) {
      console.error('[REPLY FAIL]', _);
    }
  }
}

/* ========= Start ========= */
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT} (${NODE_ENV})`);
});
