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
  // GAS：環境變數優先；不足則自動從 DB RPC 補上
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

// === 全域請求記錄器（不解析 body，不會影響 LINE 簽章） ===
app.use((req, _res, next) => {
  console.log(`[請求] ${req.method} ${req.path} ua=${req.headers['user-agent'] || ''} x-line-signature=${req.headers['x-line-signature'] ? 'yes' : 'no'}`);
  next();
});

// 只在需要的 API 掛 JSON parser，避免破壞 LINE 簽章
const jsonParser = express.json();

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY);

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

/** 指令解析（保留原本規則） */
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

  const mChange = t.match(/^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散|件))?(?:\s*(\d+))?$/);
  if (mChange) {
    const box = mChange[2] ? parseInt(mChange[2], 10) : 0;
    const pieceLabeled = mChange[3] ? parseInt(mChange[3], 10) : 0;
    const pieceTail = mChange[4] ? parseInt(mChange[4], 10) : 0;
    return {
      type: 'change',
      action: /入/.test(mChange[1]) ? 'in' : 'out',
      box,
      piece: pieceLabeled || pieceTail
    };
  }
  return null;
}

/** 解析分店與角色 */
async function resolveBranchAndRole(event) {
  const source = event.source || {};
  theUser: {
    // no-op, just a scope label for readability
  }
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

/** ===== 倉庫別 → kind_name 對應 ===== */
async function resolveWarehouseKindName(code) {
  const c = String(code || '').trim();
  if (!c || c === '未指定') return '未指定';
  // 先查 warehouse_kinds（你提供的表）
  const { data, error } = await supabase
    .from('warehouse_kinds')
    .select('kind_name, is_active')
    .eq('倉庫別', c)
    .maybeSingle();
  if (!error && data && (data.is_active === null || data.is_active === true)) {
    const name = (data.kind_name || '').toString().trim();
    if (name) return name;
  }
  // 回退：直接用 code
  return c;
}

/** RPC：變更庫存（LINE userId 先轉 auth uuid） */
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

/** ===== GAS Webhook 自動載入（ENV 優先，否則 DB RPC 取值） ===== */
let GAS_URL_CACHE = (ENV_GAS_URL || '').trim();
let GAS_SECRET_CACHE = (ENV_GAS_SECRET || '').trim();
let GAS_LOADED_ONCE = false;

async function loadGasConfigFromDBIfNeeded() {
  if (GAS_URL_CACHE && GAS_SECRET_CACHE) { GAS_LOADED_ONCE = true; return; }
  try {
    const { data, error } = await supabase
      .rpc('get_app_settings', { keys: ['gas_webhook_url', 'gas_webhook_secret'] }); // 需先建立 public.get_app_settings
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
    if (GAS_URL_CACHE && GAS_SECRET_CACHE) {
      console.log('✅ GAS Webhook 設定已載入（public RPC）');
    } else {
      console.warn('⚠️ GAS Webhook 設定缺少（可設定環境變數或 app.app_settings）');
    }
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

/** 台北時區 +08:00 的 ISO（供 GAS 5:00 分界使用） */
function formatTpeIso(date = new Date()) {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(date);
  return s.replace(' ', 'T') + '+08:00';
}

/** 推送到 GAS（優先用 ENV；否則用 DB 取得） */
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

/** LINE quick reply */
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

/** 健康檢查 & 根路徑 */
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));

/** ===== LINE Webhook（同時支援 /webhook 與 /line/webhook；不要掛任何 body parser） ===== */
const lineConfig = { channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN, channelSecret: LINE_CHANNEL_SECRET };

// LINE 的 Verify 會打 GET → 回 200
app.get('/webhook', (_req, res) => res.status(200).send('OK'));
app.get('/line/webhook', (_req, res) => res.status(200).send('OK'));

// 真正處理事件：兩條路徑都掛 line.middleware
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

// 專門接 line.middleware 發生的錯（簽章錯等）
app.use((err, req, res, next) => {
  if (req.path === '/webhook' || req.path === '/line/webhook') {
    console.error('[LINE MIDDLEWARE ERROR]', err?.message || err);
    return res.status(400).end();
  }
  return next(err);
});

/** ====== 指令主處理（保留你的邏輯） ====== */
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
    thePrice: {
      // keep naming style with your original code style
    }
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

  if (parsed.type === 'change') {
    if (parsed.action === 'in' && role !== '主管') { await replyText('您無法使用「入庫」'); return; }
    if (parsed.box === 0 && parsed.piece === 0) return;

    const sku = await getLastSku(lineUserId, branch);
    if (!sku) { await replyText('請先用「查 商品」或「條碼123 / 編號ABC」選定商品後再入/出庫。'); return; }

    const deltaBox = parsed.action === 'in' ? parsed.box : -parsed.box;
    const deltaPiece = parsed.action === 'in' ? parsed.piece : -parsed.piece;

    try {
      const r = await changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, 'LINE');
      let nb = null, np = null;
      if (r && typeof r.new_box === 'number') nb = r.new_box;
      if (r && typeof r.new_piece === 'number') np = r.new_piece;
      if (nb === null || np === null) {
        const s = await getStockByGroupSku(branch, sku);
        nb = s.box; np = s.piece;
      }

      const { data: prodRow } = await supabase
        .from('products')
        .select('貨品名稱, 箱入數, 單價')
        .eq('貨品編號', sku)
        .maybeSingle();
      const prodName = prodRow?.['貨品名稱'] || sku;

      const unitsPerBox = Number(String(prodRow?.['箱入數'] ?? '1').replace(/[^\d]/g, '')) || 1;
      const unitPrice   = Number(String(prodRow?.['單價']   ?? '0').replace(/[^0-9.]/g, '')) || 0;

      const deltaPiecesAbs = Math.abs(deltaBox) * unitsPerBox + Math.abs(deltaPiece);
      const outAmount = (deltaBox < 0 || deltaPiece < 0) ? deltaPiecesAbs * unitPrice : 0;
      const stockAmount = (nb * unitsPerBox + np) * unitPrice;

      const payload = {
        type: 'log',
        group: String(branch || '').trim().toLowerCase(),
        sku,
        name: prodName,
        units_per_box: unitsPerBox,
        unit_price: unitPrice,
        in_box: Math.max(deltaBox, 0),
        in_piece: Math.max(deltaPiece, 0),
        out_box: Math.max(-deltaBox, 0),
        out_piece: Math.max(-deltaPiece, 0),
        stock_box: nb,
        stock_piece: np,
        out_amount: outAmount,
        stock_amount: stockAmount,
        created_at: formatTpeIso(new Date())
      };
      postInventoryToGAS(payload).catch(()=>{});

      await replyText(`${parsed.action === 'in' ? '✅ 入庫成功' : '✅ 出庫成功'}\n貨品名稱 📄：${prodName}\n目前庫存：${nb}箱${np}散`);
      return;
    } catch (err) {
      console.error('change error:', err);
      await replyText(`操作失敗：${err?.message || '未知錯誤'}`);
      return;
    }
  }
  return;
}

/** ===== App 入庫（只對這條掛 jsonParser） =====
 *  🔴 重點：payload.warehouse 會送「kind_name」而非「倉庫別(code)」
 */
app.post('/app/inbound', jsonParser, async (req, res) => {
  try {
    const authz = req.headers.authorization || '';
    const m = authz.match(/^Bearer\s+(.+)$/i);
    if (!m) return res.status(401).json({ error: 'NO_AUTH' });
    const accessToken = m[1];

    // 驗證使用者
    const { data: userRes, error: authErr } = await supabase.auth.getUser(accessToken);
    if (authErr || !userRes?.user?.id) {
      return res.status(401).json({ error: 'INVALID_TOKEN' });
    }
    const userId = userRes.user.id;

    // 解析 body
    const {
      product_sku,
      in_box = 0,
      in_piece = 0,
      unit_cost_piece,         // 每件成本（必要）
      warehouse_code = '未指定'
    } = req.body || {};

    const sku = String(product_sku || '').trim().toUpperCase();
    if (!sku) return res.status(400).json({ error: 'SKU_REQUIRED' });

    const box = Number.isFinite(+in_box) ? parseInt(in_box, 10) : 0;
    const piece = Number.isFinite(+in_piece) ? parseInt(in_piece, 10) : 0;
    if (box < 0 || piece < 0 || (box === 0 && piece === 0)) {
      return res.status(400).json({ error: 'INVALID_QTY' });
    }

    const unitCost = Number(unit_cost_piece);
    if (!Number.isFinite(unitCost) || unitCost < 0) {
      return res.status(400).json({ error: 'INVALID_UNIT_COST' });
    }

    // 取得分店與群組
    const { branch_id, group } = await (async () => {
      const { data: prof, error: e1 } = await supabase
        .from('profiles').select('branch_id').eq('user_id', userId).maybeSingle();
      if (e1) throw e1;
      const branch_id = (prof?.branch_id ?? null);
      if (!branch_id) throw new Error('找不到使用者分店設定');

      const { data: br, error: e2 } = await supabase
        .from('branches').select('分店代號').eq('id', branch_id).maybeSingle();
      if (e2) throw e2;
      const code = (br?.['分店代號'] || '').toString().trim();
      if (!code) throw new Error('分店缺少分店代號');
      return { branch_id, group: code.toLowerCase() };
    })();

    // 商品資訊
    const { data: prod, error: prodErr } = await supabase
      .from('products')
      .select('貨品名稱, 箱入數, 單價')
      .eq('貨品編號', sku)
      .maybeSingle();
    if (prodErr) throw prodErr;
    if (!prod) throw new Error(`找不到商品：${sku}`);
    const name = prod['貨品名稱'] || sku;
    const units_per_box = Number(String(prod['箱入數'] ?? '1').replace(/[^\d]/g, '')) || 1;

    // 變動庫存
    const { error: changeErr } = await supabase.rpc('exec_change_inventory_by_group_sku', {
      p_group: group,
      p_sku: sku,
      p_delta_box: box,
      p_delta_piece: piece,
      p_user_id: userId,
      p_source: 'APP'
    });
    if (changeErr) throw changeErr;

    // 寫入 lots（帶 warehouse_code）
    const totalPieces = (box * units_per_box) + piece;
    const nowIso = new Date().toISOString();
    try {
      await supabase.from('inventory_lots').insert({
        branch_id,
        product_sku: sku,
        uom: 'piece',
        qty_in: totalPieces,
        unit_cost: unitCost,
        created_at: nowIso,
        created_by: userRes.user.email || userId,
        warehouse_code: warehouse_code,
      });
    } catch {
      await supabase.from('inventory_lots').insert({
        branch_id,
        product_sku: sku,
        uom: 'piece',
        qty_in: totalPieces,
        unit_cost: unitCost,
        created_at: nowIso,
        created_by: userRes.user.email || userId,
      });
    }

    // 讀現量
    const { data: invRow, error: invErr } = await supabase
      .from('inventory')
      .select('庫存箱數, 庫存散數')
      .eq('群組', group)
      .eq('貨品編號', sku)
      .maybeSingle();
    if (invErr) throw invErr;
    const stock_box = Number(invRow?.['庫存箱數'] ?? 0);
    const stock_piece = Number(invRow?.['庫存散數'] ?? 0);

    // ★ 倉庫別轉 kind_name 供 GAS 顯示
    const warehouse_display = await resolveWarehouseKindName(warehouse_code);

    // 推 GAS（05:00 分界靠 GAS 端處理）
    const unitPrice = unitCost;
    const outAmount = 0;
    const stockAmount = (stock_box * units_per_box + stock_piece) * unitPrice;
    const payload = {
      type: 'log',
      group,
      sku,
      name,
      units_per_box,
      unit_price: unitPrice,
      in_box: box,
      in_piece: piece,
      out_box: 0,
      out_piece: 0,
      stock_box,
      stock_piece,
      out_amount: outAmount,
      stock_amount: stockAmount,
      warehouse: warehouse_display, // ★ 改成傳 kind_name
      created_at: formatTpeIso(new Date())
    };
    postInventoryToGAS(payload).catch(()=>{});

    return res.json({
      ok: true,
      sku,
      name,
      units_per_box,
      stock_box,
      stock_piece,
      warehouse_display
    });
  } catch (e) {
    console.error('[APP INBOUND ERROR]', e);
    return res.status(500).json({ error: e?.message || 'SERVER_ERROR' });
  }
});

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
