import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/**
 * =========================================================
 *  LINE Bot for Inventory public.get_business_day_stock
 *  - fifo_out_and_log
 *  - /public.get_business_day_stock05:00 
 *  - warehouse_kinds(kind_id, kind_name)
 *  - 
 * =========================================================
 */

/* ======== Environment ======== */
const PORT = Number(process.env.PORT || 3000);
const LINE_CHANNEL_ACCESS_TOKEN = (process.env.LINE_CHANNEL_ACCESS_TOKEN || '').trim();
const LINE_CHANNEL_SECRET      = (process.env.LINE_CHANNEL_SECRET || '').trim();

const SUPABASE_URL               = (process.env.SUPABASE_URL || '').trim();
const SUPABASE_SERVICE_ROLE_KEY  = (process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();

const ENV_GAS_URL    = (process.env.GAS_WEBHOOK_URL || '').trim();
const ENV_GAS_SECRET = (process.env.GAS_WEBHOOK_SECRET || '').trim();

if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) console.error(' LINE ');
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) console.error(' Supabase  (URL / SERVICE_ROLE_KEY)');

/* ======== App / Supabase ======== */
const app = express(); //  webhook  body parser
app.use((req, _res, next) => {
  console.log(`[] ${req.method} ${req.path} ua=${req.headers['user-agent']||'-'} x-line-signature=${req.headers['x-line-signature']?'yes':'no'}`);
  next();
});
const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

/* ======== Runtime caches ======== */
const LAST_WAREHOUSE_BY_USER_BRANCH = new Map(); // key=`${userId}::${branch}` → 倉庫名稱（中文）
const SKU_CACHE = new Map(); // 可選，商品快取

/* ======== Helpers ======== */
const skuKey = (s) => {
  const t = String(s || '').trim();
  return t ? (t.slice(0,1).toUpperCase() + t.slice(1).toLowerCase()) : '';
};

function getBizDateTodayTPE() {
  const now = new Date();
  const tpeTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Taipei' }));
  const y = tpeTime.getFullYear();
  const m = String(tpeTime.getMonth() + 1).padStart(2, '0');
  const d = String(tpeTime.getDate()).padStart(2, '0');
  const h = tpeTime.getHours();
  const baseDate = new Date(`${y}-${m}-${d}T12:00:00+08:00`);
  if (h < 5) {
    baseDate.setDate(baseDate.getDate() - 1);
  }
  const yy = baseDate.getFullYear();
  const mm = String(baseDate.getMonth() + 1).padStart(2, '0');
  const dd = String(baseDate.getDate()).padStart(2, '0');
  return `${yy}-${mm}-${dd}`;
}

function tpeNowISO() {
  const now = new Date();
  const tpe = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Taipei' }));
  return tpe.toISOString();
}

/* ======== Warehouse dictionary ======== */
async function getWarehouseDictionary() {
  const { data, error } = await supabase
    .from('warehouse_kinds')
    .select('kind_id, kind_name')
    .order('kind_id', { ascending: true });
  if (error) throw error;
  const map = new Map();
  for (const r of (data || [])) {
    const code = String(r.kind_id || '').trim();
    const name = String(r.kind_name || '').trim();
    if (!code || !name) continue;
    map.set(code.toLowerCase(), name);
    map.set(name, name);
  }
  return map;
}

async function getWarehouseCodeForLabel(label) {
  const nm = String(label || '').trim();
  if (!nm) return null;
  const { data, error } = await supabase
    .from('warehouse_kinds')
    .select('kind_id, kind_name')
    .or(`kind_id.eq.${nm},kind_name.eq.${nm}`)
    .limit(1);
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  if (!row) return null;
  return String(row.kind_id || '').trim();
}

/* ======== LINE user → auth user_id 對應 ======== */
async function resolveAuthUuidFromLineUserId(lineUserId) {
  const { data, error } = await supabase
    .from('line_user_map')
    .select('user_id')
    .eq('line_user_id', lineUserId)
    .limit(1);
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  return row?.user_id || null;
}

/* ======== 取得目前庫存快照 (RPC: get_business_day_stock) ======== */
async function getWarehouseSnapshot(branch, sku, warehouseDisplayName) {
  const group = String(branch||'').trim().toLowerCase();
  const s = String(sku||'').trim();
  const whCode = await getWarehouseCodeForLabel(warehouseDisplayName||'');
  const bizDate = getBizDateTodayTPE();

  const { data, error } = await supabase.rpc('get_business_day_stock', {
    p_group: group,
    p_biz_date: bizDate,
    p_sku: s,
    p_warehouse_code: whCode
  });
  if (error) throw error;

  const row = Array.isArray(data) ? data[0] : data;
  if (!row) return { box:0, piece:0, unitsPerBox:1, unitPricePiece:0, stockAmount:0 };

  const box   = Number(row.stock_box   ?? 0);
  const piece = Number(row.stock_piece ?? 0);
  const unitsPerBox = Number(row.units_per_box || 1);
  const unitPricePiece = Number(row.unit_price_piece || 0);
  const stockAmount = ((box * unitsPerBox) + piece) * unitPricePiece;

  return { box, piece, unitsPerBox, unitPricePiece, stockAmount };
}

/* ======== Product search (products + 關鍵字） ======== */
async function searchByName(keyword, _role, branch) {
  const k = String(keyword||'').trim();
  if (!k) return [];
  const { data, error } = await supabase
    .from('products')
    .select('貨品編號,貨品名稱,箱入數,條碼')
    .or(`貨品編號.ilike.%${k}%,貨品名稱.ilike.%${k}%`)
    .limit(30);
  if (error) throw error;

  return (data||[]).map(r => ({
    sku:  String(r['貨品編號']||'').trim(),
    name: String(r['貨品名稱']||'').trim(),
    unitsPerBox: Number(r['箱入數']||0),
    barcode: String(r['條碼']||'').trim()
  }));
}

/* ======== 單一交易出庫（先查庫存，再呼叫 fifo_out_and_log） ======== */
async function callOutOnceTx({ branch, sku, outBox, outPiece, warehouseLabel, lineUserId }) {
  const authUuid = await resolveAuthUuidFromLineUserId(lineUserId);
  if (!authUuid) throw new Error(`${lineUserId} line_user_map `);

  // ★ 出庫前先查庫存，禁止出到負數
  const snap = await getWarehouseSnapshot(branch, sku, warehouseLabel);
  const unitsPerBox = (snap.unitsPerBox && snap.unitsPerBox > 0) ? snap.unitsPerBox : 1;

  const stockBox   = Number(snap.box   || 0);
  const stockPiece = Number(snap.piece || 0);
  const stockPieces = stockBox * unitsPerBox + stockPiece;

  const reqBox   = Number(outBox   || 0);
  const reqPiece = Number(outPiece || 0);
  const reqPieces = reqBox * unitsPerBox + reqPiece;

  if (reqBox < 0 || reqPiece < 0) {
    throw new Error('出庫數量不可為負數');
  }
  if (reqBox === 0 && reqPiece === 0) {
    throw new Error('出庫數量需大於 0');
  }

  // 庫存不足 → 直接擋下，不呼叫 fifo_out_and_log
  if (reqPieces > stockPieces) {
    const safeStockPieces = Math.max(stockPieces, 0);
    const maxBox   = Math.floor(safeStockPieces / unitsPerBox);
    const maxPiece = safeStockPieces % unitsPerBox;

    throw new Error(
      `庫存不足，可出最多：${maxBox}箱 ${maxPiece}件（目前庫存 ${stockBox}箱 ${stockPiece}件）`
    );
  }

  // ✅ 庫存足夠 → 照舊呼叫 fifo_out_and_log 扣庫存 + 寫紀錄
  const args = {
    p_group:          String(branch||'').trim().toLowerCase(),
    p_sku:            skuKey(sku),
    p_warehouse_name: String(warehouseLabel||'').trim(),
    p_out_box:        String(outBox ?? ''),   // 允許 ''，由 safe_num 接
    p_out_piece:      String(outPiece ?? ''), // 允許 ''，由 safe_num 接
    p_user_id:        authUuid,
    p_source:         'LINE',
    p_at:             new Date().toISOString()
  };

  const { data, error } = await supabase.rpc('fifo_out_and_log', args);
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  if (!row) throw new Error('出庫後回傳資料為空');

  return {
    productName:     row.product_name || sku,
    unitsPerBox:     Number(row.units_per_box   || 1) || 1,
    unitPricePiece:  Number(row.unit_price_piece || 0),
    outBox:          Number(row.out_box   || 0),
    outPiece:        Number(row.out_piece || 0),
    afterBox:        Number(row.after_box   || 0),
    afterPiece:      Number(row.after_piece || 0),
    warehouseName:   String(row.warehouse_name || warehouseLabel || '未指定'),
    stockAmount:     Number(row.stock_amount || 0),
  };
}

/* ======== GAS Webhook (optional, idempotent) ======== */
let GAS_URL_CACHE = (ENV_GAS_URL||'').trim();
let GAS_SECRET_CACHE = (ENV_GAS_SECRET||'').trim();
let GAS_LOADED_ONCE = false;
async function loadGasConfigFromDBIfNeeded() {
  if (GAS_URL_CACHE && GAS_SECRET_CACHE) { GAS_LOADED_ONCE = true; return; }
  try {
    const { data, error } = await supabase.rpc('get_app_settings', { keys: ['gas_webhook_url','gas_webhook_secret'] });
    if (error) throw error;
    if (Array.isArray(data)) {
      for (const row of data) {
        const k = String(row.key||'').trim();
        const v = String(row.value||'').trim();
        if (k==='gas_webhook_url' && v) GAS_URL_CACHE = v;
        if (k==='gas_webhook_secret' && v) GAS_SECRET_CACHE = v;
      }
    }
    GAS_LOADED_ONCE = true;
  } catch (e) {
    console.error('loadGasConfigFromDBIfNeeded error:', e);
    GAS_LOADED_ONCE = true;
  }
}

async function postInventoryToGAS(payload) {
  await loadGasConfigFromDBIfNeeded();
  const url = (GAS_URL_CACHE || '').trim();
  const secret = (GAS_SECRET_CACHE || '').trim();
  if (!url || !secret) {
    console.warn('GAS webhook 未設定（略過推送）');
    return;
  }
  const u = new URL(url.replace(/\?+.*$/, ''));
  u.searchParams.set('secret', secret);

  const res = await fetch(u.toString(), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {})
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('postInventoryToGAS failed:', res.status, txt);
  }
}

/* ======== LINE Webhook handler ======== */
app.post('/webhook', express.json(), async (req, res) => {
  const events = req.body.events || [];
  res.status(200).send('OK');
  for (const ev of events) {
    try {
      await handleEvent(ev);
    } catch (e) {
      console.error('handleEvent top-level error:', e);
    }
  }
});

/* ======== handleEvent: main logic ======== */
async function handleEvent(event) {
  if (event.type !== 'message' || event.message.type !== 'text') return;
  const text = (event.message.text || '').trim();
  const userId  = event.source.userId;
  const replyToken = event.replyToken;

  const replyText = async (msg) => {
    if (!msg) return;
    await client.replyMessage(replyToken, { type: 'text', text: msg });
  };

  // 這裡省略原本的指令解析流程，只保留出庫相關的部分示意
  // ...
  // 假設我們已經解析出:
  //   branch, skuLast, parsed.box, parsed.piece, wh, lineUserId

  // === 出庫流程 ===
  // try {
  //   const result = await callOutOnceTx({
  //     branch,
  //     sku: skuLast,
  //     outBox: parsed.box||0,
  //     outPiece: parsed.piece||0,
  //     warehouseLabel: wh,
  //     lineUserId: userId
  //   });

  //   await replyText(
  //     `✅ 出庫成功
` +
  //     `品名：${result.productName}
` +
  //     `編號：${skuLast}
` +
  //     `倉別：${result.warehouseName}
` +
  //     `出庫：${result.outBox}箱 ${result.outPiece}件
` +
  //     `👉目前庫存：${result.afterBox}箱${result.afterPiece}散`
  //   );

  //   const payload = {
  //     type: 'log',
  //     group: branch,
  //     sku: skuLast,
  //     name: result.productName,
  //     units_per_box: result.unitsPerBox,
  //     unit_price: result.unitPricePiece,
  //     in_box: 0,
  //     in_piece: 0,
  //     out_box: result.outBox,
  //     out_piece: result.outPiece,
  //     stock_box: result.afterBox,
  //     stock_piece: result.afterPiece,
  //     out_amount: result.stockAmount,
  //     stock_amount: result.stockAmount,
  //     warehouse_code: result.warehouseName,
  //     created_at: tpeNowISO()
  //   };
  //   try {
  //     await postInventoryToGAS(payload);
  //   } catch(_) {}
  // } catch (err) {
  //   console.error('change error:', err);
  //   const msg = err?.message || '';
  //   if (msg.includes('庫存不足')) {
  //     await replyText(`❌ 出庫失敗
${msg}`);
  //   } else {
  //     await replyText(`操作失敗：${msg}`);
  //   }
  //   return;
  // }

  await replyText(' App LINE ');
}

/* ======== Start server ======== */
app.listen(PORT, () => { console.log(`Server running on port ${PORT}`); });
