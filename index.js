import 'dotenv/config';
import express from 'express';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

/** =================== 環境變數 =================== */
const {
  PORT = 3000,
  LINE_CHANNEL_ACCESS_TOKEN,
  LINE_CHANNEL_SECRET,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  DEFAULT_GROUP = 'default',
  GAS_WEBHOOK_URL: ENV_GAS_URL,
  GAS_WEBHOOK_SECRET: ENV_GAS_SECRET
} = process.env;

/** =================== 初始化 =================== */
if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) {
  console.error('⚠️ 缺少 LINE 環境變數 (CHANNEL_ACCESS_TOKEN / CHANNEL_SECRET)');
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('⛔️ 缺少 Supabase 環境變數 (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY)');
}

const app = express();
// ⚠️ 不要在這裡 app.use(express.json())，避免破壞 LINE 簽章驗證
const jsonParser = express.json();

const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY);

/** =================== GAS 設定自動載入（public RPC） =================== */
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

/** 工具 */
function formatTpeIso(date = new Date()) {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(date);
  return s.replace(' ', 'T') + '+08:00';
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

/** 共用 DB 查詢 */
async function getUserBranchAndGroup(userId) {
  const { data: prof, error: e1 } = await supabase
    .from('profiles').select('branch_id')
    .eq('user_id', userId).maybeSingle();
  if (e1) throw e1;
  const branch_id = (prof?.branch_id ?? null);
  if (!branch_id) throw new Error('找不到使用者分店設定');

  const { data: br, error: e2 } = await supabase
    .from('branches').select('分店代號')
    .eq('id', branch_id).maybeSingle();
  if (e2) throw e2;
  const code = (br?.['分店代號'] || '').toString().trim();
  if (!code) throw new Error('分店缺少分店代號');
  return { branch_id, group: code.toLowerCase() };
}
async function getProductBasic(sku) {
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 箱入數, 單價')
    .eq('貨品編號', sku).maybeSingle();
  if (error) throw error;
  if (!data) throw new Error(`找不到商品：${sku}`);
  const name = data['貨品名稱'] || sku;
  const units_per_box = Number(String(data['箱入數'] ?? '1').replace(/[^\d]/g, '')) || 1;
  const unit_price_ref = Number(String(data['單價'] ?? '0').replace(/[^0-9.]/g, '')) || 0;
  return { name, units_per_box, unit_price_ref };
}
async function getStockByGroupSku(group, sku) {
  const { data, error } = await supabase
    .from('inventory').select('庫存箱數, 庫存散數')
    .eq('群組', group).eq('貨品編號', sku).maybeSingle();
  if (error) throw error;
  return {
    box: Number(data?.['庫存箱數'] ?? 0),
    piece: Number(data?.['庫存散數'] ?? 0),
  };
}

/** =========================
 *  App 統一路徑：入庫（只對這條掛 jsonParser）
 * ========================= */
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
      unit_cost_piece,
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

    // 分店／群組
    const { branch_id, group } = await getUserBranchAndGroup(userId);

    // 商品資訊
    const { name, units_per_box } = await getProductBasic(sku);

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

    // 取現量、計金額
    const stockNow = await getStockByGroupSku(group, sku);
    const unitPrice = unitCost;
    const outAmount = 0;
    const stockAmount = (stockNow.box * units_per_box + stockNow.piece) * unitPrice;

    // 推 GAS
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
      stock_box: stockNow.box,
      stock_piece: stockNow.piece,
      out_amount: outAmount,
      stock_amount: stockAmount,
      warehouse: String(warehouse_code || '').trim() || '未指定',
      created_at: formatTpeIso(new Date())
    };
    postInventoryToGAS(payload).catch(()=>{});

    return res.json({
      ok: true,
      sku,
      name,
      units_per_box,
      stock_box: stockNow.box,
      stock_piece: stockNow.piece,
    });
  } catch (e) {
    console.error('[APP INBOUND ERROR]', e);
    return res.status(500).json({ error: e?.message || 'SERVER_ERROR' });
  }
});

/** =========================
 *  LINE webhook（不掛任何 body parser！）
 * ========================= */
const lineConfig = {
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: LINE_CHANNEL_SECRET
};
const lineClient = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });

app.post('/line/webhook', line.middleware(lineConfig), async (req, res) => {
  try {
    const events = req.body.events || [];
    if (events.length) {
      console.log(`[LINE] received ${events.length} event(s)`);
    }
    await Promise.all(events.map(handleLineEvent));
    return res.status(200).end();
  } catch (err) {
    console.error('[LINE WEBHOOK HANDLER ERROR]', err);
    return res.status(500).end();
  }
});

async function handleLineEvent(event) {
  try {
    console.log('[LINE EVENT]', {
      type: event.type,
      user: event.source?.userId || '',
      msgType: event.message?.type || '',
      text: event.message?.text || ''
    });

    if (event.type === 'message' && event.message?.type === 'text') {
      const text = (event.message.text || '').trim();

      if (text.toLowerCase() === 'ping') {
        return lineClient.replyMessage(event.replyToken, { type: 'text', text: 'pong' });
      }

      // 簡單回聲
      return lineClient.replyMessage(event.replyToken, { type: 'text', text: `收到：${text}` });
    }

    // 其他事件不回覆
    return Promise.resolve();
  } catch (e) {
    console.error('[LINE EVENT ERROR]', e);
    // 不中斷整批 events
    return Promise.resolve();
  }
}

/** 專門接 line.middleware 錯誤（例如簽章錯誤） */
app.use((err, req, res, next) => {
  if (req.path === '/line/webhook') {
    console.error('[LINE MIDDLEWARE ERROR]', err?.message || err);
    return res.status(400).end();
  }
  return next(err);
});

/** ========= 健康檢查 ========= */
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));

/** ========= 啟動 ========= */
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`   - LINE bot: ${LINE_CHANNEL_ACCESS_TOKEN && LINE_CHANNEL_SECRET ? 'OK' : 'MISSING'}`);
  console.log(`   - Supabase: ${SUPABASE_URL ? 'OK' : 'MISSING'}`);
  console.log(`   - GAS Webhook: ${(ENV_GAS_URL && ENV_GAS_SECRET) ? 'ENV' : 'auto-load via public RPC get_app_settings'}`);
});
