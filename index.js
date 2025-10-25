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
  GAS_WEBHOOK_URL,          // https://script.google.com/macros/s/XXX/exec
  GAS_WEBHOOK_SECRET        // 與 Apps Script Script Properties 的 WEBHOOK_SECRET 一致
} = process.env;

if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) {
  console.error('缺少 LINE 環境變數');
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('缺少 Supabase 環境變數 (URL / SERVICE_ROLE_KEY)');
}
if (!GAS_WEBHOOK_URL || !GAS_WEBHOOK_SECRET) {
  console.error('缺少 GAS_WEBHOOK_URL / GAS_WEBHOOK_SECRET（即時推送到試算表會失效）');
}

const app = express();
app.use(express.json());

const client = new line.Client({ channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN });
const supabase = createClient(SUPABASE_URL.replace(/\/+$/, ''), SUPABASE_SERVICE_ROLE_KEY);

/** 共用：台北時區 ISO（+08:00） */
function formatTpeIso(date = new Date()) {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Taipei',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(date);
  return s.replace(' ', 'T') + '+08:00';
}

/** 共用：推送 GAS */
async function postInventoryToGAS(payload) {
  if (!GAS_WEBHOOK_URL || !GAS_WEBHOOK_SECRET) return;
  const url = `${GAS_WEBHOOK_URL.replace(/\?+.*/, '')}?secret=${encodeURIComponent(GAS_WEBHOOK_SECRET)}`;
  try {
    const res = await fetch(url, {
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

/** 共用：查 group(分店代號小寫) 與 branch_id */
async function getUserBranchAndGroup(userId /* auth.users.id */) {
  const { data: prof, error: e1 } = await supabase
    .from('profiles')
    .select('branch_id')
    .eq('user_id', userId)
    .maybeSingle();
  if (e1) throw e1;
  const branch_id = (prof?.branch_id ?? null);
  if (!branch_id) throw new Error('找不到使用者分店設定');

  const { data: br, error: e2 } = await supabase
    .from('branches')
    .select('分店代號')
    .eq('id', branch_id)
    .maybeSingle();
  if (e2) throw e2;
  const code = (br?.['分店代號'] || '').toString().trim();
  if (!code) throw new Error('分店缺少分店代號');
  return { branch_id, group: code.toLowerCase() };
}

/** 共用：取商品資訊 */
async function getProductBasic(sku) {
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 箱入數, 單價')
    .eq('貨品編號', sku)
    .maybeSingle();
  if (error) throw error;
  if (!data) throw new Error(`找不到商品：${sku}`);
  const name = data['貨品名稱'] || sku;
  const units_per_box = Number(String(data['箱入數'] ?? '1').replace(/[^\d]/g, '')) || 1;
  const unit_price_ref = Number(String(data['單價'] ?? '0').replace(/[^0-9.]/g, '')) || 0;
  return { name, units_per_box, unit_price_ref };
}

/** 共用：讀 inventory 現量 */
async function getStockByGroupSku(group, sku) {
  const { data, error } = await supabase
    .from('inventory')
    .select('庫存箱數, 庫存散數')
    .eq('群組', group)
    .eq('貨品編號', sku)
    .maybeSingle();
  if (error) throw error;
  return {
    box: Number(data?.['庫存箱數'] ?? 0),
    piece: Number(data?.['庫存散數'] ?? 0),
  };
}

/** 依 LINE userId 找 auth uuid（舊 LINE 流程仍需） */
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

/** =========================
 *  App 統一路徑：入庫（後端驗證→寫庫存→推 GAS）
 *  POST /app/inbound
 *  Authorization: Bearer <Supabase Access Token>
 *  body: { product_sku, in_box, in_piece, unit_cost_piece, warehouse_code }
 * ========================= */
app.post('/app/inbound', async (req, res) => {
  try {
    const authz = req.headers.authorization || '';
    const m = authz.match(/^Bearer\s+(.+)$/i);
    if (!m) return res.status(401).json({ error: 'NO_AUTH' });
    const accessToken = m[1];

    // 1) 驗證使用者
    const { data: userRes, error: authErr } = await supabase.auth.getUser(accessToken);
    if (authErr || !userRes?.user?.id) {
      return res.status(401).json({ error: 'INVALID_TOKEN' });
    }
    const userId = userRes.user.id;

    // 2) 解析 body
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

    // 3) 取得分店與群組
    const { branch_id, group } = await getUserBranchAndGroup(userId);

    // 4) 商品資訊
    const { name, units_per_box } = await getProductBasic(sku);

    // 5) 先做庫存變動（箱/散分開；與既有 RPC 一致）
    const deltaBox = box;
    const deltaPiece = piece;

    // 將 app 來源統一為 'APP'
    const { data: changed, error: changeErr } = await supabase.rpc('exec_change_inventory_by_group_sku', {
      p_group: group,
      p_sku: sku,
      p_delta_box: deltaBox,
      p_delta_piece: deltaPiece,
      p_user_id: userId,
      p_source: 'APP'
    });
    if (changeErr) throw changeErr;

    // 6) 寫入成本批次（inventory_lots），最佳努力帶 warehouse_code
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
    } catch (e) {
      // 後端欄位若尚未建立 warehouse_code，降級不帶它
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

    // 7) 取最新現量，計算金額
    const stockNow = await getStockByGroupSku(group, sku);
    const unitPrice = unitCost; // 入庫以實際成本推估金額
    const outAmount = 0;
    const stockAmount = (stockNow.box * units_per_box + stockNow.piece) * unitPrice;

    // 8) 推送 GAS（05:00 分界靠 GAS 端處理）
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
      created_at: formatTpeIso(new Date()) // +08:00
    };
    postInventoryToGAS(payload).catch(()=>{});

    // 9) 回覆前端
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

/** ========= 既有 LINE webhook 區塊（原封不動，略） =========
 *  如果你需要我把整段 LINE webhook 也一起貼回來，說一聲我補完整檔。
 *  這裡保留健康檢查與啟動。
 */
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
