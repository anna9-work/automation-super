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
  DEFAULT_BRANCH_ID
} = process.env;

// 基本檢查
if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) {
  console.error('缺少 LINE 環境變數');
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('缺少 Supabase 環境變數 (URL / SERVICE_ROLE_KEY)');
  process.exit(1);
}
if (!DEFAULT_BRANCH_ID) {
  console.warn('⚠️ 未提供 DEFAULT_BRANCH_ID，請在 Railway Variables 設定');
}

const app = express();
app.use(express.json());

// LINE
const lineConfig = {
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: LINE_CHANNEL_SECRET
};
const client = new line.Client(lineConfig);

// Supabase（用 Service Role Key 呼叫 RPC）
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// ===== 工具 =====
function parseCommand(text) {
  const t = text.trim();

  // 查詢：「查 xxx」或「查xxx」
  const q = t.match(/^查詢?\s*(.+)$/);
  if (q) return { type: 'query', keyword: q[1].trim() };

  // 條碼查詢：「條碼 123456」
  const b = t.match(/^條碼\s+(.+)$/);
  if (b) return { type: 'barcode', barcode: b[1].trim() };

  // 出入庫：入庫3箱2件 / 入3箱 / 入3件 / 出庫1箱 / 出2件
  const c = t.match(/^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*件)?$/);
  if (c) {
    return {
      type: 'change',
      action: /入/.test(c[1]) ? 'in' : 'out',
      box: c[2] ? parseInt(c[2], 10) : 0,
      piece: c[3] ? parseInt(c[3], 10) : 0
    };
  }
  return null;
}

// 先條碼精準、再名稱模糊。回傳含「貨品編號」以便記錄 last product。
async function findProductsByKeyword(keyword) {
  // 條碼精準
  let { data: byBarcode, error: e1 } = await supabase
    .from('products')
    .select('id, name, barcode, "貨品編號"')
    .eq('barcode', keyword)
    .limit(1)
    .maybeSingle();
  if (e1) throw e1;
  if (byBarcode) return [byBarcode];

  // 名稱模糊（最多 5 筆）
  const { data: byName, error: e2 } = await supabase
    .from('products')
    .select('id, name, barcode, "貨品編號"')
    .ilike('name', `%${keyword}%`)
    .limit(5);
  if (e2) throw e2;
  return byName || [];
}

async function findProductByBarcode(barcode) {
  const { data, error } = await supabase
    .from('products')
    .select('id, name, barcode, "貨品編號"')
    .eq('barcode', barcode)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function getStock(branchId, productId) {
  const { data, error } = await supabase
    .from('inventory')
    .select('quantity_box, quantity_piece')
    .eq('branch_id', branchId)
    .eq('product_id', productId)
    .maybeSingle();
  if (error) throw error;
  return { box: data?.quantity_box ?? 0, piece: data?.quantity_piece ?? 0 };
}

// 以「貨品編號」記錄最後查詢商品（沿用你現有表結構）
async function upsertUserLastProduct(lineUserId, sku) {
  const group = 'default'; // 若你有實際「群組」邏輯，可在此替換
  const { data } = await supabase
    .from('user_last_product')
    .select('id')
    .eq('user_id', lineUserId)
    .eq('群組', group)
    .maybeSingle();

  const now = new Date().toISOString();
  if (data) {
    await supabase
      .from('user_last_product')
      .update({ '貨品編號': sku, 建立時間: now })
      .eq('user_id', lineUserId)
      .eq('群組', group);
  } else {
    await supabase
      .from('user_last_product')
      .insert({ user_id: lineUserId, '貨品編號': sku, 群組: group, 建立時間: now });
  }
}

// 從 user_last_product 取出「貨品編號」，再對回 products 拿到 product.id
async function getLastProduct(lineUserId) {
  const group = 'default';
  const { data: last, error } = await supabase
    .from('user_last_product')
    .select('貨品編號')
    .eq('user_id', lineUserId)
    .eq('群組', group)
    .order('建立時間', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  const sku = last?.['貨品編號'];
  if (!sku) return null;

  // 嘗試三種對應：貨品編號 / 條碼 / 直接等於 id（如果你曾用 id 存過）
  const { data: product, error: e2 } = await supabase
    .from('products')
    .select('id, name, barcode, "貨品編號"')
    .or(`"貨品編號".eq.${sku},barcode.eq.${sku},id.eq.${sku}`)
    .limit(1)
    .maybeSingle();
  if (e2) throw e2;

  return product || null;
}

// 呼叫資料庫 RPC：分別加減箱/件、不換算，寫 logs、防負庫存
async function changeInventory(branchId, productId, deltaBox, deltaPiece, actorUserId = null) {
  const { data, error } = await supabase.rpc('exec_change_inventory_sql', {
    p_branch: branchId,
    p_product: productId,
    p_delta_box: deltaBox,
    p_delta_piece: deltaPiece,
    p_actor: actorUserId
  });
  if (error) throw error;
  return data; // { new_box, new_piece }
}

// ==== 路由 ====
app.get('/health', (_req, res) => res.status(200).send('OK'));

app.post('/webhook', async (req, res) => {
  const events = req.body?.events || [];
  for (const ev of events) {
    try { await handleEvent(ev); } catch (err) { console.error('handleEvent error:', err); }
  }
  res.status(200).send('OK');
});

async function handleEvent(event) {
  if (event.type !== 'message' || event.message.type !== 'text') return;

  const text = event.message.text || '';
  const parsed = parseCommand(text);
  const reply = (msg) => client.replyMessage(event.replyToken, { type: 'text', text: msg });

  if (!parsed) {
    return reply([
      '指令例：',
      '1) 查 可樂 / 查 123456（條碼或關鍵字）',
      '2) 入庫3箱2件 / 入3箱 / 入3件',
      '3) 出庫1箱 / 出2件',
      '4) 條碼 123456'
    ].join('\n'));
  }

  const branchId = DEFAULT_BRANCH_ID;
  if (!branchId) return reply('尚未設定 DEFAULT_BRANCH_ID，請先設定後再試。');

  const lineUserId = event.source?.userId || 'unknown';

  // 查詢
  if (parsed.type === 'query') {
    const list = await findProductsByKeyword(parsed.keyword);
    if (!list.length) return reply('查無此商品');

    // 回最多 5 筆並顯示庫存；把第一筆的「貨品編號」記為 last
    const lines = [];
    for (const p of list) {
      const s = await getStock(branchId, p.id);
      lines.push(`${p.name}${p.barcode ? `（${p.barcode}）` : ''}\n庫存：箱 ${s.box}、件 ${s.piece}`);
    }
    const sku = list[0]['貨品編號'] || list[0].barcode || list[0].id;
    await upsertUserLastProduct(lineUserId, sku);
    return reply(lines.join('\n\n'));
  }

  // 條碼查詢
  if (parsed.type === 'barcode') {
    const p = await findProductByBarcode(parsed.barcode);
    if (!p) return reply('查無此條碼商品');
    const s = await getStock(branchId, p.id);
    const sku = p['貨品編號'] || p.barcode || p.id;
    await upsertUserLastProduct(lineUserId, sku);
    return reply(`${p.name}${p.barcode ? `（${p.barcode}）` : ''}\n庫存：箱 ${s.box}、件 ${s.piece}`);
  }

  // 出入庫
  if (parsed.type === 'change') {
    if (parsed.box === 0 && parsed.piece === 0) return reply('數量為 0，請輸入箱或件。');

    // 用最後查詢的「貨品編號」找回 product.id
    const last = await getLastProduct(lineUserId);
    if (!last) return reply('請先「查 商品」或「條碼 123」選定商品後再入/出庫。');

    const deltaBox = parsed.action === 'in' ? parsed.box : -parsed.box;
    const deltaPiece = parsed.action === 'in' ? parsed.piece : -parsed.piece;

    try {
      const r = await changeInventory(branchId, last.id, deltaBox, deltaPiece, null);
      const sign = (n) => (n >= 0 ? `+${n}` : `${n}`);
      return reply(`${last.name}\n變動：箱 ${sign(deltaBox)}、件 ${sign(deltaPiece)}\n目前庫存：箱 ${r.new_box}、件 ${r.new_piece}`);
    } catch (err) {
      return reply(`操作失敗：${err?.message || '未知錯誤'}`);
    }
  }
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
