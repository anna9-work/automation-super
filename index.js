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
  DEFAULT_GROUP = 'default' // 若 users 找不到群組時使用
} = process.env;

if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) {
  console.error('缺少 LINE 環境變數'); process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('缺少 Supabase 環境變數 (URL / SERVICE_ROLE_KEY)'); process.exit(1);
}

const app = express();
app.use(express.json());

const client = new line.Client({
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: LINE_CHANNEL_SECRET
});

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// -------- helpers --------
function parseCommand(text) {
  const t = text.trim();
  const q = t.match(/^查詢?\s*(.+)$/);             // 查 xxx / 查詢 xxx
  if (q) return { type: 'query', keyword: q[1].trim() };
  const b = t.match(/^條碼\s+(.+)$/);              // 條碼 123456
  if (b) return { type: 'barcode', barcode: b[1].trim() };
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

async function getUserGroup(userId) {
  const { data, error } = await supabase
    .from('users').select('群組').eq('user_id', userId).maybeSingle();
  if (error) throw error;
  return data?.['群組'] || DEFAULT_GROUP;
}

async function findProductsByKeyword(keyword) {
  // 條碼精準
  let { data: byBarcode, error: e1 } = await supabase
    .from('products')
    .select('貨品名稱, 條碼, 貨品編號')
    .eq('條碼', keyword)
    .limit(1)
    .maybeSingle();
  if (e1) throw e1;
  if (byBarcode) return [byBarcode];

  // 名稱模糊
  const { data: byName, error: e2 } = await supabase
    .from('products')
    .select('貨品名稱, 條碼, 貨品編號')
    .ilike('貨品名稱', `%${keyword}%`)
    .limit(5);
  if (e2) throw e2;
  return byName || [];
}

async function findProductByBarcode(barcode) {
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 條碼, 貨品編號')
    .eq('條碼', barcode)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function getStockByGroupSku(group, sku) {
  const { data, error } = await supabase
    .from('inventory')
    .select('庫存箱數, 庫存散數')
    .eq('群組', group)
    .eq('貨品編號', sku)
    .maybeSingle();
  if (error) throw error;
  return { box: data?.['庫存箱數'] ?? 0, piece: data?.['庫存散數'] ?? 0 };
}

async function upsertUserLastProduct(lineUserId, group, sku) {
  const now = new Date().toISOString();
  const { data } = await supabase
    .from('user_last_product')
    .select('id')
    .eq('user_id', lineUserId)
    .eq('群組', group)
    .maybeSingle();

  if (data) {
    await supabase
      .from('user_last_product')
      .update({ '貨品編號': sku, '建立時間': now })
      .eq('user_id', lineUserId)
      .eq('群組', group);
  } else {
    await supabase
      .from('user_last_product')
      .insert({ user_id: lineUserId, 群組: group, '貨品編號': sku, '建立時間': now });
  }
}

async function getLastSku(lineUserId, group) {
  const { data, error } = await supabase
    .from('user_last_product')
    .select('貨品編號')
    .eq('user_id', lineUserId)
    .eq('群組', group)
    .order('建立時間', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data?.['貨品編號'] || null;
}

// 以 群組 + SKU 操作庫存的 RPC
async function changeInventoryByGroupSku(group, sku, deltaBox, deltaPiece, userId, source='LINE') {
  const { data, error } = await supabase.rpc('exec_change_inventory_by_group_sku', {
    p_group: group,
    p_sku: sku,
    p_delta_box: deltaBox,
    p_delta_piece: deltaPiece,
    p_user_id: userId,
    p_source: source
  });
  if (error) throw error;
  return data; // { new_box, new_piece }
}

// -------- routes --------
app.get('/health', (_req, res) => res.status(200).send('OK'));

app.post('/webhook', async (req, res) => {
  try {
    const events = req.body?.events || [];
    for (const ev of events) {
      try { await handleEvent(ev); } catch (err) { console.error('[HANDLE EVENT ERROR]', err); }
    }
    res.status(200).send('OK');
  } catch (e) {
    console.error('[WEBHOOK ERROR]', e);
    res.status(500).send('ERR');
  }
});

async function handleEvent(event) {
  if (event.type !== 'message' || event.message.type !== 'text') return;
  const reply = (text) => client.replyMessage(event.replyToken, { type: 'text', text });
  const text = event.message.text || '';
  const parsed = parseCommand(text);

  if (!parsed) {
    return reply([
      '指令：',
      '• 查 可樂 / 查 123456（條碼）',
      '• 入庫3箱2件 / 入3箱 / 入3件',
      '• 出庫1箱 / 出2件',
      '• 條碼 123456'
    ].join('\n'));
  }

  const lineUserId = event.source?.userId || 'unknown';
  const group = await getUserGroup(lineUserId);

  // 查詢
  if (parsed.type === 'query') {
    const list = await findProductsByKeyword(parsed.keyword);
    if (!list.length) return reply('查無此商品');
    const lines = [];
    for (const p of list) {
      const sku = p['貨品編號'];
      const s = await getStockByGroupSku(group, sku);
      lines.push(`${p['貨品名稱']}${p['條碼'] ? `（${p['條碼']}）` : ''}\n庫存：箱 ${s.box}、件 ${s.piece}`);
    }
    await upsertUserLastProduct(lineUserId, group, list[0]['貨品編號']);
    return reply(lines.join('\n\n'));
  }

  // 條碼查詢
  if (parsed.type === 'barcode') {
    const p = await findProductByBarcode(parsed.barcode);
    if (!p) return reply('查無此條碼商品');
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(group, sku);
    await upsertUserLastProduct(lineUserId, group, sku);
    return reply(`${p['貨品名稱']}${p['條碼'] ? `（${p['條碼']}）` : ''}\n庫存：箱 ${s.box}、件 ${s.piece}`);
  }

  // 出入庫
  if (parsed.type === 'change') {
    if (parsed.box === 0 && parsed.piece === 0) return reply('數量為 0，請輸入箱或件。');

    const sku = await getLastSku(lineUserId, group);
    if (!sku) return reply('請先「查 商品」或「條碼 123」選定商品後再入/出庫。');

    const deltaBox = parsed.action === 'in' ? parsed.box : -parsed.box;
    const deltaPiece = parsed.action === 'in' ? parsed.piece : -parsed.piece;

    try {
      const r = await changeInventoryByGroupSku(group, sku, deltaBox, deltaPiece, lineUserId, 'LINE');
      const sign = (n) => (n >= 0 ? `+${n}` : `${n}`);
      return reply(`貨品編號：${sku}\n變動：箱 ${sign(deltaBox)}、件 ${sign(deltaPiece)}\n目前庫存：箱 ${r.new_box}、件 ${r.new_piece}`);
    } catch (err) {
      return reply(`操作失敗：${err?.message || '未知錯誤'}`);
    }
  }
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
