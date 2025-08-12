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
  DEFAULT_GROUP = 'default'
} = process.env;

if (!LINE_CHANNEL_ACCESS_TOKEN || !LINE_CHANNEL_SECRET) {
  console.error('缺少 LINE 環境變數');
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('缺少 Supabase 環境變數 (URL / SERVICE_ROLE_KEY)');
}

const app = express();
app.use(express.json());

const client = new line.Client({
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: LINE_CHANNEL_SECRET
});

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

/** 指令解析（嚴格分流）
 *  查 可樂 / 查可樂 / 查詢 可樂     => 只搜「貨品名稱」
 *  條碼 123 / 條碼123 / 條碼：123   => 只搜「條碼（精準）」
 *  編號 ABC / 編號ABC / 編號：ABC / #ABC => 只搜「貨品編號」（先精準，找不到再模糊）
 *  入庫3箱2散 / 入3箱 / 出2散 / 入3箱1（最後的 1 視為「散」）
 */
function parseCommand(text) {
  const t = (text || '').trim();

  // 不是指令就忽略
  if (!/^(查|查詢|條碼|編號|#|入庫|入|出庫|出)/.test(t)) return null;

  // 條碼
  const mBarcode = t.match(/^條碼[:：]?\s*(.+)$/);
  if (mBarcode) return { type: 'barcode', barcode: mBarcode[1].trim() };

  // 編號（SKU）
  const mSkuHash = t.match(/^#\s*(.+)$/);
  if (mSkuHash) return { type: 'sku', sku: mSkuHash[1].trim() };
  const mSku = t.match(/^編號[:：]?\s*(.+)$/);
  if (mSku) return { type: 'sku', sku: mSku[1].trim() };

  // 查（名稱）
  const mQuery = t.match(/^查(?:詢)?\s*(.+)$/);
  if (mQuery) return { type: 'query', keyword: mQuery[1].trim() };

  // 出入庫（支援 散/件，和尾數字即散）
  // 群組1: 入/出類型；群組2: 箱數；群組3: 散(個)；群組4: 尾數字(視為散)
  const mChange = t.match(/^(入庫|入|出庫|出)\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:個|散))?(?:\s*(\d+))?$/);
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

// 取得使用者群組與角色（若查不到，回預設群組與 user 角色）
async function getUserGroupAndRole(userId) {
  const { data, error } = await supabase
    .from('users')
    .select('群組, 角色, 黑名單')
    .eq('user_id', userId)
    .maybeSingle();
  if (error) throw error;
  const group = data?.['群組'] || DEFAULT_GROUP;
  const role = data?.['角色'] || 'user';
  const blocked = !!data?.['黑名單'];
  return { group, role, blocked };
}

// 取「有庫存」的 SKU Set（群組內：庫存箱數>0 或 庫存散數>0）
async function getInStockSkuSet(group) {
  const { data, error } = await supabase
    .from('inventory')
    .select('貨品編號, 庫存箱數, 庫存散數')
    .eq('群組', group);
  if (error) throw error;
  const set = new Set();
  (data || []).forEach(row => {
    const box = Number(row['庫存箱數'] || 0);
    const piece = Number(row['庫存散數'] || 0);
    if (box > 0 || piece > 0) set.add(row['貨品編號']);
  });
  return set;
}

// —— 查詢（依你的規則）——
async function searchByName(keyword, role, group, inStockSet) {
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 條碼')
    .ilike('貨品名稱', `%${keyword}%`)
    .limit(20);
  if (error) throw error;
  let list = data || [];
  if (role === 'user') {
    list = list.filter(p => inStockSet.has(p['貨品編號']));
  }
  return list.slice(0, 10);
}

async function searchByBarcode(barcode, role, group, inStockSet) {
  const { data, error } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 條碼')
    .eq('條碼', barcode.trim())
    .maybeSingle();
  if (error) throw error;
  if (!data) return [];
  if (role === 'user' && !inStockSet.has(data['貨品編號'])) return [];
  return [data];
}

async function searchBySku(sku, role, group, inStockSet) {
  // 精準
  const { data: exact, error: e1 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 條碼')
    .eq('貨品編號', sku.trim())
    .maybeSingle();
  if (e1) throw e1;
  if (exact && (role !== 'user' || inStockSet.has(exact['貨品編號']))) {
    return [exact];
  }

  // 模糊
  const { data: like, error: e2 } = await supabase
    .from('products')
    .select('貨品名稱, 貨品編號, 條碼')
    .ilike('貨品編號', `%${sku}%`)
    .limit(20);
  if (e2) throw e2;
  let list = like || [];
  if (role === 'user') {
    list = list.filter(p => inStockSet.has(p['貨品編號']));
  }
  return list.slice(0, 10);
}

// —— 庫存 —— 
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

// —— 記住最後查商品（以 SKU）——
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

// —— 出入庫（RPC；處理 Supabase 回傳陣列）——
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
  const row = Array.isArray(data) ? data[0] : data; // RETURNS TABLE -> 常為陣列
  return row || { new_box: null, new_piece: null };
}

// —— 組 Quick Reply 選單（點了會送「編號 XXX」）——
function buildQuickReplyForProducts(products) {
  const items = products.slice(0, 12).map(p => ({
    type: 'action',
    action: {
      type: 'message',
      label: `${p['貨品名稱']}`.slice(0, 20),
      text: `編號 ${p['貨品編號']}`
    }
  }));
  return { items };
}

// —— 路由 —— 
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));

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
  const text = event.message.text || '';
  const parsed = parseCommand(text);

  // 非指令：忽略不回
  if (!parsed) return;

  const reply = (messageObj) => client.replyMessage(event.replyToken, messageObj);
  const replyText = (textStr) => reply({ type: 'text', text: textStr });

  const lineUserId = event.source?.userId || 'unknown';
  const { group, role, blocked } = await getUserGroupAndRole(lineUserId);
  if (blocked) return; // 黑名單直接忽略

  // 預先取得 in-stock set（給 user 過濾）
  const inStockSet = role === 'user' ? await getInStockSkuSet(group) : null;

  // 「查」名稱
  if (parsed.type === 'query') {
    const list = await searchByName(parsed.keyword, role, group, inStockSet || new Set());
    if (!list.length) return replyText('查無此商品');

    if (list.length > 1) {
      return reply({
        type: 'text',
        text: `找到 ${list.length} 筆，請從下方選單選擇：`,
        quickReply: buildQuickReplyForProducts(list)
      });
    }

    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(group, sku);
    await upsertUserLastProduct(lineUserId, group, sku);
    return replyText(`${p['貨品名稱']}${p['條碼'] ? `（${p['條碼']}）` : ''}\n編號：${sku}\n庫存：${s.box}箱、${s.piece}散`);
  }

  // 「條碼」
  if (parsed.type === 'barcode') {
    const list = await searchByBarcode(parsed.barcode, role, group, inStockSet || new Set());
    if (!list.length) return replyText('查無此條碼商品');
    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(group, sku);
    await upsertUserLastProduct(lineUserId, group, sku);
    return replyText(`${p['貨品名稱']}${p['條碼'] ? `（${p['條碼']}）` : ''}\n編號：${sku}\n庫存： ${s.box}箱、${s.piece}散`);
  }

  // 「編號」
  if (parsed.type === 'sku') {
    const list = await searchBySku(parsed.sku, role, group, inStockSet || new Set());
    if (!list.length) return replyText('查無此貨品編號');

    if (list.length > 1) {
      return reply({
        type: 'text',
        text: `找到 ${list.length} 筆，請從下方選單選擇：`,
        quickReply: buildQuickReplyForProducts(list)
      });
    }

    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(group, sku);
    await upsertUserLastProduct(lineUserId, group, sku);
    return replyText(`${p['貨品名稱']}${p['條碼'] ? `（${p['條碼']}）` : ''}\n編號：${sku}\n庫存： ${s.box}箱、 ${s.piece}散`);
  }

  // 出入庫（用「最後查到的貨品編號」）
  if (parsed.type === 'change') {
    if (parsed.box === 0 && parsed.piece === 0) return; // 數量 0 → 忽略

    const sku = await getLastSku(lineUserId, group);
    if (!sku) return replyText('請先用「查 商品」或「條碼123 / 編號ABC」選定商品後再入/出庫。');

    const deltaBox = parsed.action === 'in' ? parsed.box : -parsed.box;
    const deltaPiece = parsed.action === 'in' ? parsed.piece : -parsed.piece;

    try {
      const r = await changeInventoryByGroupSku(group, sku, deltaBox, deltaPiece, lineUserId, 'LINE');
      // 防止回傳為 null/array 未取到值
      let nb = null, np = null;
      if (r && typeof r.new_box === 'number') nb = r.new_box;
      if (r && typeof r.new_piece === 'number') np = r.new_piece;
      if (nb === null || np === null) {
        const s = await getStockByGroupSku(group, sku);
        nb = s.box; np = s.piece;
      }
      const sign = (n) => (n >= 0 ? `+${n}` : `${n}`);
      return replyText(`貨品編號：${sku}\n變動： ${sign(deltaBox)}箱、 ${sign(deltaPiece)}個\n目前庫存： ${nb}箱、 ${np}散`);
    } catch (err) {
      return replyText(`操作失敗：${err?.message || '未知錯誤'}`);
    }
  }
}

process.on('unhandledRejection', (reason) => {
  console.error('[UNHANDLED REJECTION]', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT EXCEPTION]', err);
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
