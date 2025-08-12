import 'dotenv/config';
import express from 'express';
import dotenv from 'dotenv';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';

dotenv.config();

const lineConfig = {
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: process.env.LINE_CHANNEL_SECRET,
};
const client = new line.Client(lineConfig);
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);

const app = express();
app.use(express.json());

// 只處理 LINE webhook
app.post('/webhook', async (req, res) => {
  try {
    const events = req.body.events || [];
    for (const event of events) {
      await handleEvent(event);
    }
    res.status(200).send('OK');
  } catch (err) {
    console.error('webhook error:', err);
    res.status(200).send('OK');
  }
});

async function handleEvent(event) {
  if (event.type !== 'message' || event.message.type !== 'text') return;

  const userId = event.source.userId || null;
  const groupId = event.source.type === 'group' ? event.source.groupId : null;
  const text = (event.message.text || '').trim();

  // 只回應這些指令，其他一律忽略
  const isCommand =
    /^查\s*/.test(text) ||
    /^編號\s*/.test(text) ||
    /^條碼\s*/.test(text) ||
    /^入/.test(text) ||
    /^出/.test(text);

  if (!isCommand) return;

  // 自動註冊使用者（私訊時）
  if (userId) await autoRegisterUser(userId);

  // 先決定分店 & 角色
  const { branch, role, blocked, needBindMsg } = await resolveBranchAndRole({ userId, groupId });

  if (blocked) {
    await replyText(event.replyToken, '您已被封鎖無法使用此功能');
    return;
  }
  if (!branch) {
    await replyText(
      event.replyToken,
      needBindMsg || '尚未綁定分店，請管理員到系統設定此聊天室/使用者的分店'
    );
    return;
  }

  // 指令路由
  if (/^入/.test(text)) {
    // user 不能入庫；主管可
    if (role !== '主管') {
      await replyText(event.replyToken, '您沒有權限使用「入庫」');
      return;
    }
    await handleStockIn(event, text, { branch, role });
    return;
  }

  if (/^出/.test(text)) {
    // user/主管都可
    await handleStockOut(event, text, { branch, role });
    return;
  }

  if (/^編號\s*/.test(text)) {
    await handleSearchBySku(event, text.replace(/^編號\s*/, '').trim(), { branch, role });
    return;
  }

  if (/^條碼\s*/.test(text)) {
    await handleSearchByBarcode(event, text.replace(/^條碼\s*/, '').trim(), { branch, role });
    return;
  }

  if (/^查\s*/.test(text)) {
    await handleSearchByName(event, text.replace(/^查\s*/, '').trim(), { branch, role });
    return;
  }
}

/* ----------------- 共用：分店與身分 ----------------- */
async function resolveBranchAndRole({ userId, groupId }) {
  // 先看是否群組聊天室
  if (groupId) {
    // 用 line_groups 綁定
    const { data: lg } = await supabase
      .from('line_groups')
      .select('群組')
      .eq('line_group_id', groupId)
      .single();

    const branch = lg?.群組 || null;

    // 群組裡的角色全部視為 user？或不看角色？→ 我們沿用 users 角色（若查不到，一律當 user）
    let role = 'user';
    let blocked = false;
    if (userId) {
      const { data: u } = await supabase.from('users').select('角色,黑名單').eq('user_id', userId).single();
      role = u?.角色 || 'user';
      blocked = !!u?.黑名單;
    }

    return {
      branch,
      role,
      blocked,
      needBindMsg: '此群組尚未綁定分店，請管理員在 line_groups 綁定分店（catch_0001/0002/0003）',
    };
  }

  // 私訊：用 users.群組
  let role = 'user';
  let branch = null;
  let blocked = false;

  if (userId) {
    const { data: u } = await supabase
      .from('users')
      .select('群組,角色,黑名單')
      .eq('user_id', userId)
      .single();
    branch = u?.群組 || null;
    role = u?.角色 || 'user';
    blocked = !!u?.黑名單;
  }

  return {
    branch,
    role,
    blocked,
    needBindMsg: '此使用者尚未綁定分店，請管理員在 users.群組 設定（catch_0001/0002/0003）',
  };
}

async function autoRegisterUser(userId) {
  const { data } = await supabase.from('users').select('user_id').eq('user_id', userId).single();
  if (!data) {
    await supabase.from('users').insert({
      user_id: userId,
      群組: 'default',
      角色: 'user',
      黑名單: false,
    });
  }
}

/* ----------------- 查詢（user 只看庫存>0；主管不限制） ----------------- */
async function handleSearchByName(event, keyword, ctx) {
  if (!keyword) {
    await replyText(event.replyToken, '請輸入關鍵字，例如「查 可樂」');
    return;
  }
  // 找產品（最多 20 筆）
  const { data: products } = await supabase
    .from('products')
    .select('貨品編號,貨品名稱')
    .ilike('貨品名稱', `%${keyword}%`)
    .limit(20);

  if (!products || products.length === 0) {
    await replyText(event.replyToken, '查無此商品');
    return;
  }

  // 抓該分店的庫存
  const skuList = products.map((p) => p.貨品編號);
  const { data: invRows } = await supabase
    .from('inventory')
    .select('貨品編號,庫存箱數,庫存散數')
    .eq('群組', ctx.branch)
    .in('貨品編號', skuList);

  const invMap = new Map((invRows || []).map((r) => [r.貨品編號, r]));
  let result = products.map((p) => {
    const inv = invMap.get(p.貨品編號);
    const b = inv?.庫存箱數 ?? 0;
    const s = inv?.庫存散數 ?? 0;
    return { ...p, 庫存箱數: b, 庫存散數: s };
  });

  // user：過濾掉庫存=0
  if (ctx.role !== '主管') {
    result = result.filter((r) => (r.庫存箱數 > 0) || (r.庫存散數 > 0));
  }

  if (result.length === 0) {
    await replyText(event.replyToken, '無此商品庫存');
    return;
  }

  if (result.length === 1) {
    const p = result[0];
    await upsertUserLastProduct(event.source.userId, ctx.branch, p.貨品編號);
    await replyText(
      event.replyToken,
      `${p.貨品名稱}\n目前庫存：箱 ${p.庫存箱數}、散 ${p.庫存散數}`
    );
    return;
  }

  // 多筆 → quick reply（顯示名稱，點了回「編號 #SKU」）
  const items = result.slice(0, 12).map((p) => ({
    type: 'action',
    action: {
      type: 'message',
      label: p.貨品名稱,
      text: `編號 ${p.貨品編號}`,
    },
  }));

  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: '找到多筆，請點選：',
    quickReply: { items },
  });
}

async function handleSearchBySku(event, sku, ctx) {
  if (!sku) {
    await replyText(event.replyToken, '請輸入格式：編號 ABC123');
    return;
  }
  const { data: product } = await supabase
    .from('products')
    .select('貨品編號,貨品名稱')
    .eq('貨品編號', sku)
    .single();

  if (!product) {
    await replyText(event.replyToken, '查無此商品');
    return;
  }

  const { data: inv } = await supabase
    .from('inventory')
    .select('庫存箱數,庫存散數')
    .eq('群組', ctx.branch)
    .eq('貨品編號', sku)
    .single();

  const b = inv?.庫存箱數 ?? 0;
  const s = inv?.庫存散數 ?? 0;

  if (ctx.role !== '主管' && b === 0 && s === 0) {
    await replyText(event.replyToken, '無此商品庫存');
    return;
  }

  await upsertUserLastProduct(event.source.userId, ctx.branch, sku);
  await replyText(event.replyToken, `${product.貨品名稱}\n目前庫存：箱 ${b}、散 ${s}`);
}

async function handleSearchByBarcode(event, barcode, ctx) {
  if (!barcode) {
    await replyText(event.replyToken, '請輸入格式：條碼 1234567890');
    return;
  }
  const { data: product } = await supabase
    .from('products')
    .select('貨品編號,貨品名稱')
    .eq('條碼', barcode)
    .single();

  if (!product) {
    await replyText(event.replyToken, '查無此條碼商品');
    return;
  }

  const { data: inv } = await supabase
    .from('inventory')
    .select('庫存箱數,庫存散數')
    .eq('群組', ctx.branch)
    .eq('貨品編號', product.貨品編號)
    .single();

  const b = inv?.庫存箱數 ?? 0;
  const s = inv?.庫存散數 ?? 0;

  if (ctx.role !== '主管' && (b === 0 && s === 0)) {
    await replyText(event.replyToken, '無此商品庫存');
    return;
  }

  await upsertUserLastProduct(event.source.userId, ctx.branch, product.貨品編號);
  await replyText(event.replyToken, `${product.貨品名稱}\n目前庫存：箱 ${b}、散 ${s}`);
}

/* ----------------- 入/出庫（箱/散各自變動；不換算） ----------------- */
// 支援：入庫3箱2散 / 入3箱1 / 入3箱 / 入3散
const REG_IN = /^入(?:庫)?\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:件|散))?$/;
// 支援：出3箱2散 / 出3箱 / 出3散 / 出1
const REG_OUT = /^出\s*(?:(\d+)\s*箱)?\s*(?:(\d+)\s*(?:件|散))?$/;

async function handleStockIn(event, text, ctx) {
  const userId = event.source.userId;
  const m = text.match(REG_IN);
  if (!m) {
    await replyText(event.replyToken, '格式錯誤，例：入庫3箱2散 / 入3箱1 / 入3箱 / 入3散');
    return;
  }
  const deltaBox = m[1] ? parseInt(m[1], 10) : 0;
  const deltaPiece = m[2] ? parseInt(m[2], 10) : 0;

  const last = await fetchLastProduct(userId, ctx.branch);
  if (!last) {
    await replyText(event.replyToken, '請先「查/編號/條碼」選擇商品後再入庫');
    return;
  }

  // 產品資料
  const { data: prod } = await supabase
    .from('products')
    .select('貨品名稱,箱入數,單價')
    .eq('貨品編號', last.貨品編號)
    .single();

  // 讀當前庫存
  const { data: inv } = await supabase
    .from('inventory')
    .select('庫存箱數,庫存散數')
    .eq('群組', ctx.branch)
    .eq('貨品編號', last.貨品編號)
    .single();

  const curB = inv?.庫存箱數 ?? 0;
  const curP = inv?.庫存散數 ?? 0;
  const newB = curB + deltaBox;
  const newP = curP + deltaPiece;

  // 金額（不換算，僅用箱入數/單價計算加總金額）
  const units = toInt(prod?.箱入數, 1);
  const price = toNum(prod?.單價, 0);
  const inAmount = deltaBox * units * price + deltaPiece * price;
  const stockAmount = newB * units * price + newP * price;

  // 寫入日誌
  await supabase.from('inventory_logs').insert({
    user_id: userId,
    群組: ctx.branch,
    貨品編號: last.貨品編號,
    貨品名稱: prod?.貨品名稱 ?? '',
    入庫箱數: deltaBox,
    入庫散數: deltaPiece,
    出庫箱數: 0,
    出庫散數: 0,
    庫存箱數: newB,
    庫存散數: newP,
    入庫金額: inAmount,
    出庫金額: 0,
    庫存金額: stockAmount,
    操作來源: 'LINE',
    建立時間: new Date().toISOString(),
  });

  // 更新 inventory（upsert）
  await upsertInventory(ctx.branch, last.貨品編號, newB, newP);

  await replyText(
    event.replyToken,
    `入庫成功\n貨品編號：${last.貨品編號}\n變動：箱 +${deltaBox}、散 +${deltaPiece}\n目前庫存：箱 ${newB}、散 ${newP}`
  );
}

async function handleStockOut(event, text, ctx) {
  const userId = event.source.userId;
  const m = text.match(REG_OUT);
  if (!m) {
    await replyText(event.replyToken, '格式錯誤，例：出3箱2散 / 出3箱 / 出3散 / 出1');
    return;
  }
  const deltaBox = m[1] ? parseInt(m[1], 10) : 0;
  const deltaPiece = m[2] ? parseInt(m[2], 10) : (m[1] ? 0 : 0); // 未填即 0

  const last = await fetchLastProduct(userId, ctx.branch);
  if (!last) {
    await replyText(event.replyToken, '請先「查/編號/條碼」選擇商品後再出庫');
    return;
  }

  // 產品 & 當前庫存
  const { data: prod } = await supabase
    .from('products')
    .select('貨品名稱,箱入數,單價')
    .eq('貨品編號', last.貨品編號)
    .single();

  const { data: inv } = await supabase
    .from('inventory')
    .select('庫存箱數,庫存散數')
    .eq('群組', ctx.branch)
    .eq('貨品編號', last.貨品編號)
    .single();

  const curB = inv?.庫存箱數 ?? 0;
  const curP = inv?.庫存散數 ?? 0;

  if (deltaBox > curB || deltaPiece > curP) {
    await replyText(
      event.replyToken,
      `庫存不足\n目前庫存：箱 ${curB}、散 ${curP}`
    );
    return;
  }

  const newB = curB - deltaBox;
  const newP = curP - deltaPiece;

  const units = toInt(prod?.箱入數, 1);
  const price = toNum(prod?.單價, 0);
  const outAmount = deltaBox * units * price + deltaPiece * price;
  const stockAmount = newB * units * price + newP * price;

  // 寫入日誌
  await supabase.from('inventory_logs').insert({
    user_id: userId,
    群組: ctx.branch,
    貨品編號: last.貨品編號,
    貨品名稱: prod?.貨品名稱 ?? '',
    入庫箱數: 0,
    入庫散數: 0,
    出庫箱數: deltaBox,
    出庫散數: deltaPiece,
    庫存箱數: newB,
    庫存散數: newP,
    入庫金額: 0,
    出庫金額: outAmount,
    庫存金額: stockAmount,
    操作來源: 'LINE',
    建立時間: new Date().toISOString(),
  });

  await upsertInventory(ctx.branch, last.貨品編號, newB, newP);

  await replyText(
    event.replyToken,
    `出庫成功\n貨品編號：${last.貨品編號}\n變動：箱 -${deltaBox}、散 -${deltaPiece}\n目前庫存：箱 ${newB}、散 ${newP}`
  );
}

/* ----------------- DB helpers ----------------- */
async function upsertInventory(branch, sku, box, piece) {
  // 先查是否存在
  const { data: row } = await supabase
    .from('inventory')
    .select('貨品編號')
    .eq('群組', branch)
    .eq('貨品編號', sku)
    .single();

  if (row) {
    await supabase
      .from('inventory')
      .update({ 庫存箱數: box, 庫存散數: piece, 更新時間: new Date().toISOString() })
      .eq('群組', branch)
      .eq('貨品編號', sku);
  } else {
    await supabase.from('inventory').insert({
      群組: branch,
      貨品編號: sku,
      庫存箱數: box,
      庫存散數: piece,
      更新時間: new Date().toISOString(),
    });
  }
}

async function fetchLastProduct(userId, branch) {
  if (!userId) return null;
  const { data } = await supabase
    .from('user_last_product')
    .select('貨品編號')
    .eq('user_id', userId)
    .eq('群組', branch)
    .order('建立時間', { ascending: false })
    .limit(1)
    .single();
  return data || null;
}

async function upsertUserLastProduct(user_id, branch, sku) {
  if (!user_id) return;
  const nowIso = new Date().toISOString();
  const { data } = await supabase
    .from('user_last_product')
    .select('id')
    .eq('user_id', user_id)
    .eq('群組', branch)
    .single();
  if (data) {
    await supabase
      .from('user_last_product')
      .update({ 貨品編號: sku, 建立時間: nowIso })
      .eq('user_id', user_id)
      .eq('群組', branch);
  } else {
    await supabase.from('user_last_product').insert({
      user_id,
      群組: branch,
      貨品編號: sku,
      建立時間: nowIso,
    });
  }
}

/* ----------------- 小工具 ----------------- */
async function replyText(replyToken, text) {
  try {
    await client.replyMessage(replyToken, { type: 'text', text: String(text) });
  } catch (e) {
    console.error('reply error:', e);
  }
}

function toInt(v, def = 0) {
  const n = parseInt(String(v || '').replace(/[^\d-]/g, ''), 10);
  return Number.isFinite(n) ? n : def;
}
function toNum(v, def = 0) {
  const n = Number(String(v || '').replace(/[^\d.-]/g, ''));
  return Number.isFinite(n) ? n : def;
}

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Server running on port ${port}`));
