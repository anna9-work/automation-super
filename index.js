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

// 提醒：line.Client 其實只需要 channelAccessToken；channelSecret 通常用在 middleware 驗章
const client = new line.Client({
  channelAccessToken: LINE_CHANNEL_ACCESS_TOKEN
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

/** 依事件來源解析分店與角色
 * 群組聊天室：查 line_groups by groupId → 得到「群組」（分店代號）
 * 私訊：查 users by userId 的 群組
 * 角色：一律看 users.角色（查不到預設 user）
 */
async function resolveBranchAndRole(event) {
  const source = event.source || {};
  const userId = source.userId || null;
  const isGroup = source.type === 'group';
  const groupId = isGroup ? source.groupId : null;

  // 角色/黑名單（沿用 users；找不到視為 user / 未封鎖）
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

  // 分店（branch）
  if (isGroup) {
    // 以 line_groups 綁定為準
    const { data: lg } = await supabase
      .from('line_groups')
      .select('群組')
      .eq('line_group_id', groupId)
      .maybeSingle();
    const branch = lg?.群組 || null;
    return {
      branch,
      role,
      blocked,
      needBindMsg: '此群組尚未綁定分店，請管理員設定'
    };
  } else {
    // 私訊：看 users.群組
    const { data: u2 } = await supabase
      .from('users')
      .select('群組')
      .eq('user_id', userId)
      .maybeSingle();
    const branch = u2?.群組 || null;
    return {
      branch,
      role,
      blocked,
      needBindMsg: '此使用者尚未綁定分店，請管理員設定'
    };
  }
}

// 取得/建立使用者（僅私訊時）
async function autoRegisterUser(userId) {
  if (!userId) return;
  const { data } = await supabase.from('users').select('user_id').eq('user_id', userId).maybeSingle();
  if (!data) {
    await supabase.from('users').insert({
      user_id: userId,
      群組: DEFAULT_GROUP,
      角色: 'user',
      黑名單: false
    });
  }
}

// 取「有庫存」的 SKU Set（群組內：庫存箱數>0 或 庫存散數>0）
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

// —— 查詢（依你的規則）——
async function searchByName(keyword, role, branch, inStockSet) {
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

async function searchByBarcode(barcode, role, branch, inStockSet) {
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

async function searchBySku(sku, role, branch, inStockSet) {
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
async function getStockByGroupSku(branch, sku) {
  const { data, error } = await supabase
    .from('inventory')
    .select('庫存箱數, 庫存散數')
    .eq('群組', branch)
    .eq('貨品編號', sku)
    .maybeSingle();
  if (error) throw error;
  return { box: data?.['庫存箱數'] ?? 0, piece: data?.['庫存散數'] ?? 0 };
}

// —— 記住最後查商品（以 SKU）——
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

// —— 出入庫（RPC；處理 Supabase 回傳陣列）——
async function changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, userId, source = 'LINE') {
  const { data, error } = await supabase.rpc('exec_change_inventory_by_group_sku', {
    p_group: branch,
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

// —— Quick Reply 選單（點了會送「編號 XXX」）——
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

// —— 輔助：把 groupId / roomId / userId 與文字印出 —— 
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
    console.log(
      `[LINE EVENT] type=${event?.type} source=${src.type || '-'} groupId=${groupId || '-'} roomId=${roomId || '-'} userId=${userId || '-'} text="${text}"`
    );
  } catch (e) {
    console.error('[LINE EVENT LOG ERROR]', e);
  }
}

// —— 路由 —— 
app.get('/health', (_req, res) => res.status(200).send('OK'));
app.get('/', (_req, res) => res.status(200).send('RUNNING'));

app.post('/webhook', async (req, res) => {
  try {
    const events = req.body?.events || [];

    // ※ 如需完整原始 JSON，打開下行註解（log 會較多）
    // console.log('[WEBHOOK RAW]', JSON.stringify(req.body, null, 2));

    for (const ev of events) {
      // 每個事件都印出 groupId/roomId/userId/文字，方便在 Railway Logs 搜尋
      logEventSummary(ev);

      try {
        await handleEvent(ev);
      } catch (err) {
        console.error('[HANDLE EVENT ERROR]', err);
      }
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

  const source = event.source || {};
  const isGroup = source.type === 'group';
  const lineUserId = source.userId || null;

  // 私訊時自動註冊
  if (!isGroup && lineUserId) await autoRegisterUser(lineUserId);

  // 解析分店/角色
  const { branch, role, blocked, needBindMsg } = await resolveBranchAndRole(event);
  if (blocked) return; // 黑名單直接忽略
  if (!branch) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: needBindMsg || '尚未分店綁定，請管理員設定'
    });
    return;
  }

  const reply = (messageObj) => client.replyMessage(event.replyToken, messageObj);
  const replyText = (textStr) => reply({ type: 'text', text: textStr });

  // 預先取得 in-stock set（給 user 過濾）
  const inStockSet = role === 'user' ? await getInStockSkuSet(branch) : new Set();

  // 「查」名稱
  if (parsed.type === 'query') {
    const list = await searchByName(parsed.keyword, role, branch, inStockSet);
    if (!list.length) {
      await replyText(role === '主管' ? '查無此商品' : '無此商品庫存');
      return;
    }

    if (list.length > 1) {
      await reply({
        type: 'text',
        text: `找到以下與「${parsed.keyword}」相關的選項`,
        quickReply: buildQuickReplyForProducts(list)
      });
      return;
    }

    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (role === 'user' && s.box === 0 && s.piece === 0) {
      await replyText('無此商品庫存');
      return;
    }
    await upsertUserLastProduct(lineUserId, branch, sku);
    await replyText(`貨品名稱：${p['貨品名稱']} (/p)貨品編號：${sku}(/p)目前庫存：${s.box}箱${s.piece}散`);
    return;
  }

  // 「條碼」
  if (parsed.type === 'barcode') {
    const list = await searchByBarcode(parsed.barcode, role, branch, inStockSet);
    if (!list.length) {
      await replyText(role === '主管' ? '查無此條碼商品' : '無此商品庫存');
      return;
    }
    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (role === 'user' && s.box === 0 && s.piece === 0) {
      await replyText('無此商品庫存');
      return;
    }
    await upsertUserLastProduct(lineUserId, branch, sku);
    await replyText(`${p['貨品名稱']}${p['條碼'] ? `（${p['條碼']}）` : ''}\n編號：${sku}\n庫存： ${s.box}箱、${s.piece}散`);
    return;
  }

  // 「編號」
  if (parsed.type === 'sku') {
    const list = await searchBySku(parsed.sku, role, branch, inStockSet);
    if (!list.length) {
      await replyText(role === '主管' ? '查無此貨品編號' : '無此商品庫存');
      return;
    }

    if (list.length > 1) {
      await reply({
        type: 'text',
        text: `找到以下與「${parsed.sku}」相關的選項`,
        quickReply: buildQuickReplyForProducts(list)
      });
      return;
    }

    const p = list[0];
    const sku = p['貨品編號'];
    const s = await getStockByGroupSku(branch, sku);
    if (role === 'user' && s.box === 0 && s.piece === 0) {
      await replyText('無此商品庫存');
      return;
    }
    await upsertUserLastProduct(lineUserId, branch, sku);
    await replyText(`${p['貨品名稱']}${p['條碼'] ? `（${p['條碼']}）` : ''}\n貨品編號：${sku}\n目前庫存： ${s.box}箱、 ${s.piece}散`);
    return;
  }

  // 出入庫（用「最後查到的貨品編號」）
  if (parsed.type === 'change') {
    // 權限檢查：入庫只允許主管
    if (parsed.action === 'in' && role !== '主管') {
      await replyText('您無法使用「入庫」');
      return;
    }

    if (parsed.box === 0 && parsed.piece === 0) return; // 數量 0 → 忽略

    const sku = await getLastSku(lineUserId, branch);
    if (!sku) {
      await replyText('請先用「查 商品」或「條碼123 / 編號ABC」選定商品後再入/出庫。');
      return;
    }

    const deltaBox = parsed.action === 'in' ? parsed.box : -parsed.box;
    const deltaPiece = parsed.action === 'in' ? parsed.piece : -parsed.piece;

    try {
      const r = await changeInventoryByGroupSku(branch, sku, deltaBox, deltaPiece, lineUserId, 'LINE');
      // 防止回傳為 null/array 未取到值
      let nb = null, np = null;
      if (r && typeof r.new_box === 'number') nb = r.new_box;
      if (r && typeof r.new_piece === 'number') np = r.new_piece;
      if (nb === null || np === null) {
        const s = await getStockByGroupSku(branch, sku);
        nb = s.box; np = s.piece;
      }
      const sign = (n) => (n >= 0 ? `+${n}` : `${n}`);

      // 取得貨品名稱（僅用於回覆顯示）
      const { data: prodNameRow } = await supabase
        .from('products')
        .select('貨品名稱')
        .eq('貨品編號', sku)
        .maybeSingle();
      const prodName = prodNameRow?.['貨品名稱'] || sku;

      await replyText(`${parsed.action === 'in' ? '✅入庫成功' : '✅出庫成功'}(/p)貨品名稱：${prodName}(/p)目前庫存${nb}箱${np}散`);
      return;
    } catch (err) {
      console.error('change error:', err);
      await replyText(`操作失敗：${err?.message || '未知錯誤'}`);
      return;
    }
  }

  // 其他型別：忽略
  return;
}

// 全域錯誤保護
process.on('unhandledRejection', (reason) => {
  console.error('[UNHANDLED REJECTION]', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT EXCEPTION]', err);
});

// 啟動
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
