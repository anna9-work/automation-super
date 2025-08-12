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
      needBindMsg: '此群組尚未綁定分店，請管理員在 line_groups.群組 設為 catch_0001/0002/0003'
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
      needBindMsg: '此使用者尚未綁定分店，請管理員在 users.群組 設為 catch_0001/0002/0003'
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
