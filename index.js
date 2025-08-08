import express from 'express';
import dotenv from 'dotenv';
import line from '@line/bot-sdk';
import { createClient } from '@supabase/supabase-js';
import { Parser } from 'json2csv';

dotenv.config();

const lineConfig = {
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: process.env.LINE_CHANNEL_SECRET,
};
const client = new line.Client(lineConfig);

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);

const app = express();
app.use(express.json());

app.post('/webhook', async (req, res) => {
  const events = req.body.events;
  if (!Array.isArray(events)) return res.status(200).send('OK');
  for (const event of events) {
    await handleEvent(event);
  }
  res.status(200).send('OK');
});

async function handleEvent(event) {
  if (event.type !== 'message' || event.message.type !== 'text') return;

  const userId = event.source.userId;
  const messageText = event.message.text.trim();

  // 只回覆指定指令開頭的訊息，非指令訊息不回應
  const validCommands = ['查', '入', '出', '條碼', '詳細', '庫存', '匯出報表', '取消出庫'];
  if (!validCommands.some((cmd) => messageText.startsWith(cmd))) return;

  await autoRegisterUser(userId);

  const { data: userData } = await supabase
    .from('users')
    .select('*')
    .eq('user_id', userId)
    .single();

  if (userData?.黑名單 === true) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '您已被封鎖無法使用此功能',
    });
    return;
  }

  if (messageText === '取消出庫') {
    await handleCancelLastStockLog(event, userData);
    return;
  }

  if (messageText === '庫存') {
    await handleCheckInventory(event, userData);
    return;
  }

  if (messageText === '匯出報表') {
    if (userData.角色 === '主管') {
      await handleExportReport(event, userData);
    } else {
      await client.replyMessage(event.replyToken, {
        type: 'text',
        text: '您沒有權限使用此功能',
      });
    }
    return;
  }

  if (messageText.startsWith('入')) {
    if (userData.角色 !== '主管') {
      await client.replyMessage(event.replyToken, {
        type: 'text',
        text: '您沒有權限使用此功能',
      });
      return;
    }
    await handleStockIn(event, messageText, userData);
    return;
  }

  if (messageText.startsWith('出')) {
    if (userData.角色 !== '主管' && userData.角色 !== 'user') {
      await client.replyMessage(event.replyToken, {
        type: 'text',
        text: '您沒有權限使用此功能',
      });
      return;
    }
    await handleStockOut(event, messageText, userData);
    return;
  }

  if (messageText.startsWith('查')) {
    await handleSearchProduct(event, messageText, userData);
    return;
  }

  if (messageText.startsWith('條碼')) {
    await handleSearchByBarcode(event, messageText, userData);
    return;
  }

  if (messageText.startsWith('詳細')) {
    await handleProductDetail(event, messageText, userData);
    return;
  }

  // 預設回覆（不應到這裡，因已過濾非指令訊息）
  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: '請輸入正確指令。',
  });
}

// 使用者自動註冊（如無資料則新增）
async function autoRegisterUser(userId) {
  const { data } = await supabase.from('users').select('*').eq('user_id', userId).single();
  if (!data) {
    await supabase.from('users').insert({
      user_id: userId,
      群組: 'default',
      角色: 'user',
      黑名單: false,
    });
  }
}

// 多筆查詢及單筆精確查詢功能保留並優化
async function handleSearchProduct(event, messageText, userData) {
  const userId = event.source.userId;
  const keyword = messageText.replace(/^查\s*/, '').trim();
  if (!keyword) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '請輸入欲查詢的商品名稱，例如「查 蘋果」',
    });
    return;
  }

  // 單一精確查詢，格式：查#貨品編號
  if (keyword.startsWith('#')) {
    const productId = keyword.slice(1);
    const { data: product } = await supabase
      .from('products')
      .select('*')
      .eq('貨品編號', productId)
      .single();

    if (!product) {
      await client.replyMessage(event.replyToken, {
        type: 'text',
        text: '查無此商品',
      });
      return;
    }

    await upsertUserLastProduct(userId, userData.群組, product.貨品編號);

    const { data: stockSummary } = await supabase
      .from('stock_summary')
      .select('*')
      .eq('貨品編號', product.貨品編號)
      .eq('群組', userData.群組)
      .single();

    const 庫存箱 = stockSummary?.庫存箱數 ?? 0;
    const 庫存散 = stockSummary?.庫存散數 ?? 0;

    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: `查詢到：${product.貨品名稱}\n目前庫存：${庫存箱}箱${庫存散}件`,
    });
    return;
  }

  // 模糊查詢
  const { data: products } = await supabase
    .from('products')
    .select('*')
    .ilike('貨品名稱', `%${keyword}%`);

  if (!products || products.length === 0) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '查無此商品',
    });
    return;
  }

  // 多筆查詢時，更新第一筆商品為用戶最後查詢
  await upsertUserLastProduct(userId, userData.群組, products[0].貨品編號);

  if (products.length === 1) {
    // 單筆直接回覆並顯示庫存
    const product = products[0];

    const { data: stockSummary } = await supabase
      .from('stock_summary')
      .select('*')
      .eq('貨品編號', product.貨品編號)
      .eq('群組', userData.群組)
      .single();

    const 庫存箱 = stockSummary?.庫存箱數 ?? 0;
    const 庫存散 = stockSummary?.庫存散數 ?? 0;

    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: `查詢到：${product.貨品名稱}\n目前庫存：${庫存箱}箱${庫存散}件`,
    });
    return;
  }

  // 多筆商品回覆 quick reply 讓使用者點選
  const quickItems = products.slice(0, 10).map((p) => ({
    type: 'action',
    action: {
      type: 'message',
      label: p.貨品名稱,
      text: `查 #${p.貨品編號}`,
    },
  }));

  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: `找到多筆商品，請點選：`,
    quickReply: {
      items: quickItems,
    },
  });
}

// 條碼查詢
async function handleSearchByBarcode(event, messageText, userData) {
  const userId = event.source.userId;
  const barcode = messageText.replace(/^條碼\s*/, '').trim();

  if (!barcode) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '請輸入欲查詢的條碼，例如「條碼 1234567890」',
    });
    return;
  }

  const { data: product } = await supabase
    .from('products')
    .select('*')
    .eq('條碼', barcode)
    .single();

  if (!product) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '查無此條碼商品',
    });
    return;
  }

  await upsertUserLastProduct(userId, userData.群組, product.貨品編號);

  const { data: stockSummary } = await supabase
    .from('stock_summary')
    .select('*')
    .eq('貨品編號', product.貨品編號)
    .eq('群組', userData.群組)
    .single();

  const 庫存箱 = stockSummary?.庫存箱數 ?? 0;
  const 庫存散 = stockSummary?.庫存散數 ?? 0;

  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: `條碼查詢：${product.貨品名稱}\n目前庫存：${庫存箱}箱${庫存散}件`,
  });
}

// 詳細資訊查詢
async function handleProductDetail(event, messageText, userData) {
  const keyword = messageText.replace(/^詳細\s*/, '').trim();
  if (!keyword) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '請輸入欲查詢詳細資訊的商品名稱，例如「詳細 蘋果」',
    });
    return;
  }

  const { data: product } = await supabase
    .from('products')
    .select('*')
    .ilike('貨品名稱', `%${keyword}%`)
    .limit(1)
    .single();

  if (!product) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '查無此商品',
    });
    return;
  }

  const { data: stockSummary } = await supabase
    .from('stock_summary')
    .select('*')
    .eq('貨品編號', product.貨品編號)
    .eq('群組', userData.群組)
    .single();

  const 庫存箱 = stockSummary?.庫存箱數 ?? 0;
  const 庫存散 = stockSummary?.庫存散數 ?? 0;

  const details = `
商品名稱：${product.貨品名稱}
貨品編號：${product.貨品編號}
條碼：${product.條碼 ?? ''}
目前庫存：${庫存箱}箱${庫存散}件
  `.trim();

  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: details,
  });
}

// 入庫功能（支援多種格式）
// 支援格式：入庫3箱2件、入3箱2件、入3箱、入3件
async function handleStockIn(event, messageText, userData) {
  const userId = event.source.userId;

  // 使用正則解析多種入庫格式
  // 解析方式：入(庫)?(\d+)?箱?(\d+)?件?
  // 會捕捉箱數與件數（皆為選填）
  const regex = /^入(庫)?(?:\s*(\d+)箱)?(?:\s*(\d+)件)?$/;
  const match = messageText.match(regex);

  if (!match) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '⚠️ 指令格式錯誤，請使用「入庫3箱2件」、「入3箱」、「入3件」的格式',
    });
    return;
  }

  // match[2] 為箱數，match[3] 為件數
  const 入庫箱 = match[2] ? parseInt(match[2], 10) : 0;
  const 入庫散 = match[3] ? parseInt(match[3], 10) : 0;

  // 先取得用戶最後查詢商品
  const { data: lastProd } = await supabase
    .from('user_last_product')
    .select('*')
    .eq('user_id', userId)
    .eq('群組', userData.群組)
    .order('updated_at', { ascending: false })
    .limit(1)
    .single();

  if (!lastProd) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '請先查詢商品後才能入庫',
    });
    return;
  }

  // 讀取商品資料(箱入數, 單價)
  const { data: productData } = await supabase
    .from('products')
    .select('貨品名稱, 箱入數, 單價')
    .eq('貨品編號', lastProd.貨品編號)
    .single();

  // 查詢當前庫存
  const { data: currentStock } = await supabase
    .from('stock_summary')
    .select('*')
    .eq('群組', userData.群組)
    .eq('貨品編號', lastProd.貨品編號)
    .single();

  const currentBox = currentStock?.庫存箱數 ?? 0;
  const currentPiece = currentStock?.庫存散數 ?? 0;

  // 直接相加，無轉換
  const newStockBox = currentBox + 入庫箱;
  const newStockPiece = currentPiece + 入庫散;

  const 箱入數 = productData?.箱入數 ?? 1;
  const 單價 = productData?.單價 ?? 0;

  const 入庫金額 = 入庫箱 * 箱入數 * 單價 + 入庫散 * 單價;
  const 出庫金額 = 0;
  const 庫存金額 = newStockBox * 箱入數 * 單價 + newStockPiece * 單價;

  // 寫入庫存紀錄
  await supabase.from('stock_log').insert({
    user_id: userId,
    群組: userData.群組,
    貨品編號: lastProd.貨品編號,
    貨品名稱: productData?.貨品名稱 ?? '',
    入庫箱數: 入庫箱,
    入庫散數: 入庫散,
    出庫箱數: 0,
    出庫散數: 0,
    庫存箱數: newStockBox,
    庫存散數: newStockPiece,
    入庫金額,
    出庫金額,
    庫存金額,
    建立時間: new Date().toISOString(),
  });

  // 更新庫存總表
  await updateStockSummary(userData.群組, lastProd.貨品編號, newStockBox, newStockPiece);

  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: `入庫成功！目前庫存：${newStockBox}箱${newStockPiece}件`,
  });
}

// 出庫功能（支援多種格式）
// 支援格式：出3箱2件、出3箱、出3件
async function handleStockOut(event, messageText, userData) {
  const userId = event.source.userId;

  // 解析出庫指令格式
  // 支援格式：出(\d+)?箱?(\d+)?件?
  const regex = /^出(?:\s*(\d+)箱)?(?:\s*(\d+)件)?$/;
  const match = messageText.match(regex);

  if (!match) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '⚠️ 指令格式錯誤，請使用「出3箱2件」、「出3箱」、「出3件」的格式',
    });
    return;
  }

  // match[1]為箱數，match[2]為件數
  const 出庫箱 = match[1] ? parseInt(match[1], 10) : 0;
  const 出庫散 = match[2] ? parseInt(match[2], 10) : 0;

  // 先取得用戶最後查詢商品
  const { data: lastProd } = await supabase
    .from('user_last_product')
    .select('*')
    .eq('user_id', userId)
    .eq('群組', userData.群組)
    .order('updated_at', { ascending: false })
    .limit(1)
    .single();

  if (!lastProd) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '請先查詢商品後才能出庫',
    });
    return;
  }

  // 讀取商品資料(箱入數, 單價)
  const { data: productData } = await supabase
    .from('products')
    .select('貨品名稱, 箱入數, 單價')
    .eq('貨品編號', lastProd.貨品編號)
    .single();

  // 取得當前庫存
  const { data: stockSummary } = await supabase
    .from('stock_summary')
    .select('*')
    .eq('貨品編號', lastProd.貨品編號)
    .eq('群組', userData.群組)
    .single();

  const currentStockBox = stockSummary?.庫存箱數 ?? 0;
  const currentStockPiece = stockSummary?.庫存散數 ?? 0;

  // 確認庫存足夠（箱、件分開判斷）
  if (出庫箱 > currentStockBox || 出庫散 > currentStockPiece) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: `庫存不足，無法出庫！目前庫存：${currentStockBox}箱${currentStockPiece}件`,
    });
    return;
  }

  // 出庫後庫存計算（箱、件直接扣，不做換算）
  const newStockBox = currentStockBox - 出庫箱;
  const newStockPiece = currentStockPiece - 出庫散;

  const 箱入數 = productData?.箱入數 ?? 1;
  const 單價 = productData?.單價 ?? 0;

  const 出庫金額 = 出庫箱 * 箱入數 * 單價 + 出庫散 * 單價;
  const 入庫金額 = 0;
  const 庫存金額 = newStockBox * 箱入數 * 單價 + newStockPiece * 單價;

  // 寫入庫存紀錄
  await supabase.from('stock_log').insert({
    user_id: userId,
    群組: userData.群組,
    貨品編號: lastProd.貨品編號,
    貨品名稱: productData?.貨品名稱 ?? '',
    入庫箱數: 0,
    入庫散數: 0,
    出庫箱數: 出庫箱,
    出庫散數: 出庫散,
    庫存箱數: newStockBox,
    庫存散數: newStockPiece,
    入庫金額,
    出庫金額,
    庫存金額,
    建立時間: new Date().toISOString(),
  });

  // 更新庫存總表
  await updateStockSummary(userData.群組, lastProd.貨品編號, newStockBox, newStockPiece);

  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: `出庫成功！目前庫存：${newStockBox}箱${newStockPiece}件`,
  });
}

// 取消最後一筆出庫紀錄
async function handleCancelLastStockLog(event, userData) {
  const userId = event.source.userId;

  // 找出最後一筆出庫紀錄 (入庫箱數+入庫散數=0，且出庫數>0)
  const { data: lastOutLog } = await supabase
    .from('stock_log')
    .select('*')
    .eq('user_id', userId)
    .eq('群組', userData.群組)
    .or('出庫箱數.gt.0,出庫散數.gt.0')
    .order('建立時間', { ascending: false })
    .limit(1)
    .single();

  if (!lastOutLog) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '找不到可取消的出庫紀錄',
    });
    return;
  }

  // 還原庫存：將出庫數量加回庫存
  const restoredBox = lastOutLog.庫存箱數 + lastOutLog.出庫箱數;
  const restoredPiece = lastOutLog.庫存散數 + lastOutLog.出庫散數;

  // 更新庫存總表
  await updateStockSummary(userData.群組, lastOutLog.貨品編號, restoredBox, restoredPiece);

  // 刪除該筆出庫紀錄
  await supabase.from('stock_log').delete().eq('id', lastOutLog.id);

  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: `已取消最後一筆出庫，庫存回復為：${restoredBox}箱${restoredPiece}件`,
  });
}

// 查詢庫存清單
async function handleCheckInventory(event, userData) {
  const { data: stockList } = await supabase
    .from('stock_summary')
    .select('*')
    .eq('群組', userData.群組)
    .order('庫存箱數', { ascending: false });

  if (!stockList || stockList.length === 0) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '庫存資料為空',
    });
    return;
  }

  let replyText = '庫存清單：\n';
  for (const item of stockList) {
    replyText += `${item.貨品名稱}：${item.庫存箱數}箱${item.庫存散數}件\n`;
  }

  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: replyText,
  });
}

// 匯出報表 (主管專用)
async function handleExportReport(event, userData) {
  const { data: stockLogs } = await supabase
    .from('stock_log')
    .select('*')
    .eq('群組', userData.群組)
    .order('建立時間', { ascending: false });

  if (!stockLogs || stockLogs.length === 0) {
    await client.replyMessage(event.replyToken, {
      type: 'text',
      text: '無庫存紀錄可匯出',
    });
    return;
  }

  const fields = [
    '貨品名稱',
    '貨品編號',
    '入庫箱數',
    '入庫散數',
    '出庫箱數',
    '出庫散數',
    '庫存箱數',
    '庫存散數',
    '入庫金額',
    '出庫金額',
    '庫存金額',
    '建立時間',
  ];

  const parser = new Parser({ fields });
  const csv = parser.parse(stockLogs);

  // 傳送CSV檔案作為文字訊息（Line Messaging API無法直接傳檔案，故改用文字回覆）
  await client.replyMessage(event.replyToken, {
    type: 'text',
    text: `匯出報表CSV內容：\n${csv}`,
  });
}

// 更新庫存總表函式
async function updateStockSummary(群組, 貨品編號, 庫存箱數, 庫存散數) {
  // 先檢查該商品是否有庫存紀錄
  const { data } = await supabase
    .from('stock_summary')
    .select('*')
    .eq('群組', 群組)
    .eq('貨品編號', 貨品編號)
    .single();

  if (data) {
    // 更新
    await supabase
      .from('stock_summary')
      .update({
        庫存箱數,
        庫存散數,
      })
      .eq('群組', 群組)
      .eq('貨品編號', 貨品編號);
  } else {
    // 新增
    await supabase.from('stock_summary').insert({
      群組,
      貨品編號,
      庫存箱數,
      庫存散數,
    });
  }
}

// 更新或新增使用者最後查詢商品
async function upsertUserLastProduct(user_id, 群組, 貨品編號) {
  const { data } = await supabase
    .from('user_last_product')
    .select('*')
    .eq('user_id', user_id)
    .eq('群組', 群組)
    .single();

  if (data) {
    await supabase
      .from('user_last_product')
      .update({ 貨品編號, updated_at: new Date().toISOString() })
      .eq('user_id', user_id)
      .eq('群組', 群組);
  } else {
    await supabase
      .from('user_last_product')
      .insert({ user_id, 群組, 貨品編號, updated_at: new Date().toISOString() });
  }
}

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
