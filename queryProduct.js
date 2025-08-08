require('dotenv').config();
const express = require('express');
const line = require('@line/bot-sdk');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(express.json());

const config = {
  channelAccessToken: Ji4Q6jEyce6RdoBB1xS137OH5ITxIGOjQWleUcqGyUAJk0iPopIr8c51zLGA9DLYRssKJzbZshw9D8psdeBk4mYof1O/Ac4JoAP5PSntZ1kdPmJC2Xmsqg0lRTmIzYRkWaDK2AvktqIHiyHBAwelPwdB04t89/1O/w1cDnyilFU=,
  channelSecret: 8b48b1e700befe111eeb8cd9cc923634,
};

const client = new line.Client(config);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

app.post('/webhook', line.middleware(config), async (req, res) => {
  const events = req.body.events;

  const results = await Promise.all(events.map(async (event) => {
    if (event.type === 'message' && event.message.type === 'text') {
      const msg = event.message.text.trim();

      // 假設用戶輸入「查 可樂」
      if (msg.startsWith('查')) {
        const keyword = msg.slice(1).trim();

        const { data, error } = await supabase
          .from('products')
          .select('*')
          .ilike('name', `%${keyword}%`);

        if (error || !data || data.length === 0) {
          return client.replyMessage(event.replyToken, {
            type: 'text',
            text: `❌ 找不到與「${keyword}」有關的商品`,
          });
        }

        const item = data[0];
        return client.replyMessage(event.replyToken, {
          type: 'text',
          text: `✅ 查到商品：${item.name}\n目前庫存：${item.stock || 0}`,
        });
      }

      return client.replyMessage(event.replyToken, {
        type: 'text',
        text: `請輸入「查 關鍵字」來查詢商品`,
      });
    }
  }));

  res.json(results);
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`LINE Bot is running on http://localhost:${port}`);
});
