import chatwork
import threading
import time
from flask import Flask , request , jsonify
import os
import fluxer
import wr

ALLOWED_CHANNEL_IDS = {
    "1569680575495680017",
    "1565706890367729664",
}



SECRET_TOKEN = None
TOKEN = os.environ.get("FLUXER_BOT_TOKEN")
app = Flask(__name__)
CW_API_TOKEN = os.environ.get("chatwork_api_token")
bot = fluxer.Bot(command_prefix="!", intents=fluxer.Intents.default())

cw_ITRSA = chatwork.setup(452904052,CW_API_TOKEN)
if not TOKEN:
    print("API未設定")
    exit()

def fluxer_to_chatwork():
    return None

@app.route("/cw_webhook")
def webhook():
    signature = request.headers.get("X-ChatWorkWebhookSignature")
    if not chatwork.webhook_verify_signature(request.data, signature, SECRET_TOKEN): # type: ignore
        return "invalid signature", 403
    ip         = request.headers.get('X-Forwarded-For').split(',')[0].strip()
    data       = request.json
    room_id    = chatwork.webhook_get_roomid(data)
    body       = chatwork.webhook_get_message(data)
    account_id = chatwork.webhook_get_account_id(data)
    message_id = chatwork.webhook_get_message_id(data)


    
    return jsonify({"status": "ok"}), 200
    

@bot.event
async def on_message(message):
        print(f"[{message.channel_id}] {message.author.username}: {message.content}")

        if str(message.channel_id) in ALLOWED_CHANNEL_IDS:
            if message.author.global_name:
                cw_ITRSA.messagesend(f"{message.author.global_name} : {message.content}") 

            else:
                cw_ITRSA.messagesend(f"{message.author.username} : {message.content}") 


        if message.content == "/live?":
            await message.reply("生きてるお")
        if message.content == "おみくじ":
            kekka = wr.choice({"大吉": 500, "中吉" : 1000, "吉": 5000, "小吉" : 6000 , "凶": 3000 ,"大凶" : 1000, "超大吉(0.06%)\nおめでとう🎉" : 10 , "しあ吉(0.006%)\n？？？？？？？？？？？？？？？？？？？？\nすげぇな" : 1})
            await message.reply(kekka)

bot.run(TOKEN)