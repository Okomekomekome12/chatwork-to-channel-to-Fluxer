import chatwork
import threading
import time
from flask import Flask , request , jsonify
import os
import fluxer
import wr
import requests

print("start")

ALLOWED_CHANNEL_IDS = {
    "1569680575495680017",
    "1565706890367729664",
    "1569680575495680016",
}



SECRET_TOKEN = None
TOKEN        = os.environ.get("FLUXER_BOT_TOKEN")
app          = Flask(__name__)
CW_API_TOKEN = os.environ.get("chatwork_api_token")
bot          = fluxer.Bot(command_prefix="!", intents=fluxer.Intents.default())
Fluxer_webhook_url = os.environ.get("fluxer_webhook_url")

cw_ITRSA     = chatwork.setup(453039746,CW_API_TOKEN)
if not TOKEN:
    print("API未設定")
    exit()

def fluxer_to_chatwork():
    return None

@app.route("/cw_webhook" , methods=["POST"])
def webhook():
    signature = request.headers.get("X-ChatWorkWebhookSignature")
    if not chatwork.webhook_verify_signature(request.data, signature, SECRET_TOKEN): # type: ignore
        return "invalid signature", 403
    data       = request.json
    room_id    = chatwork.webhook_get_roomid(data)
    body       = chatwork.webhook_get_message(data)
    account_id = chatwork.webhook_get_account_id(data)
    message_id = chatwork.webhook_get_message_id(data)
    print(body)
    if not body:
        return jsonify({"status": "ok"}), 200
    if body.count("[info]") >= 1:
        return jsonify({"status": "ok"}), 200
    
    username   = cw_ITRSA.get_user_name(account_id)


        
    message = body.replace("@everyone" , "")
    message = message.replace("@here" , "")
    requests.post(str(Fluxer_webhook_url), json={"content": f"#bot {username} : {message}", "username": "カレハ"})
    
    return jsonify({"status": "ok"}), 200
    

@bot.event
async def on_message(message):
        print(f"[{message.channel_id}] {message.author.username}: {message.content}")
        if message.content.count("#bot") >= 1:
            pass
        elif str(message.channel_id) in ALLOWED_CHANNEL_IDS:
            if message.author.global_name:
                FLUX_message = message.content
                edit_message_flux = FLUX_message.replace("[toall]","")
                cw_ITRSA.messagesend(f"[info][title]{message.author.global_name}からのメッセージ[/title]{edit_message_flux}[/info]") 

            else:
                FLUX_message = message.content
                edit_message_flux = FLUX_message.replace("[toall]","")
                cw_ITRSA.messagesend(f"[info][title]{message.author.username}からのメッセージ[/title]{edit_message_flux}[/info]") 


        if message.content == "/live?":
            await message.reply("生きてるお")
        if message.content == "おみくじ":
            kekka = wr.choice({"大吉": 500, "中吉" : 1000, "吉": 5000, "小吉" : 6000 , "凶": 3000 ,"大凶" : 1000, "超大吉(0.06%)\nおめでとう🎉" : 10 , "しあ吉(0.006%)\n？？？？？？？？？？？？？？？？？？？？\nすげぇな" : 1})
            await message.reply(kekka)
def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    bot.run(TOKEN)