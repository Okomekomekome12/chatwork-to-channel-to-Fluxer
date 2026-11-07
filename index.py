from flask import Flask , jsonify
from channel_msg import ChannelMessenger
from channel_auth import ChannelWorksClient
import chatwork
import threading

app = Flask(__name__)

CW_API_TOKEN = ""
client = ChannelWorksClient(
    room_id="",
    email="",
    password="",
    log_level=ChannelWorksClient.LOG_DEBUG
)
client.login()
client.start_auto_touch()
def on_push(wrapper):
    entity = wrapper.get("entity") or {}
    print("受信:", entity.get("plainText"))


messager = ChannelMessenger(
    client=client,
    channel_slug="",
    group_path="/grops/114514",
    log_level=ChannelMessenger.LOG_DEBUG,
    on_push=on_push
)
messager.start()
@app.route("/", methods=["GET"])
def root():
    return "Ok" , 200

@app.route("/webhook", methods=["POST"])
def webhook():


    room
    room_id    = chatwork.webhook_get_roomid
    message_id = chatwork.webhook_get_message_id
    body       = chatwork.webhook_get_message
    account_id = chatwork.webhook_get_account_id

    cw = chatwork.setup(room_id,CW_API_TOKEN)

    return jsonify({"status" : "ok"}) , 200