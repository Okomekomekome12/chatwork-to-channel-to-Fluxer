"""
=============================================
                使い方
            Hello World例
        import chatwork
        cw = chatwork.setup("ルームid","APIトークン")
        cw.messagesend("hello world")

    機能一覧

    messagesend    メッセージを送信
    viewer     閲覧者を決める
    admin      管理者を決める
    member     メンバーを決める
    member_remove メンバーを削除する
    get_new_messages 最新のメッセージを取得(一度取得したメッセージは再度返さない)
                     msg = cw.get_new_messages → msg["body"] / msg["account_name"]
    get_message_link 最新のメッセージのリンクを取得
    get_user_name アカウントIDからユーザー名を取得
    get_sender_name メッセージIDから送信者名を取得(Webhook用・退室済みの人も取れる)
    create_task タスクを作成する
    delete_task タスクを削除する
    webhook_get_message()  webhookからメッセージ本文を取得
    webhook_get_roomid()　 webhookからルームIDを取得
    webhook_get_account_id() webhookからアカウントIDを取得
    webhook_get_message_id() webhookからメッセージIDを取得
    webhook_verify_signature() webhookの署名を検証
=============================================

"""


import requests
import hashlib
import hmac
import base64

# ================================================================
#  Webhook ヘルパー
# ================================================================

def webhook_get_message(data: dict):
    """Webhook ペイロードからメッセージ本文を返す"""
    return data.get("webhook_event", {}).get("body")

def webhook_get_roomid(data: dict):
    """Webhook ペイロードからルームIDを返す"""
    return data.get("webhook_event", {}).get("room_id")

def webhook_get_account_id(data: dict):
    return data.get("webhook_event", {}).get("account_id")

def webhook_get_message_id(data: dict):
    """Webhook ペイロードからメッセージIDを返す"""
    return data.get("webhook_event", {}).get("message_id")

def webhook_verify_signature(body_bytes: bytes, signature_header: str, secret_token: str) -> bool:
    """署名を検証して True / False を返す"""
    if secret_token is None:
        return True
    if signature_header is None:
        return False
    expected = base64.b64encode(
        hmac.new(
            base64.b64decode(secret_token),
            body_bytes,
            hashlib.sha256,
        ).digest()
    ).decode()
    return hmac.compare_digest(expected, signature_header)

class Message(dict):
    """get_new_messages が返すメッセージ(dictとして使える)。
    存在しないキーは KeyError ではなく None を返す。
    cw.get_new_messages() と括弧付きで呼んでも動く(新着がある場合)。
    """
    def __missing__(self, key):
        return None

    def __call__(self):
        return self


class setup:
    # ルームごとに「最後に取得したメッセージID」を保持する(setupを作り直しても共有される)
    _last_seen_message_ids = {}

    def __init__(self, room_id, api_token):
        self.room_id = room_id
        self.api_token = api_token

    def messagesend(self, message):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/messages"
        payload = {"body": message}
        headers = {"x-chatworktoken": self.api_token}
        
        response = requests.post(url, data=payload, headers=headers)
        print(response.text)
        if response.status_code != 200:
            return "error"
    def viewer(self, account_id):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/members"
        headers = {"x-chatworktoken": self.api_token}

        # 現在のメンバーを取得
        current_members = requests.get(url, headers=headers)
        members_data = current_members.json()

        # メンバーIDをリストに格納
        admin_ids = []
        member_ids = []
        readonly_ids = []
        #選別作業()
        for member in members_data:
            role = member["role"]
            if f"{member['account_id']}" == f"{account_id}":
                continue
            if role == "admin":
                admin_ids.append(str(member["account_id"]))
            elif role == "member":
                member_ids.append(str(member["account_id"]))
            elif role == "readonly":
                readonly_ids.append(str(member["account_id"]))
        readonly_ids.append(str(account_id))

        payload = {
            "members_admin_ids": ",".join(admin_ids),
            "members_member_ids": ",".join(member_ids),
            "members_readonly_ids": ",".join(readonly_ids)
        }

        response = requests.put(url, data=payload, headers=headers)
        print(response.text)
        if response.status_code != 200:
            return "error"
    def admin(self,account_id):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/members"
        headers = {"x-chatworktoken": self.api_token}

        # 現在のメンバーを取得
        current_members = requests.get(url, headers=headers)
        members_data = current_members.json()

        # 既存のメンバーIDをリストに格納
        admin_ids = []
        member_ids = []
        readonly_ids = []
        #選別作業()
        for member in members_data:
            role = member["role"]
            if f"{member['account_id']}" == f"{account_id}":
                continue
            elif role == "admin":
                admin_ids.append(str(member["account_id"]))
            elif role == "member":
                member_ids.append(str(member["account_id"]))
            elif role == "readonly":
                readonly_ids.append(str(member["account_id"]))
        admin_ids.append(str(account_id))

        payload = {
            "members_admin_ids": ",".join(admin_ids),
            "members_member_ids": ",".join(member_ids),
            "members_readonly_ids": ",".join(readonly_ids)
        }

        response = requests.put(url, data=payload, headers=headers)
        print(response.text)
        if response.status_code != 200:
            return "error"
    def member(self,account_id):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/members"
        headers = {"x-chatworktoken": self.api_token}

        # 現在のメンバーを取得
        current_members = requests.get(url, headers=headers)
        members_data = current_members.json()

        # 既存のメンバーIDをリストに格納
        admin_ids = []
        member_ids = []
        readonly_ids = []
        #選別作業()
        for member in members_data:
            role = member["role"]
            if f"{member['account_id']}" == f"{account_id}":
                continue
            elif role == "admin":
                admin_ids.append(str(member["account_id"]))
            elif role == "member":
                member_ids.append(str(member["account_id"]))
            elif role == "readonly":
                readonly_ids.append(str(member["account_id"]))
        member_ids.append(str(account_id))

        payload = {
            "members_admin_ids": ",".join(admin_ids),
            "members_member_ids": ",".join(member_ids),
            "members_readonly_ids": ",".join(readonly_ids)
        }

        response = requests.put(url, data=payload, headers=headers)
        print(response.text)
        if response.status_code != 200:
            return "error"
    def member_remove(self,account_id):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/members"
        headers = {"x-chatworktoken": self.api_token}
        # 現在のメンバーを取得
        
        current_members = requests.get(url, headers=headers)
        members_data = current_members.json()
        # メンバーIDをリストに格納
        admin_ids = []
        member_ids = []
        readonly_ids = []
        #選別作業
        for member in members_data:
            role = member["role"]
            if f"{member['account_id']}" == f"{account_id}":
                continue
            elif role == "admin":
                admin_ids.append(str(member["account_id"]))
            elif role == "member":
                member_ids.append(str(member["account_id"]))
            elif role == "readonly":
                readonly_ids.append(str(member["account_id"]))
        payload = {
            "members_admin_ids": ",".join(admin_ids),
            "members_member_ids": ",".join(member_ids),
            "members_readonly_ids": ",".join(readonly_ids)
        }
        response = requests.put(url, data=payload, headers=headers)
        print(response.text)
    @property
    def get_new_messages(self):
        """最新のメッセージを返す(括弧なしで使える)。

            msg = cw.get_new_messages
            msg["body"]          本文
            msg["account_name"]  送信者名
            msg["account_id"]    送信者のアカウントID
            msg["message_id"]    メッセージID

        既に一度取得したメッセージ・メッセージが無い場合は、
        表示せず None を返す(if msg: で判定してから使うこと)。
        注意: アクセスするたびに「取得済み」になるので、
        msg に一度入れてから msg["body"] などを使うこと。
        """
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/messages"
        headers = {"x-chatworktoken": self.api_token}
        params = {"force": 1}

        response = requests.get(url, headers=headers, params=params)

        # メッセージが無い場合は 204 (本文なし) が返る
        if response.status_code != 200 or not response.text.strip():
            return None

        messages = response.json()
        if not messages:
            return None

        # 最新のメッセージを取得(messages[-1]にすることで最新100件のところ最新1件のみ取得)
        latest = messages[-1]
        message_id = str(latest["message_id"])

        # 既に取得済みのメッセージなら何も表示せず None を返す
        if self._last_seen_message_ids.get(str(self.room_id)) == message_id:
            return None

        # 新しいメッセージなので記録して表示
        self._last_seen_message_ids[str(self.room_id)] = message_id
        msg = Message(latest)
        msg["account_name"] = latest["account"]["name"]
        msg["account_id"] = latest["account"]["account_id"]
        print(f"送信者: {msg['account_name']}")
        print(f"メッセージ: {msg['body']}")
        return msg
    def get_message_link(self):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/messages"
        headers = {"x-chatworktoken": self.api_token}
        params = {"force": 1}

        response = requests.get(url, headers=headers, params=params)
        messages = response.json()

        latest = messages[-1]
        message_id = latest["message_id"]
        link = f"https://www.chatwork.com/#!rid{self.room_id}-{message_id}"
        print(f"最新メッセージリンク: {link}")
        if response.status_code != 200:
            return "error"
        else:
            return link
    def create_task(self, task,account_id):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/tasks"
        payload = {
            "limit_type": "none",
            "body": task,
            "to_ids": account_id
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
            "x-chatworktoken": self.api_token
        }
        response = requests.post(url, data=payload, headers=headers)
        print(response.text)
        #変数で指定されたときtask_idを返すようにする
        task_id = response.json()["task_ids"]
        return task_id
    def delete_task(self,task_id):

        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/tasks/{task_id}/status"

        payload = { "body": "done" }
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
            "x-chatworktoken": self.api_token
        }

        response = requests.put(url, data=payload, headers=headers)

        print(response.text)
        if response.status_code != 200:
            return "error"
    def delete_message(self,message_id):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/messages/{message_id}"
        headers = {"accept": "application/json",
                   "x-chatworktoken": self.api_token,
                    }
        response = requests.delete(url, headers=headers)
        print(response.text)
    def get_description(self):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}"

        headers = {
            "accept": "application/json",
            "x-chatworktoken": self.api_token
        }

        response = requests.get(url, headers=headers)
        description = response.json()["description"]
        if response.status_code != 200:
            return "error"
        else:
            return description
    def get_room_name(self):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}"

        headers = {
            "accept": "application/json",
            "x-chatworktoken": self.api_token
        }

        response = requests.get(url, headers=headers)
        name = response.json()["name"]
        if response.status_code != 200:
            return "error"
        else:
            return name
    def edit_room_description(self,description,name):
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}"

        payload = {
            "name": name,
            "description": description,
            "icon_preset": "group"
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
            "x-chatworktoken": self.api_token
        }

        response = requests.put(url, data=payload, headers=headers)
        if response.status_code != 200:
            return "error"
        else:
            return response.text
    def is_admin(self, account_id) -> bool:
        """指定した account_id が管理者かどうかを返す"""
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/members"
        headers = {"x-chatworktoken": self.api_token}

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return False

        for member in response.json():
            if str(member["account_id"]) == str(account_id):
                return member["role"] == "admin"

        return False 
    def get_user_name(self, account_id):
        """指定した account_id のユーザー名を返す。
        ルームのメンバー → コンタクト一覧の順に検索する。
        見つからない場合・取得失敗時は None を返す。
        """
        headers = {"x-chatworktoken": self.api_token}

        # 1. ルームのメンバーから探す
        url = f"https://api.chatwork.com/v2/rooms/{self.room_id}/members"
        response = requests.get(url, headers=headers)
        if response.status_code == 200 and response.text.strip():
            for member in response.json():
                if str(member["account_id"]) == str(account_id):
                    return member["name"]

        # 2. 見つからなければコンタクト一覧から探す
        response = requests.get("https://api.chatwork.com/v2/contacts", headers=headers)
        if response.status_code == 200 and response.text.strip():
            for contact in response.json():
                if str(contact["account_id"]) == str(account_id):
                    return contact["name"]

        return None
    def get_sender_name(self, message_id, room_id=None):
        """メッセージIDから送信者名を取得する(Webhook用)。
        メッセージ自体に送信者情報が入っているので、既にルームを抜けた人や
        コンタクトにいない人でも名前が取れる。失敗時は None を返す。
        """
        room_id = room_id or self.room_id
        url = f"https://api.chatwork.com/v2/rooms/{room_id}/messages/{message_id}"
        headers = {"x-chatworktoken": self.api_token}

        response = requests.get(url, headers=headers)
        if response.status_code != 200 or not response.text.strip():
            return None

        return response.json().get("account", {}).get("name")

def auto_accept_contacts(api_token):
    """コンタクト申請を全て自動承認する"""
    url = "https://api.chatwork.com/v2/incoming_requests"
    headers = {"X-ChatWorkToken": api_token}
    
    # コンタクト申請一覧を取得
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        return
    
    requests_list = response.json()
    
    for req in requests_list:
        request_id = req["request_id"]
        name = req["name"]
        
        # 承認
        accept_url = f"https://api.chatwork.com/v2/incoming_requests/{request_id}"
        result = requests.put(accept_url, headers=headers)
        
        if result.status_code == 200:
            print(f"コンタクト承認: {name}")
        else:
            print(f"コンタクト承認失敗: {name}")