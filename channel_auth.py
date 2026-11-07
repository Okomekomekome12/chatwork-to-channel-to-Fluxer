"""
ChannelWorks クライアントライブラリ

使い方:
    from channel_works import ChannelWorksClient

    client = ChannelWorksClient(
        email="you@example.com",
        password="your-password",
        log_level=ChannelWorksClient.LOG_DEBUG,
    )
    client.login()                 # sign_in + boot
    client.start_auto_touch()      # 10分ごとに自動で touch を叩いてトークン更新
    tokens = client.get_tokens()   # 最新トークンを取得
    ...
    client.stop_auto_touch()

log_level:
    0 = 何も出力しない
    1 = エラー・警告のみ (デフォルト)
    2 = デバッグログも出力
"""

import json
import threading
from collections import OrderedDict
from urllib.parse import urlencode

import requests


SIGN_IN_URI = "https://api.channel.works/desk/account/sign-in"
BOOT_URI = "https://api.channel.io/front/v8/elastic/plugins/b823a684-f97e-4e4e-bf12-48b70c1939ae/boot"
TOUCH_URI = "https://api.channel.works/desk/account/touch"

BOOT_URL_TEMPLATE = "https://channel.works/{room_id}/user-chats"
BOOT_AVATAR_URL = "https://cf.channel.io/avatar/emoji/bee.f7dc8e.png"

DEFAULT_TOUCH_INTERVAL = 600  # 10 分

# Set-Cookie から抜き出す対象トークン
_TOKEN_NAMES = ("x-account", "x-account-refresh", "ch-session-1", "ch-veil-id")


class ChannelWorksClient:
    # ログレベル
    LOG_NONE = 0
    LOG_ERROR = 1
    LOG_DEBUG = 2

    def __init__(
        self,
        room_id,
        email=None,
        password=None,
        log_level=LOG_ERROR,
        touch_interval=DEFAULT_TOUCH_INTERVAL,
        timeout=30,
    ):
        if not room_id:
            raise ValueError("room_id は必須です。")

        self.room_id = room_id
        self.email = email
        self.password = password
        self.log_level = log_level
        self.touch_interval = touch_interval
        self.timeout = timeout

        self._lock = threading.RLock()

        # 最新トークン（この値は常に上書きされる。固定値にはならない）
        self._x_account = None
        self._x_account_refresh = None
        self._ch_session_1 = None
        self._ch_veil_id = None

        # boot に必要なアカウント情報
        self._account_name = None
        self._account_email = None
        self._account_id = None
        self._member_hash = None

        # 自動 touch 用スレッド
        self._touch_thread = None
        self._touch_stop = threading.Event()

    # ------------------------------------------------------------------
    # ログ
    # ------------------------------------------------------------------
    def _log_debug(self, msg):
        if self.log_level >= self.LOG_DEBUG:
            print(f"[DEBUG] {msg}")

    def _log_error(self, msg):
        if self.log_level >= self.LOG_ERROR:
            print(f"[ERROR] {msg}")

    def set_log_level(self, level):
        self.log_level = level

    # ------------------------------------------------------------------
    # 内部ユーティリティ
    # ------------------------------------------------------------------
    @staticmethod
    def _common_headers():
        return {
            "accept": "application/json",
            "accept-language": "ja",
            "content-type": "application/json",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Chromium\";v=\"152\", \"Not?A_Brand\";v=\"24\", \"Google Chrome\";v=\"152\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "Referer": "https://channel.works/",
        }

    @staticmethod
    def _get_set_cookie_headers(resp):
        try:
            headers = resp.raw.headers.getlist("Set-Cookie")
            if headers:
                return headers
        except Exception:
            pass
        raw = resp.headers.get("Set-Cookie")
        return [raw] if raw else []

    @staticmethod
    def _extract_new_tokens(set_cookie_headers):
        new_tokens = {}
        for set_cookie in set_cookie_headers:
            first_part = set_cookie.split(";", 1)[0].strip()
            if "=" not in first_part:
                continue
            name, value = first_part.split("=", 1)
            name = name.strip()
            if name in _TOKEN_NAMES and value:
                new_tokens[name] = value
        return new_tokens

    def _build_cookie_header(self):
        cookies = OrderedDict()
        if self._ch_session_1:
            cookies["ch-session-1"] = self._ch_session_1
        if self._ch_veil_id:
            cookies["ch-veil-id"] = self._ch_veil_id
        if self._x_account:
            cookies["x-account"] = self._x_account
        if self._x_account_refresh:
            cookies["x-account-refresh"] = self._x_account_refresh
            cookies["x-account-refresh-bound"] = "false"
        return "; ".join(f"{k}={v}" for k, v in cookies.items())

    def _apply_new_tokens(self, new_tokens):
        """Set-Cookie から取得した新トークンを保持値に反映する。"""
        with self._lock:
            if "x-account" in new_tokens:
                self._x_account = new_tokens["x-account"]
            if "x-account-refresh" in new_tokens:
                self._x_account_refresh = new_tokens["x-account-refresh"]
            if "ch-session-1" in new_tokens:
                self._ch_session_1 = new_tokens["ch-session-1"]
            if "ch-veil-id" in new_tokens:
                self._ch_veil_id = new_tokens["ch-veil-id"]

    # ------------------------------------------------------------------
    # 認証
    # ------------------------------------------------------------------
    def sign_in(self):
        if not self.email or not self.password:
            raise ValueError("email と password を指定してください。")

        headers = self._common_headers()
        headers["x-supports-refresh"] = "true"
        body = json.dumps({"email": self.email, "password": self.password}).encode("utf-8")

        self._log_debug(f"sign_in POST {SIGN_IN_URI}")
        resp = requests.post(SIGN_IN_URI, headers=headers, data=body, timeout=self.timeout)

        if not (200 <= resp.status_code < 300):
            self._log_error(f"sign_in 失敗 status={resp.status_code} body={resp.text}")
            return resp

        set_cookies = self._get_set_cookie_headers(resp)
        new_tokens = self._extract_new_tokens(set_cookies)
        self._apply_new_tokens(new_tokens)
        self._log_debug(f"sign_in 取得トークン: {sorted(new_tokens.keys())}")

        if not self._x_account or not self._x_account_refresh:
            self._log_error("sign_in 応答から x-account / x-account-refresh を取得できませんでした。")

        try:
            data = resp.json()
        except ValueError:
            self._log_error("sign_in 応答の JSON パースに失敗しました。")
            return resp

        account_info = data["account"]
        with self._lock:
            self._account_name = account_info["name"]
            self._account_email = account_info["email"]
            self._account_id = account_info["id"]
            self._member_hash = data["memberHash"]

        self._log_debug("sign_in 成功")
        return resp

    def boot(self):
        if not self._account_id or not self._member_hash:
            self._log_error("boot 前に sign_in を実行してください。")
            return None

        profile = json.dumps(
            {
                "name": self._account_name,
                "email": self._account_email,
                "avatarUrl": BOOT_AVATAR_URL,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        
        boot_url_value = BOOT_URL_TEMPLATE.format(room_id=self.room_id)

        body = urlencode({
            "url": boot_url_value,   # ← 変更
            "profile": profile,
            "memberId": self._account_id,
            "memberHash": self._member_hash,
        }).encode("utf-8")

        headers = {
            "accept": "application/json",
            "accept-language": "ja",
            "content-type": "application/x-www-form-urlencoded",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Chromium\";v=\"152\", \"Not?A_Brand\";v=\"24\", \"Google Chrome\";v=\"152\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "sec-fetch-storage-access": "none",
            "Referer": "https://channel.works/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " # User-Agentは必須
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/152.0.0.0 Safari/537.36"
            ),
        }

        self._log_debug(f"boot POST {BOOT_URI} memberId={self._account_id}")
        resp = requests.post(BOOT_URI, headers=headers, data=body, timeout=self.timeout)

        if not (200 <= resp.status_code < 300):
            self._log_error(f"boot 失敗 status={resp.status_code} body={resp.text}")
            return resp

        # boot 応答で ch-session-1 / ch-veil-id が降ってくる場合に備えて反映
        self._apply_new_tokens(self._extract_new_tokens(self._get_set_cookie_headers(resp)))

        try:
            data = resp.json()
        except ValueError:
            self._log_error("boot 応答の JSON パースに失敗しました。")
            return resp

        with self._lock:
            self._ch_session_1 = data.get("sessionJWT", self._ch_session_1)
            self._ch_veil_id = data.get("veilId", self._ch_veil_id)

        self._log_debug("boot 成功")
        return resp

    def login(self):
        """sign_in → boot をまとめて実行する。"""
        self.sign_in()
        self.boot()

    # ------------------------------------------------------------------
    # touch
    # ------------------------------------------------------------------
    def touch(self):
        """最新トークンを使って touch を 1 回叩き、返ってきた新トークンで保持値を更新する。"""
        with self._lock:
            if not self._x_account:
                self._log_error("touch 実行不可: x-account 未取得")
                return None
            account = self._x_account
            cookie_header = self._build_cookie_header()

        headers = self._common_headers()
        headers["x-account"] = account
        headers["Cookie"] = cookie_header

        body = b'{"refreshOnly":true,"skipSessionExtension":false}'

        self._log_debug(f"touch POST {TOUCH_URI}")
        try:
            resp = requests.post(TOUCH_URI, headers=headers, data=body, timeout=self.timeout)
        except Exception as e:
            self._log_error(f"touch 通信例外: {e!r}")
            return None

        if not (200 <= resp.status_code < 300):
            self._log_error(f"touch 失敗 status={resp.status_code} body={resp.text}")
            return resp

        new_tokens = self._extract_new_tokens(self._get_set_cookie_headers(resp))
        self._apply_new_tokens(new_tokens)
        self._log_debug(f"touch 成功 更新トークン: {sorted(new_tokens.keys()) or '(更新なし)'}")
        return resp

    # ------------------------------------------------------------------
    # トークン取得
    # ------------------------------------------------------------------
    def get_tokens(self):
        """常に最新のトークンを辞書で返す。"""
        with self._lock:
            return {
                "x-account": self._x_account,
                "x-account-refresh": self._x_account_refresh,
                "ch-session-1": self._ch_session_1,
                "ch-veil-id": self._ch_veil_id,
            }

    # ------------------------------------------------------------------
    # 自動 touch
    # ------------------------------------------------------------------
    def start_auto_touch(self):
        """バックグラウンドで touch_interval 秒ごとに touch を実行する。"""
        with self._lock:
            if self._touch_thread and self._touch_thread.is_alive():
                self._log_debug("auto-touch は既に起動中")
                return
            self._touch_stop.clear()
            self._touch_thread = threading.Thread(
                target=self._touch_loop, name="ChannelWorksAutoTouch", daemon=True
            )
            self._touch_thread.start()
        self._log_debug(f"auto-touch 開始 (interval={self.touch_interval}s)")

    def stop_auto_touch(self):
        """自動 touch を停止する。"""
        self._touch_stop.set()
        thread = self._touch_thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
        self._touch_thread = None
        self._log_debug("auto-touch 停止")

    def _touch_loop(self):
        while not self._touch_stop.wait(self.touch_interval):
            try:
                self.touch()
            except Exception as e:
                self._log_error(f"auto-touch 例外: {e!r}")

    # ------------------------------------------------------------------
    # コンテキストマネージャ
    # ------------------------------------------------------------------
    def close(self):
        self.stop_auto_touch()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

