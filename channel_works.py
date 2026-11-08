"""
ChannelWorks 統合ライブラリ (ログイン + 受信 + 送信 を 1 クラスに統合)

使い方:
    from channel_works import ChannelWorks

    cw = ChannelWorks(
        room_id="...",
        email="you@example.com",
        password="your-password",
        # channel_slug は省略可 (省略時は room_id で channel_id を自動取得)
        # channel_id="...", manager_id="...",   # 直接指定してもよい
        group_id="574628",
        group_ids=["111111", "222222"],   # 追加で受信したい部屋 (省略可)
        on_push=lambda wrapper: print((wrapper.get("entity") or {}).get("plainText")),
    )
    cw.start()                      # login + 自動touch + WebSocket受信 をまとめて開始
    cw.send("こんにちは")            # 送信
    print(cw.get_latest())          # 最新メッセージ取得

    cw.wait_message()                    # 全部屋のメッセージ (届くまで無期限に待つ)
    cw.wait_message(group_id="574628")   # その部屋のメッセージだけ
    cw.join_group("333333")              # 起動後に受信する部屋を追加

    cw.wait()                       # Ctrl+C まで待機
    cw.stop()

    # with 文でも使える
    with ChannelWorks(...) as cw:
        cw.send("hello")

log_level:
    0 = 何も出力しない / 1 = エラーのみ (デフォルト) / 2 = デバッグ (送受信パケット全出力)
"""

import json
import queue
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlencode

import requests
import websocket


# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
SIGN_IN_URI = "https://api.channel.works/desk/account/sign-in"
BOOT_URI = "https://api.channel.io/front/v8/elastic/plugins/b823a684-f97e-4e4e-bf12-48b70c1939ae/boot"
TOUCH_URI = "https://api.channel.works/desk/account/touch"
API_BASE = "https://api.channel.works/desk/channels"

BOOT_URL_TEMPLATE = "https://channel.works/{room_id}/user-chats"
BOOT_AVATAR_URL = "https://cf.channel.io/avatar/emoji/bee.f7dc8e.png"

WS_URL = "wss://desk-ws.channel.io/socket.io/?platform=web&EIO=4&transport=websocket"
ACCOUNT_WS_URL = "wss://account-ws.channel.io/socket.io/?platform=web&EIO=4&transport=websocket"

DEFAULT_GROUP_ID = "574628"
DEFAULT_TOUCH_INTERVAL = 600  # 10 分

_TOKEN_NAMES = ("x-account", "x-account-refresh", "ch-session-1", "ch-veil-id")

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
)

JST = timezone(timedelta(hours=9))


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------
def _timestamp_to_text(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return str(value)
    if value < 100_000_000_000:
        return str(value)
    dt_utc = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    dt_jst = dt_utc.astimezone(JST)
    return f"{dt_jst:%Y-%m-%d %H:%M:%S.%f} JST (UTC {dt_utc:%Y-%m-%d %H:%M:%S.%f})"


def _format_json_with_timestamps(value: Any, key: Optional[str] = None) -> Any:
    if isinstance(value, dict):
        return {k: _format_json_with_timestamps(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_format_json_with_timestamps(v) for v in value]
    if key and key.lower().endswith("at") and isinstance(value, (int, float)):
        return {"epoch_ms": value, "formatted": _timestamp_to_text(value)}
    return value


def _get_nested(data: Any, *path: str) -> Any:
    current = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _find_first_key(data: Any, key: str) -> Any:
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for value in data.values():
            result = _find_first_key(value, key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for value in data:
            result = _find_first_key(value, key)
            if result is not None:
                return result
    return None


def _parse_socketio_event(packet: str) -> Tuple[Optional[str], Optional[list]]:
    if not packet.startswith("42"):
        return None, None
    payload_text = packet[2:]
    if payload_text.startswith("/"):
        try:
            namespace, payload_text = payload_text.split(",", 1)
        except ValueError:
            return None, None
    else:
        namespace = "/"
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return namespace, None
    if not isinstance(payload, list):
        return namespace, None
    return namespace, payload


class _LoggingWS:
    """WebSocket の送受信をデバッグログに出すだけの薄いラッパ。"""

    def __init__(self, ws: websocket.WebSocket, label: str, owner: "ChannelWorks") -> None:
        self._ws = ws
        self._label = label
        self._owner = owner
        self._send_count = 0
        self._recv_count = 0

    def send(self, payload: Any) -> None:
        self._send_count += 1
        text = payload.decode("utf-8", errors="replace") if isinstance(payload, (bytes, bytearray)) else payload
        self._owner._log_block_debug(f"{self._label}-SEND", f"#{self._send_count} ({len(text)} bytes)", text)
        self._ws.send(payload)

    def recv(self) -> Optional[str]:
        packet = self._ws.recv()
        self._recv_count += 1
        if packet is None:
            self._owner._log_debug(f"{self._label}-RECV", f"#{self._recv_count} (None / 切断)")
            return None
        text = packet.decode("utf-8", errors="replace") if isinstance(packet, (bytes, bytearray)) else packet
        self._owner._log_block_debug(f"{self._label}-RECV", f"#{self._recv_count} ({len(text)} chars)", text)
        return text

    def settimeout(self, t: float) -> None:
        self._ws.settimeout(t)

    def close(self) -> None:
        try:
            self._ws.close()
        finally:
            self._owner._log_debug(self._label, "WebSocket をクローズしました。")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ws, name)


# ---------------------------------------------------------------------------
# 本体
# ---------------------------------------------------------------------------
class ChannelWorks:
    LOG_NONE = 0
    LOG_ERROR = 1
    LOG_DEBUG = 2

    def __init__(
        self,
        room_id: str,
        email: Optional[str] = None,
        password: Optional[str] = None,
        channel_slug: Optional[str] = None,
        channel_id: Optional[str] = None,
        manager_id: Optional[str] = None,
        group_id: str = DEFAULT_GROUP_ID,
        group_ids: Optional[list] = None,
        on_push: Optional[Callable[[Dict[str, Any]], None]] = None,
        show_incoming: bool = True,
        log_level: int = LOG_ERROR,
        touch_interval: float = DEFAULT_TOUCH_INTERVAL,
        timeout: float = 30.0,
        heartbeat_interval: float = 30.0,
        heartbeat_response_timeout: float = 5.0,
        token_check_interval: float = 1.0,
        channel_recv_timeout: float = 3.0,
        account_recv_timeout: float = 1.0,
    ) -> None:
        if not room_id:
            raise ValueError("room_id は必須です。")

        self.room_id = room_id
        self.email = email
        self.password = password
        self.channel_slug = channel_slug
        self.channel_id = str(channel_id) if channel_id is not None else None
        self.manager_id = str(manager_id) if manager_id is not None else None
        self.group_id = str(group_id)
        self.on_push = on_push
        self.show_incoming = show_incoming
        self.log_level = log_level
        self.touch_interval = touch_interval
        self.timeout = timeout

        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_response_timeout = heartbeat_response_timeout
        self.token_check_interval = token_check_interval
        self.channel_recv_timeout = channel_recv_timeout
        self.account_recv_timeout = account_recv_timeout

        self._lock = threading.RLock()

        # トークン (常に最新値で上書きされる)
        self._x_account: Optional[str] = None
        self._x_account_refresh: Optional[str] = None
        self._ch_session_1: Optional[str] = None
        self._ch_veil_id: Optional[str] = None

        # boot に必要なアカウント情報
        self._account_name = None
        self._account_email = None
        self._account_id = None
        self._member_hash = None

        # スレッド
        self._touch_thread: Optional[threading.Thread] = None
        self._touch_stop = threading.Event()
        self._stop_event = threading.Event()
        self._channel_thread: Optional[threading.Thread] = None
        self._account_thread: Optional[threading.Thread] = None

        self._logged_in = False

        # 受信したメッセージの保存先
        self.last_message: Optional[str] = None            # 最後に受信した本文
        self.last_wrapper: Optional[Dict[str, Any]] = None  # 最後に受信した生データ
        self._inbox: "queue.Queue[str]" = queue.Queue(maxsize=1000)  # 全部屋共通

        # join する部屋 (group_id + group_ids)
        self._join_groups = []
        for g in [self.group_id] + [str(x) for x in (group_ids or [])]:
            if g not in self._join_groups:
                self._join_groups.append(g)
        self._pending_joins: "queue.Queue[str]" = queue.Queue()  # 起動後に追加された join

        # 部屋ごとの受信キュー
        self._room_inboxes: Dict[str, "queue.Queue[str]"] = {}

    @property
    def group_path(self) -> str:
        return f"/groups/{self.group_id}"

    # ------------------------------------------------------------------
    # ログ
    # ------------------------------------------------------------------
    @staticmethod
    def _now_str() -> str:
        return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _log_debug(self, tag: str, msg: str) -> None:
        if self.log_level >= self.LOG_DEBUG:
            print(f"[{self._now_str()}] [{tag}] {msg}", flush=True)

    def _log_error(self, tag: str, msg: str) -> None:
        if self.log_level >= self.LOG_ERROR:
            print(f"[{self._now_str()}] [{tag}] {msg}", flush=True)

    def _log_block_debug(self, tag: str, title: str, body: str) -> None:
        if self.log_level < self.LOG_DEBUG:
            return
        ts = self._now_str()
        print(f"\n[{ts}] [{tag}] ===== {title} =====", flush=True)
        print(body, flush=True)
        print(f"[{ts}] [{tag}] ===== /{title} =====", flush=True)

    def set_log_level(self, level: int) -> None:
        self.log_level = level

    # ==================================================================
    # 認証 (sign_in / boot / touch)
    # ==================================================================
    @staticmethod
    def _common_headers() -> Dict[str, str]:
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
    def _get_set_cookie_headers(resp: requests.Response):
        try:
            headers = resp.raw.headers.getlist("Set-Cookie")
            if headers:
                return headers
        except Exception:
            pass
        raw = resp.headers.get("Set-Cookie")
        return [raw] if raw else []

    @staticmethod
    def _extract_new_tokens(set_cookie_headers) -> Dict[str, str]:
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

    def _build_cookie_header(self) -> str:
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

    def _apply_new_tokens(self, new_tokens: Dict[str, str]) -> None:
        with self._lock:
            if "x-account" in new_tokens:
                self._x_account = new_tokens["x-account"]
            if "x-account-refresh" in new_tokens:
                self._x_account_refresh = new_tokens["x-account-refresh"]
            if "ch-session-1" in new_tokens:
                self._ch_session_1 = new_tokens["ch-session-1"]
            if "ch-veil-id" in new_tokens:
                self._ch_veil_id = new_tokens["ch-veil-id"]

    def get_tokens(self) -> Dict[str, Optional[str]]:
        """常に最新のトークンを辞書で返す。"""
        with self._lock:
            return {
                "x-account": self._x_account,
                "x-account-refresh": self._x_account_refresh,
                "ch-session-1": self._ch_session_1,
                "ch-veil-id": self._ch_veil_id,
            }

    def sign_in(self) -> requests.Response:
        if not self.email or not self.password:
            raise ValueError("email と password を指定してください。")

        headers = self._common_headers()
        headers["x-supports-refresh"] = "true"
        body = json.dumps({"email": self.email, "password": self.password}).encode("utf-8")

        self._log_debug("AUTH", f"sign_in POST {SIGN_IN_URI}")
        resp = requests.post(SIGN_IN_URI, headers=headers, data=body, timeout=self.timeout)
        if not (200 <= resp.status_code < 300):
            raise RuntimeError(f"sign_in 失敗 status={resp.status_code} body={resp.text}")

        new_tokens = self._extract_new_tokens(self._get_set_cookie_headers(resp))
        self._apply_new_tokens(new_tokens)
        self._log_debug("AUTH", f"sign_in 取得トークン: {sorted(new_tokens.keys())}")

        if not self._x_account or not self._x_account_refresh:
            raise RuntimeError("sign_in 応答から x-account / x-account-refresh を取得できませんでした。")

        try:
            data = resp.json()
            account_info = data["account"]
            with self._lock:
                self._account_name = account_info["name"]
                self._account_email = account_info["email"]
                self._account_id = account_info["id"]
                self._member_hash = data["memberHash"]
        except (ValueError, KeyError) as exc:
            raise RuntimeError(f"sign_in 応答の解析に失敗しました: {exc!r}") from exc

        self._log_debug("AUTH", "sign_in 成功")
        return resp

    def boot(self) -> requests.Response:
        if not self._account_id or not self._member_hash:
            raise RuntimeError("boot 前に sign_in を実行してください。")

        profile = json.dumps(
            {"name": self._account_name, "email": self._account_email, "avatarUrl": BOOT_AVATAR_URL},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        body = urlencode(
            {
                "url": BOOT_URL_TEMPLATE.format(room_id=self.room_id),
                "profile": profile,
                "memberId": self._account_id,
                "memberHash": self._member_hash,
            }
        ).encode("utf-8")

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
            "User-Agent": _USER_AGENT,  # User-Agent は必須
        }

        self._log_debug("AUTH", f"boot POST {BOOT_URI} memberId={self._account_id}")
        resp = requests.post(BOOT_URI, headers=headers, data=body, timeout=self.timeout)
        if not (200 <= resp.status_code < 300):
            raise RuntimeError(f"boot 失敗 status={resp.status_code} body={resp.text}")

        self._apply_new_tokens(self._extract_new_tokens(self._get_set_cookie_headers(resp)))

        try:
            data = resp.json()
        except ValueError as exc:
            raise RuntimeError("boot 応答の JSON パースに失敗しました。") from exc

        with self._lock:
            self._ch_session_1 = data.get("sessionJWT", self._ch_session_1)
            self._ch_veil_id = data.get("veilId", self._ch_veil_id)

        self._log_debug("AUTH", "boot 成功")
        return resp

    def login(self) -> None:
        """sign_in → boot をまとめて実行する。"""
        self.sign_in()
        self.boot()
        self._logged_in = True

    def touch(self) -> Optional[requests.Response]:
        """touch を 1 回叩き、返ってきた新トークンで保持値を更新する。"""
        with self._lock:
            if not self._x_account:
                self._log_error("AUTH", "touch 実行不可: x-account 未取得")
                return None
            account = self._x_account
            cookie_header = self._build_cookie_header()

        headers = self._common_headers()
        headers["x-account"] = account
        headers["Cookie"] = cookie_header
        body = b'{"refreshOnly":true,"skipSessionExtension":false}'

        self._log_debug("AUTH", f"touch POST {TOUCH_URI}")
        try:
            resp = requests.post(TOUCH_URI, headers=headers, data=body, timeout=self.timeout)
        except Exception as exc:
            self._log_error("AUTH", f"touch 通信例外: {exc!r}")
            return None

        if not (200 <= resp.status_code < 300):
            self._log_error("AUTH", f"touch 失敗 status={resp.status_code} body={resp.text}")
            return resp

        new_tokens = self._extract_new_tokens(self._get_set_cookie_headers(resp))
        self._apply_new_tokens(new_tokens)
        self._log_debug("AUTH", f"touch 成功 更新トークン: {sorted(new_tokens.keys()) or '(更新なし)'}")
        return resp

    def start_auto_touch(self) -> None:
        with self._lock:
            if self._touch_thread and self._touch_thread.is_alive():
                return
            self._touch_stop.clear()
            self._touch_thread = threading.Thread(
                target=self._touch_loop, name="ChannelWorksAutoTouch", daemon=True
            )
            self._touch_thread.start()
        self._log_debug("AUTH", f"auto-touch 開始 (interval={self.touch_interval}s)")

    def stop_auto_touch(self) -> None:
        self._touch_stop.set()
        thread = self._touch_thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
        self._touch_thread = None
        self._log_debug("AUTH", "auto-touch 停止")

    def _touch_loop(self) -> None:
        while not self._touch_stop.wait(self.touch_interval):
            try:
                self.touch()
            except Exception as exc:
                self._log_error("AUTH", f"auto-touch 例外: {exc!r}")

    # ==================================================================
    # 送信 / メッセージ取得 (REST)
    # ==================================================================
    def _api_headers_and_cookies(self):
        tokens = self.get_tokens()
        x_account = tokens.get("x-account")
        if not x_account:
            raise RuntimeError("x-account が未取得です。先に login() / start() を実行してください。")

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "origin": "https://channel.works",
            "referer": "https://channel.works/",
            "user-agent": _USER_AGENT,
            "x-account": x_account,
        }
        cookies = {"x-account": x_account}
        if tokens.get("ch-veil-id"):
            cookies["ch-veil-id"] = tokens["ch-veil-id"]
        if tokens.get("ch-session-1"):
            cookies["ch-session-1"] = tokens["ch-session-1"]
        return headers, cookies

    def _api_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """401 の場合は touch でトークンを更新して 1 回だけ再試行する。"""
        resp = None
        for attempt in range(2):
            headers, cookies = self._api_headers_and_cookies()
            resp = requests.request(
                method, url, headers=headers, cookies=cookies, timeout=self.timeout, **kwargs
            )
            if resp.status_code == 401 and attempt == 0:
                self._log_debug("API", "401 → touch して再試行")
                self.touch()
                continue
            break
        return resp

    def _messages_url(self, group_id: Optional[str] = None) -> str:
        self._ensure_ids()
        gid = str(group_id) if group_id is not None else self.group_id
        return f"{API_BASE}/{self.channel_id}/groups/{gid}/messages"

    def send(self, text: str, group_id: Optional[str] = None) -> requests.Response:
        """メッセージを送信する。group_id を省略すると self.group_id 宛て。"""
        payload = {
            "requestId": f"desk-web-{uuid.uuid4().hex[:12]}",
            "blocks": [{"type": "text", "value": text}],
            "buttons": None,
            "form": None,
            "webPage": None,
            "files": None,
            "customPayload": None,
        }
        resp = self._api_request("POST", self._messages_url(group_id), json=payload)
        if resp.ok:
            print(f'送信成功 : "{text}"', flush=True)
        else:
            self._log_error("SEND", f"送信失敗 status={resp.status_code} body={resp.text[:300]}")
        return resp

    def get_messages(
        self, limit: int = 1, sort_order: str = "desc", group_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        params = {"sortOrder": sort_order, "limit": limit, "logFolded": "false"}
        resp = self._api_request("GET", self._messages_url(group_id), params=params)
        if not resp.ok:
            self._log_error("API", f"メッセージ取得失敗 status={resp.status_code} body={resp.text[:300]}")
            return None
        return resp.json()

    def get_latest(self, group_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        data = self.get_messages(limit=1, group_id=group_id)
        if not data:
            return None
        messages = data.get("messages") or []
        return messages[0] if messages else None

    # ==================================================================
    # ID 解決
    # ==================================================================
    def _ensure_ids(self) -> None:
        if self.manager_id is not None and self.channel_id is not None:
            return
        with self._lock:
            if self.manager_id is not None and self.channel_id is not None:
                return
            if not self._logged_in:
                self.login()
            self._resolve_ids()

    def _resolve_ids(self) -> None:
        """manager_id / channel_id を GET /desk/channels/{x} から取得する。

        {x} の候補を channel_id → room_id → channel_slug の順に試し、
        成功した最初の応答から不足している ID を取り出す。
        全部失敗した場合は、各候補のステータスとサーバー応答を含めて例外にする。
        """
        candidates = []
        for c in (self.channel_id, self.room_id, self.channel_slug):
            if c and str(c) not in candidates:
                candidates.append(str(c))

        data = None
        used = None
        errors = []
        for cand in candidates:
            url = f"{API_BASE}/{cand}"
            self._log_debug("HTTP", f"GET {url}")
            resp = self._api_request("GET", url)
            self._log_debug("HTTP", f"-> status={resp.status_code} ({url})")
            if not resp.ok:
                errors.append(f"  {url} -> status={resp.status_code} body={resp.text[:300]}")
                continue
            try:
                data = resp.json()
            except ValueError:
                errors.append(f"  {url} -> JSON ではない応答 body={resp.text[:300]}")
                continue
            used = cand
            self._log_block_debug("HTTP-BODY", url, json.dumps(data, ensure_ascii=False))
            break

        if data is None:
            raise RuntimeError(
                "チャンネル情報を取得できませんでした。試した候補:\n"
                + "\n".join(errors)
                + "\nブラウザの開発者ツール(ネットワーク)で "
                "api.channel.works/desk/channels/数字/... の数字を探し、"
                "channel_id= に指定してください。"
            )

        if self.manager_id is None:
            manager_id = _get_nested(data, "manager", "id")
            if manager_id is None:
                manager = _find_first_key(data, "manager")
                if isinstance(manager, dict):
                    manager_id = manager.get("id")
            if manager_id is None:
                raise RuntimeError(
                    f"{API_BASE}/{used} の JSON から manager.id を取得できませんでした。"
                    "manager_id= を直接指定してください。"
                )
            self.manager_id = str(manager_id)

        if self.channel_id is None:
            channel_id = _get_nested(data, "channel", "id")
            if channel_id is None:
                channel_id = _find_first_key(data, "id")
            if channel_id is None:
                raise RuntimeError(
                    f"{API_BASE}/{used} の JSON から channel.id を取得できませんでした。"
                    "channel_id= を直接指定してください。"
                )
            self.channel_id = str(channel_id)

        self._log_debug(
            "HTTP", f"解決: channel_id={self.channel_id} manager_id={self.manager_id} (via {used})"
        )

    # ==================================================================
    # 受信 (WebSocket)
    # ==================================================================
    def start(self) -> None:
        """login (未ログインなら) + 自動touch + channel/account WebSocket 受信をまとめて開始する。"""
        with self._lock:
            if self._is_running():
                self._log_debug("MAIN", "既に起動中です。")
                return

            if not self._logged_in:
                self.login()
            self.start_auto_touch()
            self._ensure_ids()

            self._stop_event.clear()

            self._channel_thread = threading.Thread(
                target=self._channel_worker, name="channel-ws", daemon=True
            )
            self._account_thread = threading.Thread(
                target=self._account_worker, name="account-ws", daemon=True
            )
            self._channel_thread.start()
            self._account_thread.start()
            self._log_debug("MAIN", f"起動: manager_id={self.manager_id} channel_id={self.channel_id}")

    def join_group(self, group_id: str) -> None:
        """起動後に部屋を追加で受信対象にする (起動前に呼んでもよい)。"""
        gid = str(group_id)
        with self._lock:
            if gid in self._join_groups:
                return
            self._join_groups.append(gid)
        if self._is_running():
            self._pending_joins.put(gid)

    def wait_message(
        self, timeout: Optional[float] = None, group_id: Optional[str] = None
    ) -> Optional[str]:
        """次のメッセージが届くまで待って本文を返す。

        group_id=None  : 受信中の全部屋のメッセージ
        group_id="123" : その部屋のメッセージだけ
        timeout=None   : 届くまで無期限に待つ (タイムアウトしない)
        timeout=数値   : その秒数で来なければ None

        内部では短い間隔でポーリングするため Ctrl+C で中断でき、
        受信スレッドが終了している場合は例外で知らせる。
        """
        if group_id is None:
            q = self._inbox
        else:
            gid = str(group_id)
            with self._lock:
                q = self._room_inboxes.setdefault(gid, queue.Queue(maxsize=1000))

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            # 受信スレッドが死んでいたら、永久に待たずに知らせる
            if not self._is_running():
                # 死ぬ直前に届いた分があれば先に返す
                try:
                    return q.get_nowait()
                except queue.Empty:
                    raise RuntimeError("受信スレッドが停止しています。start() で再開してください。")

            if deadline is None:
                wait_for = 0.5
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                wait_for = min(0.5, remaining)

            try:
                return q.get(timeout=wait_for)
            except queue.Empty:
                continue

    def wait(self, poll_interval: float = 0.5) -> None:
        """受信ワーカーが終了するまでブロックする。"""
        try:
            while self._is_running():
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            self._log_debug("MAIN", "KeyboardInterrupt を受信")
            raise

    def stop(self, join_timeout: float = 5.0) -> None:
        """受信と自動touchをすべて停止する。"""
        self._stop_event.set()
        for thread in (self._channel_thread, self._account_thread):
            if thread and thread.is_alive():
                thread.join(timeout=join_timeout)
        self._channel_thread = None
        self._account_thread = None
        self.stop_auto_touch()
        self._log_debug("MAIN", "停止しました。")

    def is_running(self) -> bool:
        return self._is_running()

    def close(self) -> None:
        self.stop()

    def __enter__(self) -> "ChannelWorks":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _is_running(self) -> bool:
        return any(t is not None and t.is_alive() for t in (self._channel_thread, self._account_thread))

    # --- WebSocket 接続 -----------------------------------------------
    def _open_channel_ws(self, session_token: str) -> _LoggingWS:
        raw = websocket.create_connection(
            WS_URL, timeout=self.timeout, origin="https://desk.channel.io", host="desk-ws.channel.io"
        )
        raw.settimeout(self.channel_recv_timeout)
        self._log_debug("WS", "Channel WebSocket 接続完了")
        ws = _LoggingWS(raw, label="CHANNEL-WS", owner=self)

        deadline = time.monotonic() + self.timeout
        connect_sent = False
        while time.monotonic() < deadline:
            try:
                packet = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if packet is None:
                raise RuntimeError("Channel WebSocket が切断されました。")
            if packet.startswith("0"):
                self._log_debug("SOCKET", "Channel Engine.IO open 受信")
                self._send_channel_connect(ws, session_token)
                connect_sent = True
                continue
            if packet == "2":
                self._send_pong(ws)
                continue
            if connect_sent and packet.startswith("40/desk/channel,"):
                self._log_debug("SOCKET", "Channel connect 応答 (sid) 受信")
                self._send_join_group(ws)
                return ws
            self._log_debug("SOCKET", f"Channel 接続待機中の受信: {packet!r}")
        raise TimeoutError("Channel connect 応答を受信できませんでした。")

    def _open_account_ws(self, session_token: str) -> _LoggingWS:
        raw = websocket.create_connection(
            ACCOUNT_WS_URL, timeout=self.timeout, origin="https://desk.channel.io", host="account-ws.channel.io"
        )
        raw.settimeout(self.account_recv_timeout)
        self._log_debug("WS", "Account WebSocket 接続完了")
        ws = _LoggingWS(raw, label="ACCOUNT-WS", owner=self)

        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                packet = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if packet is None:
                raise RuntimeError("Account WebSocket が切断されました。")
            if packet.startswith("0"):
                self._log_debug("SOCKET", "Account Engine.IO open 受信")
                self._send_account_connect(ws, session_token)
                return ws
            if packet == "2":
                self._send_pong(ws)
                continue
            self._log_debug("SOCKET", f"Account 接続待機中の受信: {packet!r}")
        raise TimeoutError("Account Engine.IO open パケットを受信できませんでした。")

    # --- Socket.IO パケット送信 ---------------------------------------
    @staticmethod
    def _compact(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    def _send_join_group(self, ws: _LoggingWS) -> None:
        """join_groups に入っている全部屋に join する。"""
        with self._lock:
            groups = list(self._join_groups)
        for gid in groups:
            path = f"/groups/{gid}"
            self._log_debug("SOCKET", f"join 送信: {path}")
            ws.send("42/desk/channel," + self._compact(["join", path]))

    def _send_channel_connect(self, ws: _LoggingWS, session_token: str) -> None:
        self._log_debug("SOCKET", f"channel connect 送信: channelId={self.channel_id}")
        ws.send("40/desk/channel," + self._compact({"jwt": session_token, "channelId": self.channel_id}))

    def _send_account_connect(self, ws: _LoggingWS, session_token: str) -> None:
        self._log_debug("SOCKET", "account connect 送信")
        ws.send("40/desk/account," + self._compact({"jwt": session_token}))

    def _send_refresh(self, ws: _LoggingWS, session_token: str) -> None:
        self._log_debug("SOCKET", "refresh 送信（account WS）")
        ws.send("42/desk/account,0" + self._compact(["refresh", {"jwt": session_token}]))

    def _send_channel_refresh(self, ws: _LoggingWS, session_token: str) -> None:
        self._log_debug("SOCKET", f"refresh 送信（channel WS, channelId={self.channel_id}）")
        ws.send(
            "42/desk/channel,3"
            + self._compact(["refresh", {"jwt": session_token, "channelId": self.channel_id}])
        )

    def _send_heartbeat(self, ws: _LoggingWS) -> None:
        self._log_debug("SOCKET", f"heartbeat 送信: manager_id={self.manager_id}")
        ws.send("42/desk/channel," + self._compact(["heartbeat", self.manager_id]))

    def _send_pong(self, ws: _LoggingWS) -> None:
        self._log_debug("SOCKET", "Engine.IO ping(2) 受信 -> pong(3) 送信")
        ws.send("3")

    # --- push 処理 ----------------------------------------------------
    def _extract_group_id(self, wrapper: Dict[str, Any]) -> Optional[str]:
        """push の中身から部屋IDを推定する。取れなければ None。"""
        entity = wrapper.get("entity") or {}
        for key in ("chatId", "groupId"):
            v = entity.get(key)
            if v is not None:
                return str(v).split("/")[-1]
        v = _find_first_key(wrapper, "groupId")
        if v is not None:
            return str(v).split("/")[-1]
        return None

    @staticmethod
    def _put_limited(q: "queue.Queue[str]", item: str) -> None:
        if q.full():  # 溜まりすぎたら古いものを捨てる
            try:
                q.get_nowait()
            except queue.Empty:
                pass
        q.put_nowait(item)

    def _handle_push(self, payload: list) -> None:
        if len(payload) < 2:
            return
        wrapper = payload[1]
        if not isinstance(wrapper, dict):
            return

        if self.log_level >= self.LOG_DEBUG:
            self._log_block_debug(
                "PUSH",
                f"type={wrapper.get('type')} event={wrapper.get('event')}",
                json.dumps(_format_json_with_timestamps(wrapper), ensure_ascii=False),
            )

        gid = self._extract_group_id(wrapper)
        with self._lock:
            joined = set(self._join_groups)

        # join していない部屋の push は無視 (on_push も呼ばない)
        if gid is not None and gid not in joined:
            self._log_debug("PUSH", f"対象外の部屋 {gid} の push を無視")
            return

        text = (wrapper.get("entity") or {}).get("plainText")
        if text:
            self.last_message = text
            self.last_wrapper = wrapper
            self._put_limited(self._inbox, text)  # 全部屋共通
            if gid is not None:                    # 部屋が特定できたときだけ部屋別へ
                with self._lock:
                    room_q = self._room_inboxes.setdefault(gid, queue.Queue(maxsize=1000))
                self._put_limited(room_q, text)
            if self.show_incoming:
                print(f'受信 [{gid or "?"}] : "{text}"', flush=True)

        if self.on_push is not None:
            try:
                self.on_push(wrapper)
            except Exception as exc:
                self._log_error("PUSH", f"on_push コールバックで例外: {exc!r}")

    # --- ワーカー -----------------------------------------------------
    def _channel_worker(self) -> None:
        current_token = self.get_tokens()["x-account"] or ""
        try:
            ws = self._open_channel_ws(current_token)
        except Exception as exc:
            self._log_error("CHANNEL", f"接続失敗: {exc!r}")
            return

        last_token_check = time.monotonic()
        ready_received = False
        heartbeat_phase = "waiting"  # "waiting" / "first_sent" / "second_sent"
        phase_started_at = 0.0

        try:
            while not self._stop_event.is_set():
                now = time.monotonic()

                # 起動後に join_group() された部屋を join
                while True:
                    try:
                        new_gid = self._pending_joins.get_nowait()
                    except queue.Empty:
                        break
                    self._log_debug("SOCKET", f"join 送信: /groups/{new_gid}")
                    ws.send("42/desk/channel," + self._compact(["join", f"/groups/{new_gid}"]))

                if ready_received:
                    if heartbeat_phase == "waiting":
                        if now - phase_started_at >= self.heartbeat_interval:
                            self._send_heartbeat(ws)
                            heartbeat_phase = "first_sent"
                            phase_started_at = now
                    elif heartbeat_phase in ("first_sent", "second_sent"):
                        if now - phase_started_at >= self.heartbeat_response_timeout:
                            self._log_debug(
                                "SOCKET", f"{heartbeat_phase} heartbeat 応答なし → 再送"
                            )
                            self._send_heartbeat(ws)
                            phase_started_at = now

                if now - last_token_check >= self.token_check_interval:
                    new_token = self.get_tokens()["x-account"]
                    if new_token and new_token != current_token:
                        self._log_debug("AUTH", "x-account 変更検出 → Channel refresh 送信")
                        self._send_channel_refresh(ws, new_token)
                        current_token = new_token
                    last_token_check = now

                try:
                    packet = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                if packet is None:
                    raise RuntimeError("Channel WebSocket が切断されました。")
                if packet == "2":
                    self._send_pong(ws)
                    continue
                if packet == "3":
                    self._log_debug("SOCKET", "Channel Engine.IO pong(3) 受信")
                    continue

                namespace, payload = _parse_socketio_event(packet)
                if namespace is None or payload is None:
                    continue

                if namespace == "/desk/channel" and payload:
                    event_name = payload[0]
                    self._log_debug("SOCKET", f"event={event_name}")

                    if event_name == "ready":
                        ready_received = True
                        heartbeat_phase = "waiting"
                        phase_started_at = time.monotonic()
                    elif event_name == "heartbeat":
                        if heartbeat_phase == "first_sent":
                            self._log_debug("SOCKET", "1回目の応答受信 → 2回目を送信")
                            self._send_heartbeat(ws)
                            heartbeat_phase = "second_sent"
                            phase_started_at = time.monotonic()
                        elif heartbeat_phase == "second_sent":
                            self._log_debug("SOCKET", "2回目の応答受信 → 次サイクル待機")
                            heartbeat_phase = "waiting"
                            phase_started_at = time.monotonic()
                    elif event_name in ("create", "push"):
                        if event_name == "create":
                            self._log_block_debug("CREATE", "raw packet", packet)
                        self._handle_push(payload)
                    elif event_name == "expired":
                        self._log_error("CHANNEL", "expired を受信 → 接続を終了します。")
                        break
        except Exception as exc:
            self._log_error("CHANNEL", f"エラーで終了: {exc!r}")
        finally:
            ws.close()

    def _account_worker(self) -> None:
        current_token = self.get_tokens()["x-account"] or ""
        try:
            ws = self._open_account_ws(current_token)
        except Exception as exc:
            self._log_error("ACCOUNT", f"接続失敗: {exc!r}")
            return

        last_token_check = time.monotonic()
        try:
            while not self._stop_event.is_set():
                now = time.monotonic()
                if now - last_token_check >= self.token_check_interval:
                    new_token = self.get_tokens()["x-account"]
                    if new_token and new_token != current_token:
                        self._log_debug("AUTH", "x-account 変更検出 → refresh 送信（account WS）")
                        self._send_refresh(ws, new_token)
                        current_token = new_token
                    last_token_check = now

                try:
                    packet = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                if packet is None:
                    raise RuntimeError("Account WebSocket が切断されました。")
                if packet == "2":
                    self._send_pong(ws)
                    continue
                if packet == "3":
                    self._log_debug("SOCKET", "Account Engine.IO pong(3) 受信")
                    continue

                namespace, payload = _parse_socketio_event(packet)
                if namespace is None or payload is None:
                    continue
                self._log_debug("SOCKET", f"Account event: ns={namespace}, payload={payload!r}")
        except Exception as exc:
            self._log_error("ACCOUNT", f"エラーで終了: {exc!r}")
        finally:
            ws.close()


# ---------------------------------------------------------------------------
# サンプル: "!ping" を受信したら "pong" を返す
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    BOT_MARK = "[bot] "  # 自分の返信に反応しないための目印

    cw = ChannelWorks(
        room_id="...",
        email="you@example.com",
        password="your-password",
        channel_slug="...",
        group_id="574628",
    )

    def on_push(wrapper):
        text = (wrapper.get("entity") or {}).get("plainText")
        if not text or text.startswith(BOT_MARK):
            return
        if text.strip() == "!ping":
            cw.send(BOT_MARK + "pong")

    cw.on_push = on_push
    cw.start()  # login + 自動touch + 受信 を一括開始
    try:
        cw.wait()
    except KeyboardInterrupt:
        pass
    finally:
        cw.stop()