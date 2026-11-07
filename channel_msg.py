"""
ChannelWorks メッセージ受信ライブラリ

使い方:
    from channel_auth import ChannelWorksClient
    from channel_messenger import ChannelMessenger

    client = ChannelWorksClient(
        room_id="...",
        email="you@example.com",
        password="your-password",
        log_level=ChannelWorksClient.LOG_ERROR,
    )
    client.login()
    client.start_auto_touch()

    def on_push(wrapper):
        entity = wrapper.get("entity") or {}
        print("受信:", entity.get("plainText"))

    messenger = ChannelMessenger(
        client=client,
        channel_slug="...",                # channel_id を自動取得したい場合
        # manager_id="...", channel_id="...",  # 直接指定してもよい
        group_path="/groups/グループ番号",
        log_level=ChannelMessenger.LOG_ERROR,
        on_push=on_push,
    )
    messenger.start()
    try:
        messenger.wait()
    except KeyboardInterrupt:
        pass
    finally:
        messenger.stop()
        client.stop_auto_touch()

log_level:
    0 = 何も出力しない
    1 = エラー・警告のみ (デフォルト)
    2 = デバッグログも出力 (送受信パケット等をすべて出力)
"""

import json
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, Optional, Tuple

import requests
import websocket

from channel_auth import ChannelWorksClient


# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
DEFAULT_GROUP_PATH = "/groups/574628"

CHANNEL_API_URL_TEMPLATE = "https://api.channel.works/desk/channels/{room_id}"
CHANNEL_LOOKUP_URL_TEMPLATE = "https://api.channel.works/desk/channels/{slug}"

WS_URL = (
    "wss://desk-ws.channel.io/socket.io/"
    "?platform=web&EIO=4&transport=websocket"
)
ACCOUNT_WS_URL = (
    "wss://account-ws.channel.io/socket.io/"
    "?platform=web&EIO=4&transport=websocket"
)

JST = timezone(timedelta(hours=9))


# ---------------------------------------------------------------------------
# モジュールレベルユーティリティ（ログに依存しない純粋関数）
# ---------------------------------------------------------------------------
def _timestamp_to_text(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return str(value)
    if value < 100_000_000_000:
        return str(value)
    dt_utc = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    dt_jst = dt_utc.astimezone(JST)
    return (
        f"{dt_jst:%Y-%m-%d %H:%M:%S.%f} JST "
        f"(UTC {dt_utc:%Y-%m-%d %H:%M:%S.%f})"
    )


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


# ---------------------------------------------------------------------------
# WebSocket ラッパ（送受信をデバッグログするだけの薄いラッパ）
# ---------------------------------------------------------------------------
class _LoggingWS:
    def __init__(self, ws: websocket.WebSocket, label: str, messenger: "ChannelMessenger") -> None:
        self._ws = ws
        self._label = label
        self._messenger = messenger
        self._send_count = 0
        self._recv_count = 0

    def send(self, payload: Any) -> None:
        self._send_count += 1
        text = (
            payload.decode("utf-8", errors="replace")
            if isinstance(payload, (bytes, bytearray))
            else payload
        )
        self._messenger._log_block_debug(
            f"{self._label}-SEND",
            f"#{self._send_count} ({len(text)} bytes)",
            text,
        )
        self._ws.send(payload)

    def recv(self) -> Optional[str]:
        packet = self._ws.recv()
        self._recv_count += 1
        if packet is None:
            self._messenger._log_debug(
                f"{self._label}-RECV",
                f"#{self._recv_count} (None / 切断)",
            )
            return None
        text = (
            packet.decode("utf-8", errors="replace")
            if isinstance(packet, (bytes, bytearray))
            else packet
        )
        self._messenger._log_block_debug(
            f"{self._label}-RECV",
            f"#{self._recv_count} ({len(text)} chars)",
            text,
        )
        return text

    def settimeout(self, t: float) -> None:
        self._ws.settimeout(t)

    def close(self) -> None:
        try:
            self._ws.close()
        finally:
            self._messenger._log_debug(self._label, "WebSocket をクローズしました。")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ws, name)


# ---------------------------------------------------------------------------
# 本体
# ---------------------------------------------------------------------------
class ChannelMessenger:
    # ログレベル
    LOG_NONE = 0
    LOG_ERROR = 1
    LOG_DEBUG = 2

    def __init__(
        self,
        client: ChannelWorksClient,
        channel_slug: Optional[str] = None,
        manager_id: Optional[str] = None,
        channel_id: Optional[str] = None,
        group_path: str = DEFAULT_GROUP_PATH,
        log_level: int = LOG_ERROR,
        heartbeat_interval: float = 30.0,
        heartbeat_response_timeout: float = 5.0,
        token_check_interval: float = 1.0,
        request_timeout: float = 30.0,
        channel_recv_timeout: float = 3.0,
        account_recv_timeout: float = 1.0,
        on_push: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        if client is None:
            raise ValueError("client (ChannelWorksClient) は必須です。")

        self.client = client
        self.channel_slug = channel_slug
        self.manager_id = manager_id
        self.channel_id = channel_id
        self.group_path = group_path
        self.log_level = log_level

        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_response_timeout = heartbeat_response_timeout
        self.token_check_interval = token_check_interval
        self.request_timeout = request_timeout
        self.channel_recv_timeout = channel_recv_timeout
        self.account_recv_timeout = account_recv_timeout

        self.on_push = on_push

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._channel_thread: Optional[threading.Thread] = None
        self._account_thread: Optional[threading.Thread] = None
        # x-account の最新値。channel/account 両ワーカーがこの dict を参照する。
        self._token_holder: Dict[str, Optional[str]] = {"token": None}

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

    # ------------------------------------------------------------------
    # 公開 API
    # ------------------------------------------------------------------
    def start(self) -> None:
        """channel / account の各 WebSocket をバックグラウンドで起動する。"""
        with self._lock:
            if self._is_running():
                self._log_debug("MAIN", "ChannelMessenger は既に起動中です。")
                return

            tokens = self.client.get_tokens() or {}
            required = ("ch-session-1", "ch-veil-id", "x-account", "x-account-refresh")
            missing = [k for k in required if not tokens.get(k)]
            if missing:
                raise RuntimeError(
                    "client から必要なトークンが取得できません。先に login() を実行してください: "
                    + ", ".join(missing)
                )

            if self.manager_id is None or self.channel_id is None:
                self._resolve_ids(tokens)

            self._token_holder["token"] = tokens["x-account"]
            self._stop_event.clear()

            self._channel_thread = threading.Thread(
                target=self._channel_worker,
                args=(tokens["x-account"], self.channel_id, self.manager_id),
                name="channel-ws",
                daemon=True,
            )
            self._account_thread = threading.Thread(
                target=self._account_worker,
                name="account-ws",
                daemon=True,
            )
            self._channel_thread.start()
            self._account_thread.start()
            self._log_debug(
                "MAIN",
                f"起動: manager_id={self.manager_id} channel_id={self.channel_id}",
            )

    def wait(self, poll_interval: float = 0.5) -> None:
        """両ワーカーが終了するまでブロックする。"""
        try:
            while self._is_running():
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            self._log_debug("MAIN", "KeyboardInterrupt を受信")
            raise

    def stop(self, join_timeout: float = 5.0) -> None:
        """ワーカーを停止する。"""
        self._stop_event.set()
        for thread in (self._channel_thread, self._account_thread):
            if thread and thread.is_alive():
                thread.join(timeout=join_timeout)
        self._channel_thread = None
        self._account_thread = None
        self._log_debug("MAIN", "ChannelMessenger を停止しました。")

    def is_running(self) -> bool:
        return self._is_running()

    def close(self) -> None:
        self.stop()

    def __enter__(self) -> "ChannelMessenger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------
    # 内部: 状態
    # ------------------------------------------------------------------
    def _is_running(self) -> bool:
        return any(
            t is not None and t.is_alive()
            for t in (self._channel_thread, self._account_thread)
        )

    # ------------------------------------------------------------------
    # 内部: HTTP / ID 解決
    # ------------------------------------------------------------------
    def _resolve_ids(self, tokens: Dict[str, str]) -> None:
        room_id = getattr(self.client, "room_id", None)
        if not room_id:
            raise RuntimeError("client.room_id が取得できません。")

        session = requests.Session()
        session.cookies.update(
            {
                "ch-session-1": tokens.get("ch-session-1") or "",
                "ch-veil-id": tokens.get("ch-veil-id") or "",
                "x-account": tokens.get("x-account") or "",
                "x-account-refresh": tokens.get("x-account-refresh") or "",
            }
        )

        room_url = CHANNEL_API_URL_TEMPLATE.format(room_id=room_id)

        if self.manager_id is None:
            room_json = self._http_get_json(session, room_url, tokens)
            manager_id = _get_nested(room_json, "manager", "id")
            if manager_id is None:
                manager = _find_first_key(room_json, "manager")
                if isinstance(manager, dict):
                    manager_id = manager.get("id")
            if manager_id is None:
                raise RuntimeError(
                    f"{room_url} の JSON から manager.id を取得できませんでした。"
                )
            self.manager_id = str(manager_id)

        if self.channel_id is None:
            if not self.channel_slug:
                raise RuntimeError(
                    "channel_id を自動解決するには channel_slug を指定してください。"
                )
            lookup_url = CHANNEL_LOOKUP_URL_TEMPLATE.format(slug=self.channel_slug)
            channel_json = self._http_get_json(session, lookup_url, tokens)
            channel_id = _get_nested(channel_json, "channel", "id")
            if channel_id is None:
                channel_id = _find_first_key(channel_json, "id")
            if channel_id is None:
                raise RuntimeError(
                    f"{lookup_url} の JSON から channel.id を取得できませんでした。"
                )
            self.channel_id = str(channel_id)

    def _http_get_json(
        self, session: requests.Session, url: str, tokens: Dict[str, str]
    ) -> Any:
        self._log_debug("HTTP", f"GET {url}")
        response = session.get(
            url,
            headers={
                "Accept": "application/json",
                "Accept-Language": "ja",
                "Referer": "https://channel.works/",
                "Origin": "https://channel.works",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/152.0.0.0 Safari/537.36"
                ),
                "x-account": tokens["x-account"],
            },
            timeout=self.request_timeout,
        )
        self._log_debug("HTTP", f"-> status={response.status_code} ({url})")
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"JSON を取得できませんでした: {url}\n"
                f"status={response.status_code}\n"
                f"body={response.text[:500]}"
            ) from exc
        self._log_block_debug(
            "HTTP-BODY",
            url,
            json.dumps(data, ensure_ascii=False),
        )
        return data

    # ------------------------------------------------------------------
    # 内部: WebSocket 接続
    # ------------------------------------------------------------------
    def _open_channel_ws(self, session_token: str, channel_id: str) -> _LoggingWS:
        raw = websocket.create_connection(
            WS_URL,
            timeout=self.request_timeout,
            origin="https://desk.channel.io",
            host="desk-ws.channel.io",
        )
        raw.settimeout(self.channel_recv_timeout)
        self._log_debug("WS", "Channel WebSocket 接続完了")
        ws = _LoggingWS(raw, label="CHANNEL-WS", messenger=self)

        deadline = time.monotonic() + self.request_timeout
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
                self._send_channel_connect(ws, session_token, channel_id)
                connect_sent = True
                continue
            if packet == "2":
                self._send_pong(ws)
                continue
            if connect_sent and packet.startswith("40/desk/channel,"):
                self._log_debug("SOCKET", "Channel connect 応答 (sid) 受信")
                self._send_join_group(ws, self.group_path)
                return ws
            self._log_debug("SOCKET", f"Channel 接続待機中の受信: {packet!r}")
        raise TimeoutError("Channel connect 応答を受信できませんでした。")

    def _open_account_ws(self, session_token: str) -> _LoggingWS:
        raw = websocket.create_connection(
            ACCOUNT_WS_URL,
            timeout=self.request_timeout,
            origin="https://desk.channel.io",
            host="account-ws.channel.io",
        )
        raw.settimeout(self.account_recv_timeout)
        self._log_debug("WS", "Account WebSocket 接続完了")
        ws = _LoggingWS(raw, label="ACCOUNT-WS", messenger=self)

        deadline = time.monotonic() + self.request_timeout
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

    # ------------------------------------------------------------------
    # 内部: Socket.IO パケット送信
    # ------------------------------------------------------------------
    def _send_join_group(self, ws: _LoggingWS, group_path: str) -> None:
        message = '42/desk/channel,' + json.dumps(
            ["join", group_path],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self._log_debug("SOCKET", f"join 送信: {group_path}")
        ws.send(message)

    def _send_channel_connect(
        self, ws: _LoggingWS, session_token: str, channel_id: str
    ) -> None:
        message = '40/desk/channel,' + json.dumps(
            {"jwt": session_token, "channelId": channel_id},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self._log_debug("SOCKET", f"channel connect 送信: channelId={channel_id}")
        ws.send(message)

    def _send_account_connect(self, ws: _LoggingWS, session_token: str) -> None:
        message = '40/desk/account,' + json.dumps(
            {"jwt": session_token},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self._log_debug("SOCKET", "account connect 送信")
        ws.send(message)

    def _send_refresh(self, ws: _LoggingWS, session_token: str) -> None:
        message = '42/desk/account,0' + json.dumps(
            ["refresh", {"jwt": session_token}],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self._log_debug("SOCKET", "refresh 送信（account WS）")
        ws.send(message)

    def _send_channel_refresh(
        self, ws: _LoggingWS, session_token: str, channel_id: str
    ) -> None:
        message = '42/desk/channel,3' + json.dumps(
            ["refresh", {"jwt": session_token, "channelId": channel_id}],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self._log_debug(
            "SOCKET", f"refresh 送信（channel WS, channelId={channel_id}）"
        )
        ws.send(message)

    def _send_heartbeat(self, ws: _LoggingWS, manager_id: str) -> None:
        message = '42/desk/channel,' + json.dumps(
            ["heartbeat", manager_id],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self._log_debug("SOCKET", f"heartbeat 送信: manager_id={manager_id}")
        ws.send(message)

    def _send_pong(self, ws: _LoggingWS) -> None:
        self._log_debug("SOCKET", "Engine.IO ping(2) 受信 -> pong(3) 送信")
        ws.send("3")

    # ------------------------------------------------------------------
    # 内部: push 処理
    # ------------------------------------------------------------------
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
                json.dumps(
                    _format_json_with_timestamps(wrapper),
                    ensure_ascii=False,
                ),
            )

        if self.on_push is not None:
            try:
                self.on_push(wrapper)
            except Exception as exc:
                self._log_error("PUSH", f"on_push コールバックで例外: {exc!r}")

    # ------------------------------------------------------------------
    # 内部: channel ワーカー
    # ------------------------------------------------------------------
    def _channel_worker(
        self, session_token: str, channel_id: str, manager_id: str
    ) -> None:
        try:
            ws = self._open_channel_ws(session_token, channel_id)
        except Exception as exc:
            self._log_error("CHANNEL", f"接続失敗: {exc!r}")
            return

        current_token = session_token
        last_token_check = time.monotonic()

        ready_received = False
        heartbeat_phase = "waiting"  # "waiting" / "first_sent" / "second_sent"
        phase_started_at = 0.0

        try:
            while not self._stop_event.is_set():
                now = time.monotonic()

                if ready_received:
                    if heartbeat_phase == "waiting":
                        if now - phase_started_at >= self.heartbeat_interval:
                            self._send_heartbeat(ws, manager_id)
                            heartbeat_phase = "first_sent"
                            phase_started_at = now
                    elif heartbeat_phase == "first_sent":
                        if now - phase_started_at >= self.heartbeat_response_timeout:
                            self._log_debug(
                                "SOCKET",
                                f"1回目 heartbeat 応答なし ({self.heartbeat_response_timeout}s) → 再送",
                            )
                            self._send_heartbeat(ws, manager_id)
                            phase_started_at = now
                    elif heartbeat_phase == "second_sent":
                        if now - phase_started_at >= self.heartbeat_response_timeout:
                            self._log_debug(
                                "SOCKET",
                                f"2回目 heartbeat 応答なし ({self.heartbeat_response_timeout}s) → 再送",
                            )
                            self._send_heartbeat(ws, manager_id)
                            phase_started_at = now

                if now - last_token_check >= self.token_check_interval:
                    new_token = self._token_holder.get("token")
                    if new_token and new_token != current_token:
                        self._log_debug(
                            "AUTH", "x-account 変更検出 → Channel refresh 送信"
                        )
                        self._send_channel_refresh(ws, new_token, channel_id)
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
                            self._log_debug(
                                "SOCKET", "1回目の応答受信 → 2回目を送信"
                            )
                            self._send_heartbeat(ws, manager_id)
                            heartbeat_phase = "second_sent"
                            phase_started_at = time.monotonic()
                        elif heartbeat_phase == "second_sent":
                            self._log_debug(
                                "SOCKET", "2回目の応答受信 → 次サイクル待機"
                            )
                            heartbeat_phase = "waiting"
                            phase_started_at = time.monotonic()
                    elif event_name == "create":
                        self._log_block_debug("CREATE", "raw packet", packet)
                        # ★ create も on_push に流す
                        self._handle_push(payload)
                    elif event_name == "push":
                        self._handle_push(payload)
                    elif event_name == "expired":
                        self._log_error(
                            "CHANNEL", "expired を受信 → 接続を終了します。"
                        )
                        break
        except Exception as exc:
            self._log_error("CHANNEL", f"エラーで終了: {exc!r}")
        finally:
            ws.close()

    # ------------------------------------------------------------------
    # 内部: account ワーカー
    # ------------------------------------------------------------------
    def _account_worker(self) -> None:
        try:
            ws = self._open_account_ws(self._token_holder["token"] or "")
        except Exception as exc:
            self._log_error("ACCOUNT", f"接続失敗: {exc!r}")
            return

        last_token_check = time.monotonic()
        try:
            while not self._stop_event.is_set():
                now = time.monotonic()
                if now - last_token_check >= self.token_check_interval:
                    current_tokens = self.client.get_tokens() or {}
                    current_session_token = current_tokens.get("x-account")
                    if (
                        current_session_token
                        and current_session_token != self._token_holder["token"]
                    ):
                        self._log_debug(
                            "AUTH", "x-account 変更検出 → refresh 送信（account WS）"
                        )
                        self._send_refresh(ws, current_session_token)
                        self._token_holder["token"] = current_session_token
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
                self._log_debug(
                    "SOCKET", f"Account event: ns={namespace}, payload={payload!r}"
                )
        except Exception as exc:
            self._log_error("ACCOUNT", f"エラーで終了: {exc!r}")
        finally:
            ws.close()

