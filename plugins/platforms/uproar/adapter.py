"""Uproar (uproar.chat) gateway adapter.

Perceives over the dial-out WebSocket at ``GET /api/bots/{id}/stream`` and acts
through the bot execute endpoint ``POST /api/bots/{id}/{token}``.  The agent
dials out, so no public URL or webhook receiver is required.  Uses aiohttp,
already a Hermes dependency.

Environment variables:
    UPROAR_BOT_ID                  Agent (bot) ID
    UPROAR_TOKEN                   Agent execute token
    UPROAR_URL                     Server URL (default https://uproar.chat)
    UPROAR_ALLOWED_USERS           Comma-separated user IDs
    UPROAR_HOME_CHANNEL            Channel ID for cron/notification delivery
    UPROAR_REQUIRE_MENTION         Require @mention in server channels
    UPROAR_FREE_RESPONSE_CHANNELS  Channel IDs exempt from the mention gate
    UPROAR_ALLOWED_CHANNELS        Whitelist of channel IDs
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.platforms.helpers import MessageDeduplicator

from agent.secret_scope import UnscopedSecretError as _UnscopedSecretError
from agent.secret_scope import get_secret as _scoped_get_secret


def _get_scoped_secret(name, default=None):
    """Scope-aware credential read with the default-profile startup fallback."""
    try:
        val = _scoped_get_secret(name, default)
    except _UnscopedSecretError:
        val = os.getenv(name)
    return val if val is not None else default


logger = logging.getLogger(__name__)

DEFAULT_URL = "https://uproar.chat"

MAX_MESSAGE_LENGTH = 2000

_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.2

_SUBSCRIBE_EVENTS = [
    "message_create",
    "message_edit",
    "message_delete",
    "reaction_add",
    "reaction_remove",
]

_UPLOAD_MAX_FILES = 4
_CHANNEL_LIST_TTL = 900.0

_EMBED_METADATA_KEY = "uproar_embed"
_EMBED_DESCRIPTION_LIMIT = 4000
_COLOR_ASK = 0x5865F2
_COLOR_WARN = 0xFAA61A

_RETRY_429_ATTEMPTS = 2
_RETRY_429_DEFAULT_DELAY = 5.0
_RETRY_429_MAX_DELAY = 60.0


def _truthy(value: Optional[str], default: bool = False) -> bool:
    if value is None or value == "":
        return default
    return str(value).strip().lower() not in {"false", "0", "no", "off"}


def _csv_set(raw: Any) -> set:
    if raw is None:
        return set()
    if isinstance(raw, (list, tuple, set)):
        return {str(v).strip() for v in raw if str(v).strip()}
    return {v.strip() for v in str(raw).split(",") if v.strip()}


def _redact(token: str) -> str:
    """Mask a token for log output."""
    if not token:
        return ""
    return f"{token[:4]}…{token[-2:]}" if len(token) > 8 else "…"


def check_uproar_requirements() -> bool:
    """Return True if the adapter's runtime dependency is available."""
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        logger.warning("Uproar: aiohttp not installed")
        return False


def _resolve_bot_id(config: Optional[PlatformConfig] = None) -> str:
    extra = (getattr(config, "extra", {}) or {}) if config is not None else {}
    return str(extra.get("bot_id") or os.getenv("UPROAR_BOT_ID", "")).strip()


def _resolve_url(config: Optional[PlatformConfig] = None) -> str:
    extra = (getattr(config, "extra", {}) or {}) if config is not None else {}
    return str(
        extra.get("url") or os.getenv("UPROAR_URL", "") or DEFAULT_URL
    ).strip().rstrip("/")


def validate_uproar_config(config: PlatformConfig) -> bool:
    """Return True when Uproar has enough config to connect."""
    token = (
        getattr(config, "token", None) or _get_scoped_secret("UPROAR_TOKEN", "")
    ).strip()
    if not token:
        logger.debug("Uproar: UPROAR_TOKEN not set")
        return False
    if not _resolve_bot_id(config):
        logger.warning("Uproar: UPROAR_BOT_ID not set")
        return False
    return True


class UproarAdapter(BasePlatformAdapter):
    """Gateway adapter for Uproar."""

    splits_long_messages = True

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    _ACK_EMOJI = "\U0001f440"
    _OK_EMOJI = "\u2705"
    _FAIL_EMOJI = "\u274c"

    def __init__(self, config: PlatformConfig, **kwargs):
        super().__init__(config=config, platform=Platform("uproar"))

        self._base_url: str = _resolve_url(config)
        self._bot_id: str = _resolve_bot_id(config)
        self._token: str = (
            config.token or _get_scoped_secret("UPROAR_TOKEN", "")
        ).strip()

        self._bot_user_id: str = ""

        self._session: Any = None
        self._ws: Any = None
        self._ws_task: Optional[asyncio.Task] = None
        self._closing = False
        self._cursor: str = ""
        self._cursor_seq: str = ""

        self._channel_cache: Dict[str, Dict[str, Any]] = {}
        self._dm_target_cache: Dict[str, str] = {}
        self._last_error: str = ""
        self._channels_cache: List[Dict[str, Any]] = []
        self._channels_cached_at: float = 0.0
        self._last_overflow_preview: Dict[str, str] = {}
        self._dedup = MessageDeduplicator()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @property
    def _execute_url(self) -> str:
        return f"{self._base_url}/api/bots/{self._bot_id}/{self._token}"

    def _read_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def _execute(self, action: str, **fields: Any) -> Optional[Dict[str, Any]]:
        """POST one action to the execute endpoint, waiting out a 429.

        Both channel slowmode and the per-bot write budget answer 429 with a
        ``retry_after``. Treating that as a hard failure silently drops the
        rest of a chunked reply.
        """
        import aiohttp

        payload = {"action": action}
        payload.update({k: v for k, v in fields.items() if v is not None})
        self._last_error = ""

        for attempt in range(_RETRY_429_ATTEMPTS + 1):
            try:
                async with self._session.post(
                    self._execute_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    body = await resp.text()
                    if resp.status == 429 and attempt < _RETRY_429_ATTEMPTS:
                        delay = self._retry_after_seconds(resp, body)
                        logger.info(
                            "Uproar %s rate limited, retrying in %.0fs", action, delay
                        )
                        await asyncio.sleep(delay)
                        continue
                    if resp.status >= 400:
                        self._last_error = body[:200]
                        logger.warning(
                            "Uproar %s → HTTP %s: %s", action, resp.status, body[:200]
                        )
                        return None
                    try:
                        return json.loads(body)
                    except (json.JSONDecodeError, TypeError):
                        return {}
            except Exception as exc:
                logger.warning("Uproar %s failed: %s", action, exc)
                return None
        return None

    @staticmethod
    def _retry_after_seconds(resp: Any, body: str) -> float:
        """Seconds to wait after a 429, from the body or the header."""
        try:
            value = float(json.loads(body).get("retry_after"))
        except Exception:
            try:
                value = float(resp.headers.get("Retry-After", ""))
            except (TypeError, ValueError):
                value = _RETRY_429_DEFAULT_DELAY
        if value <= 0:
            value = _RETRY_429_DEFAULT_DELAY
        return min(value, _RETRY_429_MAX_DELAY)

    async def _read(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """GET a Read API path below /api/bots/{id}/."""
        import aiohttp

        url = f"{self._base_url}/api/bots/{self._bot_id}/{path}"
        try:
            async with self._session.get(
                url,
                headers=self._read_headers(),
                params=params or {},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status >= 400:
                    logger.debug("Uproar read %s → HTTP %s", path, resp.status)
                    return None
                return await resp.json()
        except Exception as exc:
            logger.warning("Uproar read %s failed: %s", path, exc)
            return None

    async def _upload(self, chat_id: str, files: List[str]) -> List[Dict[str, Any]]:
        """Upload local files and return their attachment objects."""
        import aiohttp

        out: List[Dict[str, Any]] = []
        for chunk_start in range(0, len(files), _UPLOAD_MAX_FILES):
            chunk = files[chunk_start:chunk_start + _UPLOAD_MAX_FILES]
            form = aiohttp.FormData()
            added = False
            for path in chunk:
                if not path or not os.path.exists(path):
                    continue
                with open(path, "rb") as fh:
                    form.add_field(
                        "files", fh.read(), filename=os.path.basename(path)
                    )
                added = True
            if not added:
                continue
            try:
                async with self._session.post(
                    f"{self._base_url}/api/bots/{self._bot_id}/attachments",
                    params={"channel_id": chat_id},
                    data=form,
                    headers=self._read_headers(),
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.warning(
                            "Uproar upload → HTTP %s: %s", resp.status, body[:200]
                        )
                        continue
                    data = await resp.json()
                    if isinstance(data, list):
                        out.extend(data)
            except Exception as exc:
                logger.warning("Uproar upload failed: %s", exc)
        return out

    # ------------------------------------------------------------------
    # Required overrides
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Authenticate, then start the dial-out WebSocket listener."""
        import aiohttp

        if not self._bot_id or not self._token:
            logger.error("Uproar: UPROAR_BOT_ID or UPROAR_TOKEN not configured")
            return False

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        self._closing = False

        probe = await self._read("events", {"since": ""})
        if probe is None:
            logger.error(
                "Uproar: failed to authenticate as %s (token %s) on %s — check "
                "UPROAR_BOT_ID and UPROAR_TOKEN",
                self._bot_id,
                _redact(self._token),
                self._base_url,
            )
            await self._session.close()
            return False

        self._set_cursor(str(probe.get("cursor") or ""))
        logger.info(
            "Uproar: authenticated as %s on %s", self._bot_id, self._base_url
        )

        self._ws_task = asyncio.create_task(self._ws_loop())
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        """Stop the listener and close the socket and HTTP session."""
        self._closing = True

        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except (asyncio.CancelledError, Exception):
                pass

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._session is not None and not self._session.closed:
            await self._session.close()

        logger.info("Uproar: disconnected")

    async def _open_dm_with(self, user_id: str) -> Optional[str]:
        """Open (or fetch) the DM channel with a user, cached.

        Resolution is lazy: a send is tried against the id as given, and only a
        rejected channel sends us here. Probing every send would double the
        request count and spend the read budget on the common path, where the
        id is already a channel.
        """
        cached = self._dm_target_cache.get(user_id)
        if cached:
            return cached
        opened = await self._execute("open_dm", target_user_id=user_id)
        if not opened or "id" not in opened:
            return None
        resolved = str(opened["id"])
        self._dm_target_cache[user_id] = resolved
        return resolved

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message, chunked to the 2000-character content limit."""
        if not content:
            return SendResult(success=True)

        chat_id = self._dm_target_cache.get(chat_id, chat_id)

        embed = self._embed_from_metadata(content, metadata)
        if embed is not None:
            data = await self._execute(
                "send", channel_id=chat_id, embeds=[embed], reply_to=reply_to
            )
            if not data or "id" not in data:
                return SendResult(success=False, error="Failed to send message")
            return SendResult(success=True, message_id=data["id"])

        chunks = self.truncate_message(self.format_message(content), MAX_MESSAGE_LENGTH)

        last_id = None
        for index, chunk in enumerate(chunks):
            data = await self._execute(
                "send",
                channel_id=chat_id,
                content=chunk,
                reply_to=reply_to if index == 0 else None,
            )
            if (
                not data
                and index == 0
                and "invalid channel" in self._last_error.lower()
            ):
                opened = await self._open_dm_with(chat_id)
                if opened:
                    chat_id = opened
                    data = await self._execute(
                        "send",
                        channel_id=chat_id,
                        content=chunk,
                        reply_to=reply_to,
                    )
            if not data or "id" not in data:
                return SendResult(success=False, error="Failed to send message")
            last_id = data["id"]

        return SendResult(success=True, message_id=last_id)

    @staticmethod
    def _embed_from_metadata(
        content: str, metadata: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Build an embed for a prompt that asked to be rendered as one.

        Uproar has no buttons, so an interactive prompt still resolves by the
        user typing a reply. The embed only makes it visually distinct from
        ordinary chat, which is what the button-capable adapters get from
        their own embeds.
        """
        spec = (metadata or {}).get(_EMBED_METADATA_KEY)
        if not isinstance(spec, dict):
            return None
        body = content or ""
        if len(body) > _EMBED_DESCRIPTION_LIMIT:
            body = body[: _EMBED_DESCRIPTION_LIMIT - 1] + "\u2026"
        embed: Dict[str, Any] = {"description": body}
        title = spec.get("title")
        if title:
            embed["title"] = str(title)[:256]
        color = spec.get("color")
        if isinstance(color, int) and 0 <= color <= 16777215:
            embed["color"] = color
        return embed

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Render the clarify prompt as an embed, keeping the text fallback.

        The base builds the numbered list and registers the text intercept, so
        this only asks for the embed treatment and delegates the rest.
        """
        return await super().send_clarify(
            chat_id,
            question,
            choices,
            clarify_id,
            session_key,
            metadata=self._with_embed(metadata, "Hermes needs your input", _COLOR_ASK),
        )

    @staticmethod
    def _with_embed(
        metadata: Optional[Dict[str, Any]], title: str, color: int
    ) -> Dict[str, Any]:
        merged = dict(metadata or {})
        merged[_EMBED_METADATA_KEY] = {"title": title, "color": color}
        return merged

    async def list_channels(self) -> List[Dict[str, Any]]:
        """Enumerate visible channels so targets can be named, not just id'd.

        Without this the channel directory is empty for Uproar and an agent can
        only send to a raw channel id, while Discord and Slack accept a name.

        The gateway rebuilds the directory every five minutes, and this costs
        one read per server plus one, so a bot in many servers would spend most
        of its 60/min read budget on bursts that rediscover the same channels.
        The result is cached, and a failed read returns the last good list
        rather than an empty one, which would drop every name target until the
        next success.
        """
        now = time.monotonic()
        if self._channels_cache and now - self._channels_cached_at < _CHANNEL_LIST_TTL:
            return self._channels_cache

        servers = await self._read("servers")
        if not isinstance(servers, list):
            return self._channels_cache

        out: List[Dict[str, Any]] = []
        for server in servers:
            if not isinstance(server, dict) or not server.get("id"):
                continue
            channels = await self._read(
                "channels", {"server_id": str(server["id"])}
            )
            if not isinstance(channels, list):
                continue
            for channel in channels:
                if not isinstance(channel, dict) or not channel.get("id"):
                    continue
                if channel.get("is_archived"):
                    continue
                self._channel_cache.setdefault(str(channel["id"]), channel)
                out.append(
                    {
                        "id": str(channel["id"]),
                        "name": str(channel.get("name") or channel["id"]),
                        "type": self._chat_type(channel),
                        "guild": str(server.get("name") or server["id"]),
                    }
                )

        if not out and self._channels_cache:
            return self._channels_cache
        self._channels_cache = out
        self._channels_cached_at = now
        return out

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return the channel's name and type, cached per channel."""
        cached = self._channel_cache.get(chat_id)
        if cached is None:
            data = await self._read(f"channels/{chat_id}")
            if not isinstance(data, dict):
                return {"name": chat_id, "type": "channel", "chat_id": chat_id}
            cached = data
            self._channel_cache[chat_id] = data

        return {
            "name": cached.get("name") or chat_id,
            "type": self._chat_type(cached),
            "chat_id": chat_id,
        }

    @staticmethod
    def _chat_type(channel: Dict[str, Any]) -> str:
        """Classify a channel as a server channel, a group DM, or a 1:1 DM.

        ``is_group`` is authoritative: it tracks whether the DM has an owner,
        so a group whose members left is still a group. A member count would
        call that a 1:1 and silently drop the mention gate.
        """
        if not (channel.get("is_dm") or not channel.get("server_id")):
            return "channel"
        return "group" if channel.get("is_group") else "dm"

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Emit the typing indicator over the socket, falling back to REST.

        The socket frame costs no rate-limit budget, which matters because the
        gateway refreshes typing every two seconds.
        """
        ws = self._ws
        if ws is not None and not ws.closed:
            try:
                await ws.send_json(
                    {"type": "typing", "data": {"channel_id": chat_id}}
                )
                return
            except Exception as exc:
                logger.debug("Uproar: typing frame failed, using REST: %s", exc)
        await self._execute("typing", channel_id=chat_id)

    # ------------------------------------------------------------------
    # Optional overrides
    # ------------------------------------------------------------------

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        """Edit one of the agent's own messages.

        Past the 2000-character cap a streaming preview truncates, but the
        finalized answer splits across follow-up messages so nothing is lost.
        """
        formatted = self.format_message(content)
        preview_key = f"{chat_id}:{message_id}"

        if len(formatted) > MAX_MESSAGE_LENGTH:
            chunks = self.truncate_message(formatted, MAX_MESSAGE_LENGTH)
            if finalize:
                self._last_overflow_preview.pop(preview_key, None)
                return await self._edit_overflow_split(chat_id, message_id, chunks)
            formatted = chunks[0]
            if self._last_overflow_preview.get(preview_key) == formatted:
                return SendResult(success=True, message_id=message_id)
            data = await self._execute(
                "edit", message_id=message_id, content=formatted
            )
            if not data or "id" not in data:
                return SendResult(success=False, error="Failed to edit message")
            self._last_overflow_preview[preview_key] = formatted
            return SendResult(success=True, message_id=data["id"])

        self._last_overflow_preview.pop(preview_key, None)
        data = await self._execute("edit", message_id=message_id, content=formatted)
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to edit message")
        return SendResult(success=True, message_id=data["id"])

    async def _edit_overflow_split(
        self, chat_id: str, message_id: str, chunks: List[str]
    ) -> SendResult:
        """Edit the first chunk in place, then post the remainder."""
        data = await self._execute("edit", message_id=message_id, content=chunks[0])
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to edit message")

        last_id = data["id"]
        for chunk in chunks[1:]:
            follow = await self._execute("send", channel_id=chat_id, content=chunk)
            if not follow or "id" not in follow:
                return SendResult(
                    success=False, error="Failed to send overflow continuation"
                )
            last_id = follow["id"]
        return SendResult(success=True, message_id=last_id)

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete one of the agent's own messages."""
        data = await self._execute("delete", message_id=message_id)
        return bool(data)

    async def _add_reaction(
        self, chat_id: str, message_id: str, emoji: str
    ) -> None:
        await self._execute("react", message_id=message_id, emoji=emoji)

    async def _remove_reaction(
        self, chat_id: str, message_id: str, emoji: Optional[str] = None
    ) -> None:
        """Remove a reaction, defaulting to the in-progress marker.

        The shared ack flow calls this with two arguments, but Uproar rejects
        an unreact with no emoji, so fall back to the one this adapter adds.
        """
        await self._execute(
            "unreact", message_id=message_id, emoji=emoji or self._ACK_EMOJI
        )

    def _reactions_enabled(self) -> bool:
        return os.getenv("UPROAR_REACTIONS", "true").lower() not in {
            "false",
            "0",
            "no",
        }

    async def on_processing_start(self, event: MessageEvent) -> None:
        """Mark the message as being worked on."""
        if not self._reactions_enabled():
            return
        chat_id = getattr(event.source, "chat_id", None)
        if chat_id and event.message_id:
            await self._add_reaction(chat_id, event.message_id, self._ACK_EMOJI)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Download a remote image and re-upload it as an attachment."""
        import aiohttp

        try:
            async with self._session.get(
                image_url, timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                if resp.status >= 400:
                    return SendResult(
                        success=False, error=f"Failed to fetch image ({resp.status})"
                    )
                blob = await resp.read()
        except Exception as exc:
            return SendResult(success=False, error=f"Failed to fetch image: {exc}")

        from gateway.platforms.base import cache_image_from_bytes

        suffix = Path(image_url.split("?")[0]).suffix or ".png"
        local = cache_image_from_bytes(blob, suffix)
        return await self._send_local_files(chat_id, [local], caption, reply_to)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        return await self._send_local_files(chat_id, [image_path], caption, reply_to)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        return await self._send_local_files(chat_id, [file_path], caption, reply_to)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        return await self._send_local_files(chat_id, [audio_path], caption, reply_to)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        return await self._send_local_files(chat_id, [video_path], caption, reply_to)

    async def send_animation(
        self,
        chat_id: str,
        animation_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a GIF. Uproar keeps the animation, so upload it as-is."""
        return await self.send_image(
            chat_id, animation_url, caption, reply_to, metadata
        )

    async def send_multiple_images(
        self,
        chat_id: str,
        images: List[Tuple[str, str]],
        metadata: Optional[Dict[str, Any]] = None,
        human_delay: float = 0.0,
    ) -> None:
        """Bundle a batch into as few messages as the upload cap allows.

        The default sends one message per image. Uproar takes an attachments
        array, so a set of images arrives as one message instead of five.
        """
        import aiohttp

        from gateway.platforms.base import cache_image_from_bytes

        local: List[str] = []
        for image_url, _alt in images:
            path = image_url
            if path.startswith("file://"):
                from urllib.parse import unquote, urlparse

                path = unquote(urlparse(path).path)
            if os.path.exists(path):
                local.append(path)
                continue
            try:
                async with self._session.get(
                    image_url, timeout=aiohttp.ClientTimeout(total=60)
                ) as resp:
                    if resp.status >= 400:
                        logger.warning(
                            "Uproar: image fetch %s → HTTP %s",
                            image_url[:80],
                            resp.status,
                        )
                        continue
                    blob = await resp.read()
            except Exception as exc:
                logger.warning("Uproar: image fetch failed: %s", exc)
                continue
            suffix = Path(image_url.split("?")[0]).suffix or ".png"
            local.append(cache_image_from_bytes(blob, suffix))

        if not local:
            return

        for chunk_start in range(0, len(local), _UPLOAD_MAX_FILES):
            if human_delay > 0 and chunk_start:
                await asyncio.sleep(human_delay)
            await self._send_local_files(
                chat_id, local[chunk_start:chunk_start + _UPLOAD_MAX_FILES], None, None
            )

    async def _send_local_files(
        self,
        chat_id: str,
        paths: List[str],
        caption: Optional[str],
        reply_to: Optional[str],
    ) -> SendResult:
        attachments = await self._upload(chat_id, paths)
        if not attachments:
            return SendResult(success=False, error="Attachment upload failed")

        content = self.format_message(caption or "")
        if len(content) > MAX_MESSAGE_LENGTH:
            content = self.truncate_message(content, MAX_MESSAGE_LENGTH)[0]

        data = await self._execute(
            "send",
            channel_id=chat_id,
            content=content or None,
            attachments=[
                {"url": a.get("url"), "thumb_url": a.get("thumb_url")}
                for a in attachments
                if a.get("url")
            ],
            reply_to=reply_to,
        )
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to send attachment")
        return SendResult(success=True, message_id=data["id"])

    def format_message(self, content: str) -> str:
        """Pass markdown through; Uproar renders it client-side."""
        return content

    # ------------------------------------------------------------------
    # WebSocket dial-out
    # ------------------------------------------------------------------

    async def _ws_loop(self) -> None:
        """Hold the socket open, reconnecting with backoff and catching up."""
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._ws_connect_and_listen()
                delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._closing:
                    return
                import aiohttp

                if (
                    isinstance(exc, aiohttp.WSServerHandshakeError)
                    and exc.status in {401, 403}
                ):
                    logger.error(
                        "Uproar WS auth rejected (HTTP %d) — stopping reconnect",
                        exc.status,
                    )
                    self._set_fatal_error(
                        "uproar_auth_error",
                        f"Uproar WebSocket authentication rejected (HTTP {exc.status}). "
                        "The agent token is invalid or regenerated, or the agent is "
                        "paused — check UPROAR_TOKEN and the agent in Settings › Bots.",
                        retryable=False,
                    )
                    await self._notify_fatal_error()
                    return
                logger.warning(
                    "Uproar WS error: %s — reconnecting in %.0fs", exc, delay
                )

            if self._closing:
                return

            await asyncio.sleep(delay + delay * _RECONNECT_JITTER * random.random())
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _ws_connect_and_listen(self) -> None:
        """One socket session: connect, subscribe, catch up, then stream."""
        ws_url = re.sub(r"^http", "ws", self._base_url) + (
            f"/api/bots/{self._bot_id}/stream"
        )
        logger.info("Uproar: connecting to %s", ws_url)

        self._ws = await self._session.ws_connect(
            ws_url, headers=self._read_headers(), heartbeat=30.0
        )

        try:
            await self._ws.send_json(
                {"type": "subscribe", "data": {"events": _SUBSCRIBE_EVENTS}}
            )
            await self._catch_up()

            async for raw in self._ws:
                if self._closing:
                    return
                if raw.type in {raw.type.TEXT, raw.type.BINARY}:
                    try:
                        frame = json.loads(raw.data)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    await self._handle_frame(frame)
                elif raw.type in {
                    raw.type.ERROR,
                    raw.type.CLOSE,
                    raw.type.CLOSING,
                    raw.type.CLOSED,
                }:
                    logger.info("Uproar: WebSocket closed (%s)", raw.type)
                    break
        finally:
            self._ws = None

    def _set_cursor(self, cursor: str) -> None:
        """Store an events cursor, remembering its ``|seq`` suffix."""
        if not cursor:
            return
        self._cursor = cursor
        head, sep, tail = cursor.rpartition("|")
        self._cursor_seq = tail if sep else ""

    def _advance_cursor_to(self, created_at: str) -> None:
        """Move the cursor to a live message, keeping the outbox sequence.

        A bare timestamp is accepted by the server, but it resets the outbox
        sequence to the current maximum, which silently skips non-message
        events that arrived while the socket was down.
        """
        if not created_at:
            return
        self._cursor = (
            f"{created_at}|{self._cursor_seq}" if self._cursor_seq else created_at
        )

    async def _catch_up(self) -> None:
        """Replay anything missed while the socket was down."""
        if not self._cursor:
            return
        data = await self._read("events", {"since": self._cursor})
        if not isinstance(data, dict):
            return
        events = data.get("events") or []
        if events:
            logger.info("Uproar: replaying %d missed event(s)", len(events))
        for event in events:
            await self._handle_frame(event)
        self._set_cursor(str(data.get("cursor") or ""))

    async def _handle_frame(self, frame: Dict[str, Any]) -> None:
        """Route one socket or catch-up frame."""
        ftype = frame.get("type")
        data = frame.get("data")

        if ftype == "ready":
            if isinstance(data, dict):
                self._bot_user_id = str(data.get("user_id") or "")
                logger.info(
                    "Uproar: stream ready (bot_id=%s user_id=%s)",
                    data.get("bot_id"),
                    self._bot_user_id or "unknown",
                )
            return

        if not isinstance(data, dict):
            return

        if ftype in ("reaction_add", "reaction_remove"):
            await self._handle_reaction(ftype, data)
            return

        if ftype in ("message_edit", "message_delete"):
            await self._handle_message_change(ftype, data)
            return

        if ftype != "message_create":
            return

        await self._handle_message_create(data)

    @staticmethod
    def _platform_events_subscribed() -> bool:
        try:
            from hermes_cli.lifecycle import has_hook

            return has_hook("gateway_platform_event")
        except Exception:
            return False

    async def _handle_message_change(self, ftype: str, data: Dict[str, Any]) -> None:
        """Normalize an edit or delete onto the gateway platform-event boundary."""
        handler = getattr(self, "_platform_event_handler", None)
        if handler is None or not self._platform_events_subscribed():
            return

        author = str(data.get("user_id") or "")
        if self._bot_user_id and author == self._bot_user_id:
            return

        chat_id = str(data.get("channel_id") or "")
        message_id = str(data.get("id") or data.get("message_id") or "")
        if not chat_id or not message_id:
            return

        deleted = ftype == "message_delete"
        text = data.get("content")
        payload = {
            "chat_id": chat_id[:128],
            "message_id": message_id[:128],
            "thread_id": None,
            "text": text[:8192] if isinstance(text, str) else None,
        }
        if not deleted:
            payload["edited_at"] = str(data.get("edited_at") or "")[:64] or None

        event = {
            "platform": "uproar",
            "event_type": "message_deleted" if deleted else "message_edited",
            "payload": payload,
        }
        source = self.build_source(
            chat_id=chat_id,
            chat_type="channel" if data.get("server_id") else "dm",
            user_id=author or None,
            user_name=data.get("display_name") or data.get("username"),
            scope_id=str(data.get("server_id") or "") or None,
            message_id=message_id,
        )
        try:
            await handler(event, source)
        except Exception:
            logger.debug("Uproar: platform event dispatch error", exc_info=True)

    async def _handle_reaction(self, ftype: str, data: Dict[str, Any]) -> None:
        """Forward a reaction to the gateway hook surface.

        Mirrors the Slack adapter, so hook consumers see reaction:added and
        reaction:removed on Uproar too. The agent's own reactions are skipped,
        otherwise the acknowledgement reactions it adds would echo back.
        """
        handler = getattr(self, "_reaction_handler", None)
        if handler is None:
            return

        user_id = str(data.get("user_id") or "")
        if self._bot_user_id and user_id == self._bot_user_id:
            return

        action = "added" if ftype == "reaction_add" else "removed"
        message = data.get("message")
        item_user_id = (
            message.get("user_id") if isinstance(message, dict) else None
        )
        try:
            await handler(
                {
                    "platform": "uproar",
                    "event_name": f"reaction:{action}",
                    "reaction": data.get("emoji"),
                    "user_id": user_id,
                    "item_user_id": item_user_id,
                    "item_type": "message",
                    "channel_id": data.get("channel_id"),
                    "message_ts": data.get("message_id"),
                    "event_ts": None,
                    "raw_event": data,
                }
            )
        except Exception:
            logger.debug("Uproar: reaction hook forwarding failed", exc_info=True)

    async def _handle_message_create(self, msg: Dict[str, Any]) -> None:
        """Turn a message_create payload into a gateway MessageEvent."""
        message_id = str(msg.get("id") or "")
        if not message_id or self._dedup.is_duplicate(message_id):
            return

        sender_id = str(msg.get("user_id") or "")
        if self._bot_user_id and sender_id == self._bot_user_id:
            return
        if msg.get("type"):
            return

        self._advance_cursor_to(str(msg.get("created_at") or ""))

        channel_id = str(msg.get("channel_id") or "")
        server_id = str(msg.get("server_id") or "")
        is_dm = not server_id
        text = msg.get("content") or ""

        is_command = text.lstrip().startswith("/")

        info = await self.get_chat_info(channel_id)
        chat_type = info.get("type") or ("dm" if is_dm else "channel")

        if chat_type != "dm":
            allowed = _csv_set(
                (self.config.extra or {}).get("allowed_channels")
                if self.config.extra
                else None
            ) or _csv_set(os.getenv("UPROAR_ALLOWED_CHANNELS"))
            if allowed and channel_id not in allowed:
                logger.debug("Uproar: ignoring non-allowed channel %s", channel_id)
                return

            require_mention = _truthy(os.getenv("UPROAR_REQUIRE_MENTION"), True)
            free_channels = _csv_set(os.getenv("UPROAR_FREE_RESPONSE_CHANNELS"))
            if require_mention and channel_id not in free_channels and not is_command:
                if not self._is_mentioned(msg):
                    logger.debug(
                        "Uproar: skipping unmentioned message in %s", channel_id
                    )
                    return
            text = self._strip_mention(text, msg)

        msg_type = MessageType.TEXT
        if is_command:
            text = text.lstrip()
            msg_type = MessageType.COMMAND

        media_urls, media_types = await self._collect_attachments(msg)
        if media_types and msg_type == MessageType.TEXT:
            if any(m.startswith("image/") for m in media_types):
                msg_type = MessageType.PHOTO
            elif any(m.startswith("audio/") for m in media_types):
                msg_type = MessageType.VOICE
            else:
                msg_type = MessageType.DOCUMENT

        sender_name = (
            msg.get("nickname")
            or msg.get("display_name")
            or msg.get("username")
            or sender_id
        )

        source = self.build_source(
            chat_id=channel_id,
            chat_name=info.get("name"),
            chat_type=chat_type,
            user_id=sender_id,
            user_name=sender_name,
            scope_id=server_id or None,
            message_id=message_id,
            is_bot=bool(msg.get("is_bot")),
        )

        from gateway.platforms.base import resolve_channel_prompt

        channel_prompt = resolve_channel_prompt(self.config.extra, channel_id, None)

        reply = msg.get("reply_msg")
        reply_kwargs: Dict[str, Any] = {}
        if isinstance(reply, dict):
            reply_author = reply.get("user_id") or ""
            reply_kwargs = {
                "reply_to_message_id": str(
                    msg.get("reply_to") or reply.get("id") or ""
                )
                or None,
                "reply_to_text": self._strip_mention(reply.get("content") or "", msg)
                or None,
                "reply_to_author_id": str(reply_author) or None,
                "reply_to_author_name": (
                    reply.get("nickname")
                    or reply.get("display_name")
                    or reply.get("username")
                ),
                "reply_to_is_own_message": bool(
                    self._bot_user_id and reply_author == self._bot_user_id
                ),
            }

        await self.handle_message(
            MessageEvent(
                text=text,
                message_type=msg_type,
                source=source,
                raw_message=msg,
                message_id=message_id,
                media_urls=media_urls or None,
                media_types=media_types or None,
                channel_prompt=channel_prompt,
                **reply_kwargs,
            )
        )

    def _is_mentioned(self, msg: Dict[str, Any]) -> bool:
        """True when the agent is mentioned, @everyone'd, or replied to."""
        if msg.get("mentions_everyone"):
            return True
        for mention in msg.get("mentions") or []:
            if isinstance(mention, dict) and mention.get("user_id") == self._bot_user_id:
                return True
        reply = msg.get("reply_msg")
        if isinstance(reply, dict) and reply.get("user_id") == self._bot_user_id:
            return True
        if self._bot_user_id and f"@user[[{self._bot_user_id}]]" in (
            msg.get("content") or ""
        ):
            return True
        return False

    def _strip_mention(self, text: str, msg: Dict[str, Any]) -> str:
        """Drop the agent's own mention and make the remaining ones readable.

        Uproar puts mentions on the wire as ``@user[[<uuid>]]`` and the client
        resolves them to pills. Left alone the model sees the raw markup.
        """
        names = {}
        for mention in msg.get("mentions") or []:
            if not isinstance(mention, dict):
                continue
            uid = mention.get("user_id")
            if uid:
                names[uid] = mention.get("display_name") or mention.get("username")

        def _replace(match):
            uid = match.group(1)
            if uid == self._bot_user_id:
                return ""
            handle = names.get(uid)
            return f"@{handle}" if handle else "@user"

        text = re.sub(r"@user\[\[([a-f0-9-]+)\]\]", _replace, text)
        text = re.sub(r"@role\[\[[a-f0-9-]+\]\]", "@role", text)
        return re.sub(r"[ \t]{2,}", " ", text).strip()

    async def _collect_attachments(self, msg: Dict[str, Any]) -> tuple:
        """Download the message's attachments into the local media cache."""
        import aiohttp

        raw = msg.get("attachments")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                raw = None
        if not isinstance(raw, list) or not raw:
            return [], []

        from gateway.platforms.base import (
            cache_audio_from_bytes,
            cache_document_from_bytes,
            cache_image_from_bytes,
        )

        urls: List[str] = []
        types: List[str] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            url = item.get("url")
            if not url:
                continue
            if url.startswith("/"):
                url = f"{self._base_url}{url}"
            mime = item.get("content_type") or "application/octet-stream"
            name = item.get("filename") or Path(url.split("?")[0]).name
            try:
                async with self._session.get(
                    url, timeout=aiohttp.ClientTimeout(total=60)
                ) as resp:
                    if resp.status >= 400:
                        logger.warning(
                            "Uproar: attachment fetch %s → HTTP %s", name, resp.status
                        )
                        continue
                    blob = await resp.read()
            except Exception as exc:
                logger.warning("Uproar: attachment fetch %s failed: %s", name, exc)
                continue

            suffix = Path(name).suffix
            if mime.startswith("image/"):
                local = cache_image_from_bytes(blob, suffix or ".png")
            elif mime.startswith("audio/"):
                local = cache_audio_from_bytes(blob, suffix or ".ogg")
            else:
                local = cache_document_from_bytes(blob, name)
            urls.append(local)
            types.append(mime)

        return urls, types


# ---------------------------------------------------------------------------
# Out-of-process cron delivery
# ---------------------------------------------------------------------------


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Send through the execute endpoint without a live gateway adapter.

    Used by ``tools/send_message_tool`` when cron runs in its own process.
    ``thread_id`` and ``force_document`` are accepted for signature parity;
    Uproar has no threads and stores every upload as a generic attachment.
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    base_url = _resolve_url(pconfig)
    bot_id = _resolve_bot_id(pconfig)
    token = (
        getattr(pconfig, "token", None) or _get_scoped_secret("UPROAR_TOKEN", "")
    ).strip()
    if not bot_id or not token:
        return {
            "error": "Uproar standalone send: UPROAR_BOT_ID and UPROAR_TOKEN must both be set"
        }

    execute_url = f"{base_url}/api/bots/{bot_id}/{token}"
    media_files = media_files or []

    from gateway.platforms.base import proxy_kwargs_for_aiohttp, resolve_proxy_url

    proxy = resolve_proxy_url(platform_env_var="UPROAR_PROXY")
    sess_kw, req_kw = proxy_kwargs_for_aiohttp(proxy)

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60), **sess_kw
        ) as session:
            attachments: List[Dict[str, Any]] = []
            for media in media_files:
                path = media.get("path") if isinstance(media, dict) else media
                if not path or not os.path.exists(path):
                    continue
                form = aiohttp.FormData()
                with open(path, "rb") as fh:
                    form.add_field(
                        "files", fh.read(), filename=os.path.basename(path)
                    )
                async with session.post(
                    f"{base_url}/api/bots/{bot_id}/attachments",
                    params={"channel_id": chat_id},
                    data=form,
                    headers={"Authorization": f"Bearer {token}"},
                    **req_kw,
                ) as upload_resp:
                    if upload_resp.status >= 400:
                        body = await upload_resp.text()
                        return {
                            "error": f"Uproar upload failed ({upload_resp.status}): {body[:400]}"
                        }
                    data = await upload_resp.json()
                    if isinstance(data, list):
                        attachments.extend(
                            {"url": a.get("url"), "thumb_url": a.get("thumb_url")}
                            for a in data
                            if isinstance(a, dict) and a.get("url")
                        )

            payload: Dict[str, Any] = {"action": "send", "channel_id": chat_id}
            if message:
                payload["content"] = message[:MAX_MESSAGE_LENGTH]
            if attachments:
                payload["attachments"] = attachments

            async with session.post(execute_url, json=payload, **req_kw) as resp:
                body = await resp.text()
                if resp.status >= 400:
                    return {"error": f"Uproar send failed ({resp.status}): {body[:400]}"}
                try:
                    sent = json.loads(body)
                except (json.JSONDecodeError, TypeError):
                    sent = {}
                return {"success": True, "message_id": sent.get("id")}
    except Exception as exc:
        return {"error": f"Uproar standalone send failed: {exc}"}


# ---------------------------------------------------------------------------
# Registration hooks
# ---------------------------------------------------------------------------


def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra from env so status reflects env-only setups."""
    if not (os.getenv("UPROAR_TOKEN") and os.getenv("UPROAR_BOT_ID")):
        return None
    extra: Dict[str, Any] = {
        "bot_id": os.getenv("UPROAR_BOT_ID", "").strip(),
        "url": _resolve_url(None),
    }
    home = os.getenv("UPROAR_HOME_CHANNEL", "").strip()
    if home:
        extra["home_channel"] = {"chat_id": home}
    return extra


def _apply_yaml_config(yaml_cfg: dict, uproar_cfg: dict) -> Optional[dict]:
    """Translate config.yaml ``uproar:`` keys into env vars and extras."""
    mapping = {
        "bot_id": "UPROAR_BOT_ID",
        "url": "UPROAR_URL",
        "home_channel": "UPROAR_HOME_CHANNEL",
        "require_mention": "UPROAR_REQUIRE_MENTION",
        "allowed_users": "UPROAR_ALLOWED_USERS",
        "allow_all_users": "UPROAR_ALLOW_ALL_USERS",
    }
    for key, env in mapping.items():
        if key in uproar_cfg and not os.getenv(env):
            os.environ[env] = str(uproar_cfg[key]).lower() if isinstance(
                uproar_cfg[key], bool
            ) else str(uproar_cfg[key])

    for key, env in (
        ("free_response_channels", "UPROAR_FREE_RESPONSE_CHANNELS"),
        ("allowed_channels", "UPROAR_ALLOWED_CHANNELS"),
    ):
        value = uproar_cfg.get(key)
        if value is not None and not os.getenv(env):
            if isinstance(value, list):
                value = ",".join(str(v) for v in value)
            os.environ[env] = str(value)

    extra: Dict[str, Any] = {}
    if os.getenv("UPROAR_BOT_ID"):
        extra["bot_id"] = os.getenv("UPROAR_BOT_ID", "").strip()
    extra["url"] = _resolve_url(None)
    return extra or None


def _is_connected(config) -> bool:
    """Uproar counts as configured when both the ID and token are present."""
    import hermes_cli.gateway as gateway_mod

    return bool(
        (gateway_mod.get_env_value("UPROAR_TOKEN") or "").strip()
        and (gateway_mod.get_env_value("UPROAR_BOT_ID") or "").strip()
    )


def interactive_setup() -> None:
    """Guide the user through connecting an Uproar agent."""
    from hermes_cli.config import get_env_value, remove_env_value, save_env_value
    from hermes_cli.cli_output import (
        print_header,
        print_info,
        print_success,
        prompt,
        prompt_yes_no,
    )

    print_header("Uproar")
    if get_env_value("UPROAR_TOKEN"):
        print_info("Uproar: already configured")
        if not prompt_yes_no("Reconfigure Uproar?", False):
            return

    print_info("Create an agent at uproar.chat:")
    print_info("   1. User Settings -> Bots & Agents -> Create / Manage")
    print_info("   2. Name it, then click Create agent. Copy the token now, it shows once.")
    print_info("   3. Under Your agents, the grey line beneath the name is the bot ID.")
    print_info("   4. Step 2 of the wizard adds it to a server. It joins at once if you")
    print_info("      manage agents there.")
    print_info("   Server Settings can also create an agent, but it never shows the bot ID,")
    print_info("   and you need both. Use User Settings.")
    print()

    url = prompt(f"Uproar server URL (leave empty for {DEFAULT_URL})").strip()
    save_env_value("UPROAR_URL", (url or DEFAULT_URL).rstrip("/"))

    bot_id = prompt("Bot ID").strip()
    if not bot_id:
        print_info("Bot ID is required; nothing saved.")
        return
    token = prompt("Bot token", password=True)
    if not token:
        print_info("Bot token is required; nothing saved.")
        return

    bot_id, token = _split_pasted_credentials(bot_id, token)
    save_env_value("UPROAR_BOT_ID", bot_id)
    save_env_value("UPROAR_TOKEN", token)
    print_success("Uproar credentials saved")

    print()
    print_info("Security: restrict who can use your agent")
    print_info("   Your Uproar user ID is on your profile, or right-click yourself -> Copy ID.")
    print()
    allowed = prompt("Allowed user IDs (comma-separated, empty for open access)")
    if allowed:
        save_env_value("UPROAR_ALLOWED_USERS", allowed.replace(" ", ""))
        print_success("Uproar allowlist configured")
    else:
        print_info("No allowlist set. The gateway denies unknown senders unless you also")
        print_info("set GATEWAY_ALLOW_ALL_USERS=true, so the agent may ignore everyone.")

    print()
    print_info("Home channel: where Hermes delivers cron results and notifications.")
    print_info("   Right-click a channel -> Copy ID, or set it later by typing /sethome")
    print_info("   in the channel you want.")
    home = prompt("Home channel ID (empty to set later with /sethome)").strip()
    if home:
        save_env_value("UPROAR_HOME_CHANNEL", home)
    elif remove_env_value("UPROAR_HOME_CHANNEL"):
        print_info("Home channel cleared.")

    print()
    print_info("In a server channel the agent answers when mentioned. In a 1:1 DM it")
    print_info("answers everything. Set UPROAR_REQUIRE_MENTION=false to drop the mention")
    print_info("requirement, or UPROAR_FREE_RESPONSE_CHANNELS for named channels.")
    print_info("   Open config in your editor:  hermes config edit")


def _split_pasted_credentials(bot_id: str, token: str) -> tuple:
    """Accept a whole create-response URL pasted into either field.

    The create call answers with https://<host>/api/bots/<id>/<token>, so that
    is what people have on their clipboard.
    """
    for value in (bot_id, token):
        if "/api/bots/" not in value:
            continue
        tail = value.split("/api/bots/", 1)[1].strip("/").split("/")
        if len(tail) >= 2 and tail[0] and tail[1]:
            return tail[0], tail[1]
    return bot_id, token


def _build_adapter(config):
    return UproarAdapter(config)


def register(ctx) -> None:
    """Plugin entry point, called by the Hermes plugin system."""
    ctx.register_platform(
        name="uproar",
        label="Uproar",
        adapter_factory=_build_adapter,
        check_fn=check_uproar_requirements,
        validate_config=validate_uproar_config,
        is_connected=_is_connected,
        required_env=["UPROAR_BOT_ID", "UPROAR_TOKEN"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        allowed_users_env="UPROAR_ALLOWED_USERS",
        allow_all_env="UPROAR_ALLOW_ALL_USERS",
        cron_deliver_env_var="UPROAR_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="📣",
        allow_update_command=True,
        platform_hint=(
            "You are on a chat platform, Uproar, in a server channel or a DM. "
            "Markdown renders natively: **bold**, *italic*, ***both***, "
            "~~strike~~, ||spoiler||, `inline code`, ```code blocks```, "
            "# headers (h1-h4), > blockquotes, bullet and numbered lists, "
            "tables, and [links](url). Note that __text__ renders as UNDERLINE "
            "here, not bold — use **text** for bold. Messages are limited to "
            "2000 characters (longer replies are split automatically). "
            "You can send media files natively: to deliver a file to the user, "
            "include MEDIA:/absolute/path/to/file in your response. Images "
            "(.png, .jpg, .webp) are uploaded as image attachments with "
            "thumbnails, audio as file attachments, and other files arrive as "
            "downloadable attachments. Image URLs in markdown format "
            "![alt](url) render inline without being uploaded."
        ),
    )
