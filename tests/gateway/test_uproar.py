"""Tests for the Uproar platform plugin."""
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from gateway.config import PlatformConfig

from plugins.platforms.uproar import adapter as uproar


BOT_USER = "b7bf8803-51ba-4a5b-9225-9b167e1cfa20"
ADA_USER = "dad37541-99e7-4314-a3fc-ce758ef2527e"


def _config(**extra):
    cfg = PlatformConfig()
    cfg.enabled = True
    cfg.token = "tok_secret_value"
    cfg.extra = {"bot_id": "bot_1", "url": "https://uproar.chat", **extra}
    return cfg


def _adapter(**extra):
    a = uproar.UproarAdapter(_config(**extra))
    a._bot_user_id = BOT_USER
    return a


def _msg(**over):
    base = {
        "id": "msg_1",
        "channel_id": "ch_1",
        "server_id": "srv_1",
        "user_id": "usr_human",
        "content": "hello there",
        "username": "ada",
        "display_name": "Ada",
        "created_at": "2026-08-22T00:00:00Z",
    }
    base.update(over)
    return base


async def _capture(adapter, msg, chat_type="channel"):
    """Run one message_create and return the MessageEvent, or None."""
    seen = []

    async def _handle(event):
        seen.append(event)

    adapter.handle_message = _handle
    if not isinstance(adapter.get_chat_info, AsyncMock):
        adapter.get_chat_info = AsyncMock(
            return_value={"name": "general", "type": chat_type, "chat_id": "ch_1"}
        )
    await adapter._handle_message_create(msg)
    return seen[0] if seen else None


class TestHelpers:
    def test_truthy_defaults_and_falsey_words(self):
        assert uproar._truthy(None, True) is True
        assert uproar._truthy("", True) is True
        assert uproar._truthy("false") is False
        assert uproar._truthy("0") is False
        assert uproar._truthy("no") is False
        assert uproar._truthy("yes") is True

    def test_csv_set_handles_string_list_and_none(self):
        assert uproar._csv_set("a, b ,c") == {"a", "b", "c"}
        assert uproar._csv_set(["a", "b"]) == {"a", "b"}
        assert uproar._csv_set(None) == set()

    def test_redact_never_leaks_a_whole_token(self):
        out = uproar._redact("tok_secret_value")
        assert "secret" not in out
        assert out.startswith("tok_")

    def test_resolve_url_strips_trailing_slash_and_defaults(self, monkeypatch):
        monkeypatch.delenv("UPROAR_URL", raising=False)
        assert uproar._resolve_url(None) == uproar.DEFAULT_URL
        cfg = _config(url="https://chat.example.com/")
        assert uproar._resolve_url(cfg) == "https://chat.example.com"


class TestValidateConfig:
    def test_requires_token(self, monkeypatch):
        monkeypatch.delenv("UPROAR_TOKEN", raising=False)
        cfg = _config()
        cfg.token = ""
        assert uproar.validate_uproar_config(cfg) is False

    def test_requires_bot_id(self, monkeypatch):
        monkeypatch.delenv("UPROAR_BOT_ID", raising=False)
        cfg = _config()
        cfg.extra.pop("bot_id")
        assert uproar.validate_uproar_config(cfg) is False

    def test_accepts_complete_config(self):
        assert uproar.validate_uproar_config(_config()) is True


class TestFormatting:
    def test_image_markdown_survives(self):
        """Uproar renders ![alt](url) inline, so it must not be rewritten."""
        a = _adapter()
        text = "look ![cat](https://x.test/c.png) here"
        assert a.format_message(text) == text

    def test_max_length_matches_the_platform_limit(self):
        assert uproar.MAX_MESSAGE_LENGTH == 2000
        assert _adapter().MAX_MESSAGE_LENGTH == 2000


@pytest.mark.asyncio
class TestInboundGating:
    async def test_dm_needs_no_mention(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        a = _adapter()
        event = await _capture(a, _msg(server_id="", content="hi"), chat_type="dm")
        assert event is not None
        assert event.source.chat_type == "dm"

    async def test_channel_without_mention_is_ignored(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        assert await _capture(a, _msg()) is None

    async def test_channel_with_mention_passes_and_is_stripped(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(
            a,
            _msg(
                content=f"@user[[{BOT_USER}]] what is up",
                mentions=[{"user_id": BOT_USER, "username": "hermes"}],
            ),
        )
        assert event is not None
        assert event.text == "what is up"
        assert event.source.chat_type == "channel"

    async def test_reply_to_the_agent_counts_as_a_mention(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(a, _msg(reply_msg={"user_id": BOT_USER}))
        assert event is not None

    async def test_free_response_channel_skips_the_mention_gate(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.setenv("UPROAR_FREE_RESPONSE_CHANNELS", "ch_1")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        assert await _capture(a, _msg()) is not None

    async def test_allowed_channels_whitelist_wins_over_a_mention(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.setenv("UPROAR_ALLOWED_CHANNELS", "ch_other")
        a = _adapter()
        event = await _capture(
            a, _msg(mentions=[{"user_id": BOT_USER, "username": "hermes"}])
        )
        assert event is None


@pytest.mark.asyncio
class TestLoopAndDuplicateGuards:
    async def test_own_message_is_dropped(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        assert await _capture(a, _msg(user_id=BOT_USER)) is None

    async def test_system_message_is_dropped(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        assert await _capture(a, _msg(type="pin")) is None

    async def test_duplicate_id_is_processed_once(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        assert await _capture(a, _msg()) is not None
        assert await _capture(a, _msg()) is None

    async def test_cursor_keeps_the_outbox_sequence(self, monkeypatch):
        """A bare timestamp resets seq to the server max and skips outbox events."""
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        a._set_cursor("2026-08-22T20:43:00.740613294Z|1363")
        assert a._cursor_seq == "1363"
        await _capture(a, _msg(created_at="2026-08-22T21:00:00Z"))
        assert a._cursor == "2026-08-22T21:00:00Z|1363"

    async def test_cursor_advances_for_reconnect_catch_up(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        await _capture(a, _msg(created_at="2026-08-22T12:00:00Z"))
        assert a._cursor == "2026-08-22T12:00:00Z"


@pytest.mark.asyncio
class TestReadyFrame:
    async def test_ready_frame_records_the_agent_user_id(self):
        a = uproar.UproarAdapter(_config())
        await a._handle_frame(
            {"type": "ready", "data": {"bot_id": "bot_1", "user_id": "usr_me"}}
        )
        assert a._bot_user_id == "usr_me"

    async def test_unrelated_frame_is_ignored(self):
        a = _adapter()
        await a._handle_frame({"type": "reaction_add", "data": {"id": "x"}})


@pytest.mark.asyncio
class TestTyping:
    async def test_typing_prefers_the_socket_frame(self):
        a = _adapter()
        ws = AsyncMock()
        ws.closed = False
        a._ws = ws
        a._execute = AsyncMock()
        await a.send_typing("ch_1")
        ws.send_json.assert_awaited_once_with(
            {"type": "typing", "data": {"channel_id": "ch_1"}}
        )
        a._execute.assert_not_awaited()

    async def test_typing_falls_back_to_rest_without_a_socket(self):
        a = _adapter()
        a._ws = None
        a._execute = AsyncMock(return_value={"ok": True})
        await a.send_typing("ch_1")
        a._execute.assert_awaited_once_with("typing", channel_id="ch_1")


@pytest.mark.asyncio
class TestSend:
    async def test_send_splits_at_the_content_limit(self):
        a = _adapter()
        a._execute = AsyncMock(return_value={"id": "m1"})
        result = await a.send("ch_1", "x" * 4500)
        assert result.success is True
        assert a._execute.await_count > 1

    async def test_reply_to_is_only_set_on_the_first_chunk(self):
        a = _adapter()
        a._execute = AsyncMock(return_value={"id": "m1"})
        await a.send("ch_1", "y" * 4500, reply_to="msg_9")
        calls = a._execute.await_args_list
        assert calls[0].kwargs["reply_to"] == "msg_9"
        assert calls[1].kwargs["reply_to"] is None

    async def test_send_failure_surfaces_as_an_error(self):
        a = _adapter()
        a._execute = AsyncMock(return_value=None)
        result = await a.send("ch_1", "hi")
        assert result.success is False


@pytest.mark.asyncio
class TestStandaloneSend:
    async def test_missing_credentials_returns_an_error(self, monkeypatch):
        monkeypatch.delenv("UPROAR_BOT_ID", raising=False)
        monkeypatch.delenv("UPROAR_TOKEN", raising=False)
        cfg = PlatformConfig()
        cfg.token = ""
        cfg.extra = {}
        out = await uproar._standalone_send(cfg, "ch_1", "hi")
        assert "error" in out


class TestRegistration:
    def test_register_wires_the_expected_hooks(self):
        captured = {}

        class Ctx:
            def register_platform(self, **kwargs):
                captured.update(kwargs)

        uproar.register(Ctx())

        assert captured["name"] == "uproar"
        assert captured["label"] == "Uproar"
        assert captured["max_message_length"] == 2000
        assert captured["allowed_users_env"] == "UPROAR_ALLOWED_USERS"
        assert captured["allow_all_env"] == "UPROAR_ALLOW_ALL_USERS"
        assert captured["cron_deliver_env_var"] == "UPROAR_HOME_CHANNEL"
        assert captured["standalone_sender_fn"] is uproar._standalone_send
        assert captured["required_env"] == ["UPROAR_BOT_ID", "UPROAR_TOKEN"]

    def test_platform_hint_teaches_the_media_convention(self):
        """Every shipped adapter tells the agent how to attach a file."""
        captured = {}

        class Ctx:
            def register_platform(self, **kwargs):
                captured.update(kwargs)

        uproar.register(Ctx())
        hint = captured["platform_hint"]
        assert "MEDIA:/absolute/path/to/file" in hint
        assert "2000" in hint

    def test_env_enablement_needs_both_credentials(self, monkeypatch):
        monkeypatch.delenv("UPROAR_TOKEN", raising=False)
        monkeypatch.setenv("UPROAR_BOT_ID", "bot_1")
        assert uproar._env_enablement() is None

        monkeypatch.setenv("UPROAR_TOKEN", "tok")
        out = uproar._env_enablement()
        assert out["bot_id"] == "bot_1"

    def test_env_enablement_seeds_the_home_channel(self, monkeypatch):
        monkeypatch.setenv("UPROAR_TOKEN", "tok")
        monkeypatch.setenv("UPROAR_BOT_ID", "bot_1")
        monkeypatch.setenv("UPROAR_HOME_CHANNEL", "ch_home")
        out = uproar._env_enablement()
        assert out["home_channel"] == {"chat_id": "ch_home"}

    def test_yaml_config_does_not_override_env(self, monkeypatch):
        monkeypatch.setenv("UPROAR_BOT_ID", "from_env")
        uproar._apply_yaml_config({}, {"bot_id": "from_yaml"})
        assert os.environ["UPROAR_BOT_ID"] == "from_env"

    def test_yaml_config_joins_channel_lists(self, monkeypatch):
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        uproar._apply_yaml_config({}, {"allowed_channels": ["a", "b"]})
        assert os.environ["UPROAR_ALLOWED_CHANNELS"] == "a,b"


@pytest.mark.asyncio
class TestWireFormatMentions:
    """Uproar sends mentions as @user[[uuid]], not @handle."""

    async def test_own_mention_is_stripped_from_the_text(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(
            a,
            _msg(
                content=f"hey whats up @user[[{BOT_USER}]]",
                mentions=[{"user_id": BOT_USER, "display_name": "hermes"}],
            ),
        )
        assert event is not None
        assert "[[" not in event.text
        assert event.text == "hey whats up"

    async def test_other_mentions_become_readable_handles(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(
            a,
            _msg(
                content=f"@user[[{BOT_USER}]] ping @user[[{ADA_USER}]] please",
                mentions=[
                    {"user_id": BOT_USER, "display_name": "hermes"},
                    {"user_id": ADA_USER, "display_name": "Ada"},
                ],
            ),
        )
        assert event is not None
        assert event.text == "ping @Ada please"

    async def test_unknown_mention_falls_back(self, monkeypatch):
        a = _adapter()
        out = a._strip_mention(f"hi @user[[{ADA_USER}]]", {"mentions": []})
        assert out == "hi @user"

    async def test_role_mention_becomes_readable(self):
        a = _adapter()
        out = a._strip_mention("ping @role[[abc-123]] now", {"mentions": []})
        assert out == "ping @role now"

    async def test_gate_accepts_raw_wire_mention_without_mentions_array(
        self, monkeypatch
    ):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(a, _msg(content=f"yo @user[[{BOT_USER}]]"))
        assert event is not None


@pytest.mark.asyncio
class TestSlashCommands:
    """The bot instructs users to type bare /sethome, so it must accept it."""

    async def test_bare_slash_command_bypasses_the_mention_gate(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(a, _msg(content="/sethome"))
        assert event is not None
        assert event.message_type is uproar.MessageType.COMMAND
        assert event.text == "/sethome"

    async def test_leading_whitespace_command_still_counts(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(a, _msg(content="   /reset"))
        assert event is not None
        assert event.message_type is uproar.MessageType.COMMAND
        assert event.text == "/reset"

    async def test_plain_text_is_still_gated(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        assert await _capture(a, _msg(content="not a command")) is None

    async def test_allowed_channels_whitelist_still_blocks_commands(self, monkeypatch):
        """The channel whitelist is a hard boundary, commands do not escape it."""
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.setenv("UPROAR_ALLOWED_CHANNELS", "ch_other")
        a = _adapter()
        assert await _capture(a, _msg(content="/sethome")) is None


@pytest.mark.asyncio
class TestReplyContext:
    """The model must see what the user replied to, not just that they did."""

    async def test_reply_context_is_populated(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(
            a,
            _msg(
                content="what did you mean",
                reply_to="msg_prev",
                reply_msg={
                    "id": "msg_prev",
                    "user_id": ADA_USER,
                    "content": "the deploy is done",
                    "username": "ada",
                    "display_name": "Ada",
                },
            ),
        )
        assert event is not None
        assert event.reply_to_message_id == "msg_prev"
        assert event.reply_to_text == "the deploy is done"
        assert event.reply_to_author_id == ADA_USER
        assert event.reply_to_author_name == "Ada"
        assert event.reply_to_is_own_message is False

    async def test_reply_to_the_agent_is_flagged(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(
            a,
            _msg(
                content="go on",
                reply_msg={
                    "id": "m9",
                    "user_id": BOT_USER,
                    "content": "here is the plan",
                    "display_name": "hermes",
                },
            ),
        )
        assert event is not None
        assert event.reply_to_is_own_message is True

    async def test_reply_text_has_wire_mentions_resolved(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(
            a,
            _msg(
                content="huh",
                mentions=[{"user_id": ADA_USER, "display_name": "Ada"}],
                reply_msg={
                    "id": "m9",
                    "user_id": ADA_USER,
                    "content": f"ping @user[[{ADA_USER}]] about it",
                    "display_name": "Ada",
                },
            ),
        )
        assert event is not None
        assert "[[" not in event.reply_to_text
        assert event.reply_to_text == "ping @Ada about it"

    async def test_no_reply_leaves_fields_unset(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "false")
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(a, _msg())
        assert event is not None
        assert event.reply_to_message_id is None
        assert event.reply_to_text is None


@pytest.mark.asyncio
class TestRateLimitRetry:
    """Slowmode and the write budget both answer 429 with retry_after."""

    def _resp(self, status, headers=None):
        r = MagicMock()
        r.status = status
        r.headers = headers or {}
        return r

    async def test_retry_after_prefers_the_body(self):
        a = _adapter()
        assert a._retry_after_seconds(
            self._resp(429), '{"error":"slowmode active","retry_after":7}'
        ) == 7

    async def test_retry_after_falls_back_to_the_header(self):
        a = _adapter()
        assert a._retry_after_seconds(
            self._resp(429, {"Retry-After": "12"}), "not json"
        ) == 12

    async def test_retry_after_is_capped(self):
        a = _adapter()
        assert a._retry_after_seconds(
            self._resp(429), '{"retry_after":99999}'
        ) == uproar._RETRY_429_MAX_DELAY

    async def test_retry_after_rejects_zero(self):
        a = _adapter()
        assert a._retry_after_seconds(
            self._resp(429), '{"retry_after":0}'
        ) == uproar._RETRY_429_DEFAULT_DELAY


@pytest.mark.asyncio
class TestEditOverflow:
    """A finalized answer over the cap must not lose its tail."""

    async def test_finalize_splits_instead_of_truncating(self):
        a = _adapter()
        sent = []

        async def _exec(action, **kw):
            sent.append((action, kw))
            return {"id": f"m{len(sent)}"}

        a._execute = _exec
        long = "z" * 5000
        result = await a.edit_message("ch_1", "m0", long, finalize=True)

        assert result.success is True
        assert sent[0][0] == "edit"
        assert [s[0] for s in sent[1:]] == ["send"] * (len(sent) - 1)
        delivered = "".join(s[1]["content"] for s in sent)
        assert len(delivered) >= len(long)

    async def test_streaming_preview_truncates_and_dedupes(self):
        a = _adapter()
        calls = []

        async def _exec(action, **kw):
            calls.append(kw.get("content"))
            return {"id": "m0"}

        a._execute = _exec
        long = "y" * 5000
        await a.edit_message("ch_1", "m0", long, finalize=False)
        await a.edit_message("ch_1", "m0", long, finalize=False)

        assert len(calls) == 1
        assert len(calls[0]) <= uproar.MAX_MESSAGE_LENGTH

    async def test_short_edit_is_untouched(self):
        a = _adapter()
        a._execute = AsyncMock(return_value={"id": "m0"})
        result = await a.edit_message("ch_1", "m0", "short", finalize=True)
        assert result.success is True
        a._execute.assert_awaited_once_with(
            "edit", message_id="m0", content="short"
        )


class TestChatTypeClassification:
    """is_group tracks DM ownership, so a shrunken group is still a group."""

    def test_server_channel(self):
        assert uproar.UproarAdapter._chat_type(
            {"server_id": "srv_1", "is_dm": False}
        ) == "channel"

    def test_one_to_one_dm(self):
        assert uproar.UproarAdapter._chat_type(
            {"server_id": None, "is_dm": True, "is_group": False, "member_count": 2}
        ) == "dm"

    def test_group_dm(self):
        assert uproar.UproarAdapter._chat_type(
            {"server_id": None, "is_dm": True, "is_group": True, "member_count": 4}
        ) == "group"

    def test_two_member_group_is_still_a_group(self):
        """The trap a member count would fall into."""
        assert uproar.UproarAdapter._chat_type(
            {"server_id": None, "is_dm": True, "is_group": True, "member_count": 2}
        ) == "group"

    def test_missing_is_group_degrades_to_dm(self):
        """Older servers omit the field; never guess 'group' without it."""
        assert uproar.UproarAdapter._chat_type(
            {"server_id": None, "is_dm": True}
        ) == "dm"


@pytest.mark.asyncio
class TestGroupDmGating:
    async def test_group_dm_requires_a_mention(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        assert (
            await _capture(a, _msg(server_id="", content="chatter"), chat_type="group")
        ) is None

    async def test_group_dm_passes_with_a_mention(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        monkeypatch.delenv("UPROAR_FREE_RESPONSE_CHANNELS", raising=False)
        monkeypatch.delenv("UPROAR_ALLOWED_CHANNELS", raising=False)
        a = _adapter()
        event = await _capture(
            a,
            _msg(
                server_id="",
                content=f"@user[[{BOT_USER}]] hi",
                mentions=[{"user_id": BOT_USER, "display_name": "hermes"}],
            ),
            chat_type="group",
        )
        assert event is not None
        assert event.source.chat_type == "group"

    async def test_one_to_one_dm_still_needs_no_mention(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REQUIRE_MENTION", "true")
        a = _adapter()
        event = await _capture(
            a, _msg(server_id="", content="chatter"), chat_type="dm"
        )
        assert event is not None
        assert event.source.chat_type == "dm"


class _FakeResp:
    def __init__(self, status, body, headers=None):
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    """Replays a queued list of responses, recording each call."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def post(self, *a, **kw):
        self.calls += 1
        return self._responses.pop(0)


@pytest.mark.asyncio
class TestExecuteRetryLoop:
    async def test_a_429_is_retried_and_then_succeeds(self, monkeypatch):
        slept = []

        async def _sleep(d):
            slept.append(d)

        monkeypatch.setattr(uproar.asyncio, "sleep", _sleep)
        a = _adapter()
        a._session = _FakeSession([
            _FakeResp(429, '{"error":"slowmode active","retry_after":3}'),
            _FakeResp(201, '{"id":"m1"}'),
        ])

        out = await a._execute("send", channel_id="ch_1", content="hi")
        assert out == {"id": "m1"}
        assert a._session.calls == 2
        assert slept == [3]

    async def test_retries_are_bounded(self, monkeypatch):
        async def _sleep(d):
            pass

        monkeypatch.setattr(uproar.asyncio, "sleep", _sleep)
        a = _adapter()
        a._session = _FakeSession(
            [_FakeResp(429, '{"retry_after":1}')] * (uproar._RETRY_429_ATTEMPTS + 1)
        )

        assert await a._execute("send", channel_id="ch_1", content="hi") is None
        assert a._session.calls == uproar._RETRY_429_ATTEMPTS + 1

    async def test_a_non_429_error_is_not_retried(self, monkeypatch):
        a = _adapter()
        a._session = _FakeSession([_FakeResp(403, '{"error":"bot is paused"}')])
        assert await a._execute("send", channel_id="ch_1", content="x") is None
        assert a._session.calls == 1

    async def test_chunked_send_survives_a_mid_reply_429(self, monkeypatch):
        """The regression: chunk 1 landed, the rest were dropped silently."""
        async def _sleep(d):
            pass

        monkeypatch.setattr(uproar.asyncio, "sleep", _sleep)
        a = _adapter()
        a._session = _FakeSession([
            _FakeResp(201, '{"id":"m1"}'),
            _FakeResp(429, '{"retry_after":1}'),
            _FakeResp(201, '{"id":"m2"}'),
            _FakeResp(201, '{"id":"m3"}'),
        ])

        result = await a.send("ch_1", "z" * 5000)
        assert result.success is True
        assert result.message_id == "m3"
        assert a._session.calls == 4


class TestInteractiveSetup:
    """The wizard is the first thing a new user touches; it must not crash."""

    def _run(self, monkeypatch, answers, existing=None):
        saved, removed = {}, []
        store = dict(existing or {})

        import hermes_cli.config as cfg
        import hermes_cli.cli_output as out

        replies = list(answers)

        def _prompt(text, password=False, **kw):
            return replies.pop(0) if replies else ""

        monkeypatch.setattr(out, "prompt", _prompt, raising=False)
        monkeypatch.setattr(out, "prompt_yes_no", lambda *a, **k: True, raising=False)
        for name in ("print_header", "print_info", "print_success"):
            monkeypatch.setattr(out, name, lambda *a, **k: None, raising=False)
        monkeypatch.setattr(cfg, "get_env_value", lambda k: store.get(k), raising=False)
        monkeypatch.setattr(
            cfg, "save_env_value", lambda k, v: saved.__setitem__(k, v), raising=False
        )
        monkeypatch.setattr(
            cfg,
            "remove_env_value",
            lambda k: removed.append(k) or True,
            raising=False,
        )

        uproar.interactive_setup()
        return saved, removed

    def test_happy_path_saves_every_field(self, monkeypatch):
        saved, _ = self._run(
            monkeypatch,
            ["", "bot_1", "tok_1", "usr_a, usr_b", "ch_home"],
        )
        assert saved["UPROAR_URL"] == uproar.DEFAULT_URL
        assert saved["UPROAR_BOT_ID"] == "bot_1"
        assert saved["UPROAR_TOKEN"] == "tok_1"
        assert saved["UPROAR_ALLOWED_USERS"] == "usr_a,usr_b"
        assert saved["UPROAR_HOME_CHANNEL"] == "ch_home"

    def test_missing_bot_id_saves_no_credentials(self, monkeypatch):
        saved, _ = self._run(monkeypatch, ["", "", "tok_1"])
        assert "UPROAR_BOT_ID" not in saved
        assert "UPROAR_TOKEN" not in saved

    def test_missing_token_saves_no_credentials(self, monkeypatch):
        saved, _ = self._run(monkeypatch, ["", "bot_1", ""])
        assert "UPROAR_TOKEN" not in saved
        assert "UPROAR_BOT_ID" not in saved

    def test_custom_url_is_normalised(self, monkeypatch):
        saved, _ = self._run(
            monkeypatch, ["https://chat.example.com/", "b", "t", "", ""]
        )
        assert saved["UPROAR_URL"] == "https://chat.example.com"

    def test_empty_home_clears_the_old_one(self, monkeypatch):
        _, removed = self._run(monkeypatch, ["", "b", "t", "", ""])
        assert "UPROAR_HOME_CHANNEL" in removed


class TestPastedCredentials:
    """The create response hands you a URL, so people paste the whole thing."""

    def test_url_pasted_into_the_id_field(self):
        assert uproar._split_pasted_credentials(
            "https://uproar.chat/api/bots/bot_9/tok_9", ""
        ) == ("bot_9", "tok_9")

    def test_url_pasted_into_the_token_field(self):
        assert uproar._split_pasted_credentials(
            "bot_9", "https://uproar.chat/api/bots/bot_9/tok_9"
        ) == ("bot_9", "tok_9")

    def test_plain_values_pass_through(self):
        assert uproar._split_pasted_credentials("bot_9", "tok_9") == ("bot_9", "tok_9")

    def test_a_truncated_url_is_left_alone(self):
        assert uproar._split_pasted_credentials(
            "https://uproar.chat/api/bots/bot_9", "tok_9"
        ) == ("https://uproar.chat/api/bots/bot_9", "tok_9")


@pytest.mark.asyncio
class TestReactionAcks:
    """Discord marks a message 👀 then ✅/❌; match that."""

    def _event(self):
        a = _adapter()
        src = a.build_source(chat_id="ch_1", chat_type="channel", user_id=ADA_USER)
        return a, uproar.MessageEvent(
            text="hi",
            message_type=uproar.MessageType.TEXT,
            source=src,
            message_id="msg_1",
        )

    async def test_start_adds_the_in_progress_marker(self, monkeypatch):
        monkeypatch.delenv("UPROAR_REACTIONS", raising=False)
        a, event = self._event()
        a._execute = AsyncMock(return_value={"status": "ok"})
        await a.on_processing_start(event)
        a._execute.assert_awaited_once_with(
            "react", message_id="msg_1", emoji=a._ACK_EMOJI
        )

    async def test_reactions_can_be_switched_off(self, monkeypatch):
        monkeypatch.setenv("UPROAR_REACTIONS", "false")
        a, event = self._event()
        a._execute = AsyncMock()
        await a.on_processing_start(event)
        a._execute.assert_not_awaited()

    async def test_remove_defaults_to_the_marker_not_an_empty_emoji(self):
        """Uproar rejects an unreact with no emoji, so the default matters."""
        a = _adapter()
        a._execute = AsyncMock(return_value={})
        await a._remove_reaction("ch_1", "msg_1")
        a._execute.assert_awaited_once_with(
            "unreact", message_id="msg_1", emoji=a._ACK_EMOJI
        )

    async def test_success_swaps_the_marker_for_a_tick(self):
        from gateway.platforms.base import ProcessingOutcome

        a, event = self._event()
        calls = []

        async def _exec(action, **kw):
            calls.append((action, kw.get("emoji")))
            return {}

        a._execute = _exec
        await a.on_processing_complete(event, ProcessingOutcome.SUCCESS)
        assert calls == [
            ("unreact", a._ACK_EMOJI),
            ("react", a._OK_EMOJI),
        ]

    async def test_failure_swaps_the_marker_for_a_cross(self):
        from gateway.platforms.base import ProcessingOutcome

        a, event = self._event()
        calls = []

        async def _exec(action, **kw):
            calls.append((action, kw.get("emoji")))
            return {}

        a._execute = _exec
        await a.on_processing_complete(event, ProcessingOutcome.FAILURE)
        assert calls == [
            ("unreact", a._ACK_EMOJI),
            ("react", a._FAIL_EMOJI),
        ]


@pytest.mark.asyncio
class TestReactionEvents:
    """Slack forwards reactions to the hook surface; match that."""

    async def test_reaction_add_reaches_the_hook(self):
        a = _adapter()
        seen = []
        a.set_reaction_handler(lambda e: seen.append(e) or asyncio_sleep())
        await a._handle_frame({
            "type": "reaction_add",
            "data": {
                "message_id": "msg_1", "channel_id": "ch_1",
                "user_id": ADA_USER, "emoji": "\U0001f44d",
                "message": {"user_id": BOT_USER},
            },
        })
        assert len(seen) == 1
        e = seen[0]
        assert e["platform"] == "uproar"
        assert e["event_name"] == "reaction:added"
        assert e["reaction"] == "\U0001f44d"
        assert e["user_id"] == ADA_USER
        assert e["item_user_id"] == BOT_USER
        assert e["channel_id"] == "ch_1"
        assert e["message_ts"] == "msg_1"

    async def test_reaction_remove_names_the_right_event(self):
        a = _adapter()
        seen = []
        a.set_reaction_handler(lambda e: seen.append(e) or asyncio_sleep())
        await a._handle_frame({
            "type": "reaction_remove",
            "data": {"message_id": "m", "channel_id": "c", "user_id": ADA_USER, "emoji": "x"},
        })
        assert seen[0]["event_name"] == "reaction:removed"

    async def test_own_reactions_do_not_echo(self):
        """The adapter adds ack reactions; those must not fire the hook."""
        a = _adapter()
        seen = []
        a.set_reaction_handler(lambda e: seen.append(e) or asyncio_sleep())
        await a._handle_frame({
            "type": "reaction_add",
            "data": {"message_id": "m", "channel_id": "c", "user_id": BOT_USER, "emoji": "\U0001f440"},
        })
        assert seen == []

    async def test_no_handler_is_safe(self):
        a = _adapter()
        await a._handle_frame({
            "type": "reaction_add",
            "data": {"message_id": "m", "channel_id": "c", "user_id": ADA_USER, "emoji": "x"},
        })


async def asyncio_sleep():
    return None


@pytest.mark.asyncio
class TestMessageChangeEvents:
    """Discord surfaces edits and deletes; match the envelope."""

    def _wire(self, monkeypatch, subscribed=True):
        a = _adapter()
        got = []

        async def _handler(event, source):
            got.append((event, source))

        a.set_platform_event_handler(_handler)
        monkeypatch.setattr(
            a, "_platform_events_subscribed", staticmethod(lambda: subscribed)
        )
        return a, got

    async def test_edit_is_normalised(self, monkeypatch):
        a, got = self._wire(monkeypatch)
        await a._handle_frame({
            "type": "message_edit",
            "data": {
                "id": "msg_1", "channel_id": "ch_1", "server_id": "srv_1",
                "user_id": ADA_USER, "content": "fixed typo",
                "edited_at": "2026-08-22T23:00:00Z", "display_name": "Ada",
            },
        })
        assert len(got) == 1
        event, source = got[0]
        assert event["platform"] == "uproar"
        assert event["event_type"] == "message_edited"
        assert event["payload"]["text"] == "fixed typo"
        assert event["payload"]["message_id"] == "msg_1"
        assert source.chat_id == "ch_1"
        assert source.scope_id == "srv_1"

    async def test_delete_is_normalised(self, monkeypatch):
        a, got = self._wire(monkeypatch)
        await a._handle_frame({
            "type": "message_delete",
            "data": {"message_id": "msg_2", "channel_id": "ch_1", "user_id": ADA_USER},
        })
        assert got[0][0]["event_type"] == "message_deleted"
        assert got[0][0]["payload"]["message_id"] == "msg_2"

    async def test_the_agents_own_edits_are_not_events(self, monkeypatch):
        """Progressive streaming edits are noise, not user activity."""
        a, got = self._wire(monkeypatch)
        await a._handle_frame({
            "type": "message_edit",
            "data": {"id": "m", "channel_id": "c", "user_id": BOT_USER, "content": "partial"},
        })
        assert got == []

    async def test_nothing_fires_without_a_subscriber(self, monkeypatch):
        a, got = self._wire(monkeypatch, subscribed=False)
        await a._handle_frame({
            "type": "message_edit",
            "data": {"id": "m", "channel_id": "c", "user_id": ADA_USER, "content": "x"},
        })
        assert got == []


@pytest.mark.asyncio
class TestDmTargetResolution:
    """An agent told to DM a user gets a user id, not a channel id."""

    async def test_a_rejected_channel_is_retried_as_a_dm(self):
        a = _adapter()
        calls = []

        async def _exec(action, **kw):
            calls.append((action, kw))
            if action == "send" and kw.get("channel_id") == ADA_USER:
                a._last_error = '{"error":"invalid channel"}'
                return None
            if action == "open_dm":
                return {"id": "ch_dm"}
            return {"id": "m1"}

        a._execute = _exec
        result = await a.send(ADA_USER, "hello")

        assert result.success is True
        assert [c[0] for c in calls] == ["send", "open_dm", "send"]
        assert calls[1][1] == {"target_user_id": ADA_USER}
        assert calls[2][1]["channel_id"] == "ch_dm"

    async def test_a_normal_channel_costs_no_extra_request(self):
        """The common path must not probe or open anything."""
        a = _adapter()
        a._read = AsyncMock()
        calls = []

        async def _exec(action, **kw):
            calls.append(action)
            return {"id": "m1"}

        a._execute = _exec
        await a.send("ch_1", "hello")

        assert calls == ["send"]
        a._read.assert_not_awaited()

    async def test_the_opened_dm_is_reused(self):
        a = _adapter()
        opens = []

        async def _exec(action, **kw):
            if action == "send" and kw.get("channel_id") == ADA_USER:
                a._last_error = '{"error":"invalid channel"}'
                return None
            if action == "open_dm":
                opens.append(kw)
                return {"id": "ch_dm"}
            return {"id": "m1"}

        a._execute = _exec
        await a.send(ADA_USER, "one")
        await a.send(ADA_USER, "two")
        assert len(opens) == 1

    async def test_other_errors_are_not_retried_as_dms(self):
        a = _adapter()
        calls = []

        async def _exec(action, **kw):
            calls.append(action)
            a._last_error = '{"error":"bot lacks permission to send in this channel"}'
            return None

        a._execute = _exec
        result = await a.send("ch_1", "hi")
        assert result.success is False
        assert calls == ["send"]

    async def test_a_failed_open_reports_failure(self):
        a = _adapter()

        async def _exec(action, **kw):
            if action == "open_dm":
                return None
            a._last_error = '{"error":"invalid channel"}'
            return None

        a._execute = _exec
        result = await a.send(ADA_USER, "hi")
        assert result.success is False


@pytest.mark.asyncio
class TestEmbedPrompts:
    """Uproar has no buttons, but it does have embeds. Use them."""

    async def test_clarify_renders_as_an_embed_and_keeps_the_text(self):
        a = _adapter()
        sent = []

        async def _exec(action, **kw):
            sent.append(kw)
            return {"id": "m1"}

        a._execute = _exec
        with patch("tools.clarify_gateway.mark_awaiting_text") as mark:
            await a.send_clarify("ch_1", "Which one?", ["alpha", "beta"], "cid", "sk")

        assert mark.called, "the text intercept must still be registered"
        embeds = sent[0]["embeds"]
        assert len(embeds) == 1
        assert embeds[0]["title"] == "Hermes needs your input"
        assert "alpha" in embeds[0]["description"]
        assert "1." in embeds[0]["description"]
        assert "content" not in sent[0]

    async def test_a_normal_send_is_not_an_embed(self):
        a = _adapter()
        sent = []

        async def _exec(action, **kw):
            sent.append(kw)
            return {"id": "m1"}

        a._execute = _exec
        await a.send("ch_1", "just a message")
        assert "embeds" not in sent[0]
        assert sent[0]["content"] == "just a message"

    async def test_an_oversized_description_is_trimmed(self):
        a = _adapter()
        embed = a._embed_from_metadata("z" * 9000, {"uproar_embed": {"title": "T"}})
        assert len(embed["description"]) <= uproar._EMBED_DESCRIPTION_LIMIT

    async def test_colour_stays_in_uproar_range(self):
        """Uproar rejects a color outside 0..16777215."""
        a = _adapter()
        ok = a._embed_from_metadata("x", {"uproar_embed": {"color": 0x5865F2}})
        assert 0 <= ok["color"] <= 16777215
        bad = a._embed_from_metadata("x", {"uproar_embed": {"color": 99999999}})
        assert "color" not in bad


@pytest.mark.asyncio
class TestListChannels:
    """Without this the directory is empty and only raw ids can be targeted."""

    def _reader(self, servers, channels_by_server):
        async def _read(path, params=None):
            if path == "servers":
                return servers
            if path == "channels":
                return channels_by_server.get((params or {}).get("server_id"))
            return None
        return _read

    async def test_channels_are_listed_across_servers(self):
        a = _adapter()
        a._read = self._reader(
            [{"id": "srv_1", "name": "hermes"}, {"id": "srv_2", "name": "other"}],
            {
                "srv_1": [{"id": "ch_1", "name": "general", "server_id": "srv_1"}],
                "srv_2": [{"id": "ch_2", "name": "random", "server_id": "srv_2"}],
            },
        )
        out = await a.list_channels()
        assert {c["name"] for c in out} == {"general", "random"}
        assert {c["guild"] for c in out} == {"hermes", "other"}
        assert all(c["type"] == "channel" for c in out)

    async def test_archived_channels_are_skipped(self):
        a = _adapter()
        a._read = self._reader(
            [{"id": "srv_1", "name": "hermes"}],
            {"srv_1": [
                {"id": "ch_1", "name": "general", "server_id": "srv_1"},
                {"id": "ch_2", "name": "old", "server_id": "srv_1", "is_archived": True},
            ]},
        )
        assert [c["name"] for c in await a.list_channels()] == ["general"]

    async def test_listing_warms_the_channel_cache(self):
        """A later get_chat_info must not re-fetch what this already read."""
        a = _adapter()
        a._read = self._reader(
            [{"id": "srv_1", "name": "hermes"}],
            {"srv_1": [{"id": "ch_1", "name": "general", "server_id": "srv_1"}]},
        )
        await a.list_channels()
        assert "ch_1" in a._channel_cache

        a._read = AsyncMock()
        info = await a.get_chat_info("ch_1")
        assert info["name"] == "general"
        a._read.assert_not_awaited()

    async def test_no_servers_is_not_an_error(self):
        a = _adapter()
        a._read = AsyncMock(return_value=None)
        assert await a.list_channels() == []

    async def test_the_list_is_cached_between_rebuilds(self):
        """The gateway rebuilds every 5 minutes; each rebuild costs 1+N reads."""
        a = _adapter()
        reads = []

        async def _read(path, params=None):
            reads.append(path)
            if path == "servers":
                return [{"id": "srv_1", "name": "hermes"}]
            return [{"id": "ch_1", "name": "general", "server_id": "srv_1"}]

        a._read = _read
        first = await a.list_channels()
        second = await a.list_channels()

        assert first == second
        assert reads == ["servers", "channels"], "second call must not re-read"

    async def test_a_failed_read_keeps_the_last_good_list(self):
        """Returning [] would drop every name target until the next success."""
        a = _adapter()

        async def _ok(path, params=None):
            if path == "servers":
                return [{"id": "srv_1", "name": "hermes"}]
            return [{"id": "ch_1", "name": "general", "server_id": "srv_1"}]

        a._read = _ok
        good = await a.list_channels()
        assert len(good) == 1

        a._channels_cached_at = 0.0
        a._read = AsyncMock(return_value=None)
        assert await a.list_channels() == good

    async def test_an_empty_result_does_not_wipe_the_cache(self):
        a = _adapter()

        async def _ok(path, params=None):
            if path == "servers":
                return [{"id": "srv_1", "name": "hermes"}]
            return [{"id": "ch_1", "name": "general", "server_id": "srv_1"}]

        a._read = _ok
        good = await a.list_channels()

        a._channels_cached_at = 0.0

        async def _empty(path, params=None):
            return []

        a._read = _empty
        assert await a.list_channels() == good


@pytest.mark.asyncio
class TestStaleErrorState:
    async def test_a_network_failure_does_not_inherit_an_old_error(self):
        """A stale 'invalid channel' would trigger a bogus open_dm on the next send."""
        a = _adapter()
        a._last_error = '{"error":"invalid channel"}'
        calls = []

        async def _exec_real(action, **kw):
            calls.append(action)
            return None

        a._session = _FakeSession([_FakeResp(500, "boom")])
        assert await a._execute("send", channel_id="ch_1", content="x") is None
        assert "invalid channel" not in a._last_error

    async def test_a_success_clears_the_previous_error(self):
        a = _adapter()
        a._last_error = '{"error":"invalid channel"}'
        a._session = _FakeSession([_FakeResp(201, '{"id":"m1"}')])
        await a._execute("send", channel_id="ch_1", content="x")
        assert a._last_error == ""
