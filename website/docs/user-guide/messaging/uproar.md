# Uproar

The Uproar adapter connects Hermes to [uproar.chat](https://uproar.chat), a chat platform with servers, channels, and DMs. The agent **dials out** over a WebSocket, so there is no public URL to host, no webhook receiver, and no tunnel for local development. It uses `aiohttp`, which Hermes already depends on.

Uproar renders Markdown, so replies keep their formatting. The adapter supports channels, 1:1 DMs, group DMs, mention gating, replies, file attachments including audio and video, streaming edits, reactions, and home-channel cron delivery. Uproar's wider surface (voice channels, Spaces, watch parties, polls) is reachable over MCP, covered [below](#beyond-messaging).

> Run `hermes gateway setup` and pick **Uproar** for a guided walk-through.

## Prerequisites

You need an agent on Uproar, and both halves of its credential.

1. Open **User Settings → Bots & Agents → My Agents**.
2. Name the agent and click **Create agent**. Copy the token now, it is shown once.
3. Under **Your agents**, the grey monospace line beneath the name is the **bot ID**.
4. In step 2 of the wizard, pick a server and click **Add / Request**. It joins immediately if you manage agents there.

:::note
Server Settings can also create an agent, but that screen does not show the bot ID, and every API call needs `/api/bots/{id}/{token}`. Use **My Agents** unless you already know the ID.
:::

## Configure Hermes

### Option A — environment variables

| Variable | Required | Description |
|----------|:--------:|-------------|
| `UPROAR_BOT_ID` | ✅ | Agent (bot) ID |
| `UPROAR_TOKEN` | ✅ | Agent execute token, shown once at create or regenerate |
| `UPROAR_URL` | — | Server URL (default: `https://uproar.chat`) |
| `UPROAR_ALLOWED_USERS` | — | Comma-separated Uproar user IDs allowed to talk to the agent |
| `UPROAR_ALLOW_ALL_USERS` | — | Allow any user to trigger the agent (dev only) |
| `UPROAR_HOME_CHANNEL` | — | Channel ID for cron and notification delivery |
| `UPROAR_REQUIRE_MENTION` | — | Require an `@mention` in server channels and group DMs (default `true`) |
| `UPROAR_FREE_RESPONSE_CHANNELS` | — | Channel IDs where no mention is required |
| `UPROAR_ALLOWED_CHANNELS` | — | If set, the agent only responds in these channels |
| `UPROAR_REACTIONS` | — | Acknowledge messages with reactions (default `true`) |
| `UPROAR_PROXY` | — | Proxy URL for out-of-process cron delivery |

### Option B — config.yaml

```yaml
uproar:
  bot_id: "120c3465-e907-454a-a27d-618c1052c694"
  url: https://uproar.chat
  home_channel: "7e530572-b868-4263-8129-439f3b748672"
  require_mention: true
  free_response_channels: []
  allowed_channels: []
  allowed_users: []
```

The token stays in the environment or your secret store, never in `config.yaml`.

## Access control

By default the gateway denies senders it does not recognise. Set `UPROAR_ALLOWED_USERS` to your Uproar user ID, or the agent will ignore everyone.

To find your user ID, open your profile, or right-click yourself and choose **Copy ID**.

## Where the agent answers

| Where | Default behaviour |
|-------|-------------------|
| Server channel | Answers only when mentioned, replied to, or `@everyone`'d |
| 1:1 DM | Answers everything |
| Group DM | Answers only when mentioned, same as a channel |

A group DM is detected from `is_group` on the channel, which tracks whether the DM has an owner. A group whose members left is still a group, so member counts are not used.

Slash commands such as `/sethome` and `/reset` always work without a mention, since typing one is already addressing the agent.

Set `UPROAR_REQUIRE_MENTION=false` to answer everything everywhere, or list specific channels in `UPROAR_FREE_RESPONSE_CHANNELS`.

## Home channel

Cron results and cross-platform messages go to the home channel. Set it by typing `/sethome` in the channel you want, or set `UPROAR_HOME_CHANNEL` to a channel ID.

## Media

Send a file by including `MEDIA:/absolute/path/to/file` in a reply. Images arrive as attachments with thumbnails, and other files as downloadable attachments. Markdown image links such as `![alt](url)` render inline without being uploaded.

Inbound attachments are downloaded to the local media cache so vision tools can read them.

## Beyond messaging

The gateway adapter covers the messaging surface: channels, DMs, attachments, reactions, and edits. Uproar itself is considerably larger, and an agent reaches the rest through Uproar's own MCP server rather than the adapter.

| Surface | Examples |
|---------|----------|
| Voice channels | `go_live`, `set_video`, `set_voice_mute`, `list_live_channels` |
| Spaces (live audio rooms) | `start_space`, `join_space`, `send_space_chat`, `promote_speaker` |
| Watch parties | `start_watch_party`, `control_watch_party`, `schedule_watch_party` |
| Polls | `create_poll`, `close_poll` |
| Feed posts | posting, reposts, notifications |
| Roles and moderation | role CRUD, channel overrides, timeout, kick, ban |
| Social graph | friends, follows, blocks |

Point any MCP client at `https://uproar.chat/mcp` with the same bot token to get all of it. That keeps the gateway adapter thin and avoids a second tool stack inside it.

```bash
claude mcp add --transport http uproar https://uproar.chat/mcp
```

Voice and video are media-plane features. The MCP tools are the control plane, so `go_live` and `start_space` return a LiveKit token that a media-capable client uses; the agent orchestrates rather than carries audio.

## Transport

The adapter connects to `GET /api/bots/{id}/stream` and receives events in real time. If the socket drops it reconnects with exponential backoff and replays anything missed through the events cursor, so no message is lost across a restart.

Typing indicators are sent as a socket frame rather than an HTTP call, so keeping the typing bubble alive costs no rate-limit budget.

## Limits

| Limit | Value |
|-------|-------|
| Message content | 2000 characters, longer replies are split |
| Write actions | 30/min per agent |
| Typing | 120/min per agent |
| Editing your own message | 90/min per agent |
| Reads and events | 60/min per agent |
| Attachments | 4 files per upload request |

The adapter honours `retry_after` on a 429 and retries, so a long reply that trips channel slowmode still delivers every chunk.

## Troubleshooting

**The agent never answers in a channel.** It requires a mention by default. Mention it, or set `UPROAR_FREE_RESPONSE_CHANNELS`.

**The agent never answers at all.** Check `UPROAR_ALLOWED_USERS` contains your user ID. Without an allowlist the gateway denies unknown senders.

**`invalid token` on connect (401).** The token was regenerated. Issue a new one in My Agents and update `UPROAR_TOKEN`.

**`bot is paused` on connect (403).** Resume the agent from the server's Bots registry. A paused agent is frozen for both delivery and actions.

**The agent is not in the server.** An agent joins by admit-by-handle, an invite code, or a knock that an admin approves. It never joins on its own.
