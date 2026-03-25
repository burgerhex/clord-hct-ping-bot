import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- Configuration (Use GitHub Secrets / .env.dev) ---
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
# Comma-separated list of channel IDs to scan, e.g. "123456,789012"
CHANNEL_IDS = [c.strip() for c in os.getenv("ACHIEVEMENTS_CHANNEL_IDS", "").split(",") if c.strip()]
# Comma-separated list of role IDs to watch for pings, e.g. "111,222"
ROLE_IDS = {r.strip() for r in os.getenv("HARD_CLEARS_ROLE_IDS", "").split(",") if r.strip()}
WEBHOOK_URL = os.getenv("TARGET_WEBHOOK_URL")
# Parse the webhook's own ID from the URL (.../webhooks/{id}/{token})
# Used to filter the target channel to only our bot's messages.
_webhook_url_parts = (WEBHOOK_URL or "").rstrip("/").split("/")
WEBHOOK_ID = _webhook_url_parts[-2] if len(_webhook_url_parts) >= 2 else None
# Channel ID of the forwarding destination (needed to read messages + react)
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SHEET_ID = os.getenv("STATE_SHEET_ID")
GUILD_ID = os.getenv("GUILD_ID")
STATE_WORKSHEET = "Sheet1"

DISCORD_API = "https://discord.com/api/v10"
DISCORD_EPOCH = 1420070400000
HEADERS = {"Authorization": f"Bot {DISCORD_TOKEN}"}

# Human-readable replacement for ALL watched role mentions.
# All matched role IDs are replaced with this single string.
ROLE_DISPLAY_NAME = "`@Hard Clears Team`"

EMOJI_CHECK = "✅"  # used for forwarded-message reactions
EMOJI_FLAG = "🏁"  # marks fully reviewed forwarding messages

WEBHOOK_DELAY_SECONDS = 1
MESSAGE_FETCH_LIMIT = 100

# --- Google Sheets Setup ---
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds_json = os.getenv("GOOGLE_CREDS_JSON")
creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).worksheet(STATE_WORKSHEET)


def _sheets_retry(fn, retries=5, delay=5):
    """Retry fn() up to `retries` times on transient 5xx Google Sheets errors."""
    import gspread.exceptions
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except gspread.exceptions.APIError as e:
            status = e.response.status_code if hasattr(e, "response") else 0
            if status >= 500 and attempt < retries:
                print(f"  [sheets] 5xx error ({status}), retrying in {delay}s "
                      f"(attempt {attempt}/{retries})...")
                time.sleep(delay)
            else:
                raise
    return None


def get_last_processed_id():
    return _sheets_retry(lambda: sheet.acell("A1").value) or "0"


def update_last_id(new_id):
    _sheets_retry(lambda: sheet.update_acell("A1", str(new_id)))


def get_last_backlog_id():
    return _sheets_retry(lambda: sheet.acell("B1").value) or "0"


def update_backlog_id(msg_id):
    _sheets_retry(lambda: sheet.update_acell("B1", str(msg_id)))


# ---------------------------------------------------------------------------
# Discord API helpers
# ---------------------------------------------------------------------------

def fetch_messages(channel_id, after_id):
    """Fetch up to MESSAGE_FETCH_LIMIT messages from a channel after after_id."""
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    params = {"after": after_id, "limit": MESSAGE_FETCH_LIMIT}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return sorted(resp.json(), key=lambda m: int(m["id"]))


def fetch_latest_messages(channel_id, limit=100):
    """Fetch the most recent `limit` messages from a channel (newest-first from API, returned oldest-first)."""
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    params = {"limit": limit}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return sorted(resp.json(), key=lambda m: int(m["id"]))


def fetch_single_message(channel_id, message_id):
    """Fetch a single message by channel + message ID. Returns None on failure."""
    url = f"{DISCORD_API}/channels/{channel_id}/messages/{message_id}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 200:
        return resp.json()
    print(f"  [warn] Could not fetch message {message_id} in channel {channel_id}: {resp.status_code}")
    return None


def fetch_thread_starter(parent_channel_id, thread_id):
    """
    Fetch the channel message that started this thread.

    Discord threads have a `parent_id` (the channel), and the thread's own ID
    equals the ID of the message that created it — so we can fetch it directly
    from the parent channel.
    """
    resp = requests.get(
        f"{DISCORD_API}/channels/{parent_channel_id}/messages/{thread_id}",
        headers=HEADERS,
    )
    if resp.status_code == 200:
        return resp.json()
    print(f"  [warn] Could not fetch thread starter message {thread_id}: {resp.status_code}")
    return None


def fetch_active_threads(guild_id, channel_ids):
    """
    Return all active (non-archived) threads in the guild that belong to
    any of the given channel_ids. One API call covers all channels.
    """
    url = f"{DISCORD_API}/guilds/{guild_id}/threads/active"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"  [warn] Could not fetch active threads: {resp.status_code}")
        return []
    data = resp.json()
    return [t for t in data.get("threads", []) if t.get("parent_id") in channel_ids]


def fetch_thread_messages_after(thread_id, after_id):
    """Fetch up to MESSAGE_FETCH_LIMIT messages in a thread newer than after_id."""
    url = f"{DISCORD_API}/channels/{thread_id}/messages"
    params = {"after": after_id, "limit": MESSAGE_FETCH_LIMIT}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 200:
        return sorted(resp.json(), key=lambda m: int(m["id"]))
    print(f"  [warn] Could not fetch messages for thread {thread_id}: {resp.status_code}")
    return []


def add_reaction(channel_id, message_id, emoji):
    """Add a reaction to a message. Returns True when it succeeds."""
    import urllib.parse
    encoded = urllib.parse.quote(emoji)
    url = f"{DISCORD_API}/channels/{channel_id}/messages/{message_id}/reactions/{encoded}/@me"
    resp = requests.put(url, headers=HEADERS)
    if resp.status_code == 204:
        return True
    print(f"  [warn] Could not add reaction {emoji} to {message_id}: {resp.status_code} {resp.text}")
    return False


def fetch_pins(channel_id):
    """Return the list of pinned messages in a channel."""
    resp = requests.get(f"{DISCORD_API}/channels/{channel_id}/pins", headers=HEADERS)
    if resp.status_code == 200:
        return resp.json()
    print(f"  [warn] Could not fetch pins for channel {channel_id}: {resp.status_code}")
    return []


def pin_message(channel_id, message_id):
    """Pin a message. Returns True when it succeeds."""
    resp = requests.put(
        f"{DISCORD_API}/channels/{channel_id}/pins/{message_id}",
        headers=HEADERS,
    )
    if resp.status_code == 204:
        return True
    print(f"  [warn] Could not pin message {message_id}: {resp.status_code} {resp.text}")
    return False


def unpin_message(channel_id, message_id):
    """Unpin a message. Returns True when it succeeds."""
    resp = requests.delete(
        f"{DISCORD_API}/channels/{channel_id}/pins/{message_id}",
        headers=HEADERS,
    )
    if resp.status_code == 204:
        return True
    print(f"  [warn] Could not unpin message {message_id}: {resp.status_code} {resp.text}")
    return False


# ---------------------------------------------------------------------------
# Content extraction helpers
# ---------------------------------------------------------------------------

def extract_url(content: str):
    """Return the first URL found in a string, or None."""
    for word in content.split():
        # noinspection HttpUrlsUsage
        if word.startswith("http://") or word.startswith("https://"):
            return word
    return None


def message_has_media(msg: dict):
    """True if the message has attachments or a URL in its content."""
    if msg.get("attachments"):
        return True
    if extract_url(msg.get("content", "")):
        return True
    return False


def replace_role_mentions(content: str) -> str:
    """Replace all watched role mentions with the human-readable display name."""
    for role_id in ROLE_IDS:
        content = content.replace(f"<@&{role_id}>", ROLE_DISPLAY_NAME)
    return content


def reaction_count(msg: dict, emoji: str) -> int:
    """Return the count of a given emoji reaction on a message, or 0."""
    for r in msg.get("reactions", []):
        if r.get("emoji", {}).get("name") == emoji:
            return r.get("count", 0)
    return 0


def has_reaction(msg: dict, emoji: str) -> bool:
    """Return True if the message has at least one of the given emoji reaction."""
    return reaction_count(msg, emoji) > 0


def extract_message_links(embed_msg: dict) -> list[tuple[str, str]]:
    """
    Extract (channel_id, message_id) pairs from discord.com/channels/... URLs
    found in a forwarded webhook message's embed title and fields.
    Returns a list of (channel_id, message_id) tuples.
    """
    import re
    pattern = r"discord\.com/channels/\d+/(\d+)/(\d+)"
    results = []
    seen = set()
    for embed in embed_msg.get("embeds", []):
        # Check title
        for text in [embed.get("title", ""), embed.get("description", "")]:
            for channel_id, msg_id in re.findall(pattern, text):
                if msg_id not in seen:
                    results.append((channel_id, msg_id))
                    seen.add(msg_id)
        # Check fields
        for field in embed.get("fields", []):
            for text in [field.get("name", ""), field.get("value", "")]:
                for channel_id, msg_id in re.findall(pattern, text):
                    if msg_id not in seen:
                        results.append((channel_id, msg_id))
                        seen.add(msg_id)
    return results


# ---------------------------------------------------------------------------
# Source-message resolution
# ---------------------------------------------------------------------------

def resolve_source_message(ping_msg: dict, parent_channel_id: str, batch: list[dict]):
    """
    Determine the 'display' (source) message for the embed.

    Priority:
    1. If the ping came from inside a thread (channel_id != parent_channel_id),
       the source is always the thread's parent channel message.
    2. Ping message has a link or attachment — use it directly.
    3. Ping is a reply — if the referenced message has media, use that.
    4. The message immediately before the ping in the batch has media — use that.
       (Safety: only uses messages already fetched; no extra API call.)
    5. Fallback — use the ping message as-is.

    Returns (source_msg, debug_notes: list[str])
    """
    debug = []

    # 1. Ping is inside a thread — the source is always the thread-starter message
    if ping_msg.get("channel_id") != parent_channel_id:
        thread_id = ping_msg["channel_id"]
        thread_starter = fetch_thread_starter(parent_channel_id, thread_id)
        if thread_starter:
            debug.append(f"🧵 Used thread starter (thread {thread_id}) as source.")
            return thread_starter, debug
        else:
            debug.append(f"⚠️ Could not fetch thread starter (thread {thread_id}) — used ping message.")
            return ping_msg, debug

    # 2. Ping message has media?
    if message_has_media(ping_msg):
        debug.append("✅ Media found on ping message.")
        return ping_msg, debug

    # 3. Is the ping a reply?
    ref = ping_msg.get("referenced_message")
    if ref is None and ping_msg.get("message_reference"):
        ref_data = ping_msg["message_reference"]
        ref_channel = ref_data.get("channel_id", parent_channel_id)
        ref_msg_id = ref_data.get("message_id")
        if ref_msg_id:
            ref = fetch_single_message(ref_channel, ref_msg_id)

    if ref and message_has_media(ref):
        debug.append(f"↩️ Media found on replied-to message ({ref['id']}).")
        return ref, debug

    # 4. Check the message immediately before this ping in the already-fetched batch.
    ping_id = int(ping_msg["id"])
    prev_msg = None
    for m in reversed(batch):
        if int(m["id"]) < ping_id:
            prev_msg = m
            break

    if prev_msg is not None and message_has_media(prev_msg):
        debug.append(f"⬆️ Media found on previous message ({prev_msg['id']}).")
        return prev_msg, debug

    # 5. Fallback
    debug.append("⚠️ No media found — used ping message as-is.")
    return ping_msg, debug


# ---------------------------------------------------------------------------
# Embed construction
# ---------------------------------------------------------------------------

def build_embeds(ping_msg: dict, source_msg: dict, debug_notes: list):
    """
    Build a list of Discord embed dicts to send via webhook.

    - Primary embed: source message author, content, image, timestamp.
      The title is the raw jump URL to the source message.
      If the ping message differs from the source, an embed field shows the
      ping message content and a raw link to it.
    - Extra embeds: one per additional image attachment (up to 10 total).
    - Debug embed.

    Discord allows up to 10 embeds per webhook message.
    No top-level `content` is set — the webhook message has zero text content.
    """
    author = source_msg["author"]
    username = author.get("global_name") or author.get("username", "Unknown")
    avatar_hash = author.get("avatar")
    user_id = author["id"]
    avatar_url = (
        f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png"
        if avatar_hash
        else f"https://cdn.discordapp.com/embed/avatars/{int(user_id) % 5}.png"
    )

    content = replace_role_mentions(source_msg.get("content", ""))
    attachments = source_msg.get("attachments", [])

    # Build jump URLs — channel_id is always present in Discord message objects
    src_channel = source_msg["channel_id"]
    src_msg_id = source_msg["id"]
    guild = GUILD_ID or "@me"
    jump_url = f"https://discord.com/channels/{guild}/{src_channel}/{src_msg_id}"

    ping_channel = ping_msg["channel_id"]
    ping_id = ping_msg["id"]
    ping_url = f"https://discord.com/channels/{guild}/{ping_channel}/{ping_id}"

    # Discord snowflake → Unix timestamp for embed footer
    snowflake_ts = (int(src_msg_id) >> 22) + DISCORD_EPOCH
    unix_ts = snowflake_ts // 1000

    # --- Primary embed ---
    primary_embed = {
        "title": f"Submission: {jump_url}",
        "author": {"name": username, "icon_url": avatar_url},
        "color": 0x5865F2,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(unix_ts)),
    }

    if content:
        primary_embed["description"] = content

    image_attachments = [a for a in attachments if a.get("content_type", "").startswith("image/")]
    other_attachments = [a for a in attachments if not a.get("content_type", "").startswith("image/")]

    if image_attachments:
        primary_embed["image"] = {"url": image_attachments[0]["url"]}

    if other_attachments:
        extras = "\n".join(f"📎 [{a.get('filename', 'attachment')}]({a['url']})" for a in other_attachments)
        existing = primary_embed.get("description", "")
        primary_embed["description"] = f"{existing}\n\n{extras}" if existing else extras

    # Ping field — only when ping differs from source
    if ping_url != jump_url:
        ping_content = replace_role_mentions(ping_msg.get("content", ""))
        # noinspection PyTypeChecker
        primary_embed["fields"] = [{
            "name": f"Ping message: {ping_url}",
            "value": ping_content if ping_content else "<no content>",
            "inline": False,
        }]

    if "description" not in primary_embed:
        primary_embed["description"] = "<no content>"

    embeds = [primary_embed]

    # Extra image gallery embeds (images 2–10, shared url = gallery grouping)
    for img in image_attachments[1:9]:
        embeds.append({"url": jump_url, "image": {"url": img["url"]}, "color": 0x5865F2})

    # Debug embed
    embeds.append({
        "title": "🔍 Debug Info",
        "description": "```\n" + "\n".join(debug_notes) + "\n```",
        "color": 0x2C2F33,
    })

    return embeds


# ---------------------------------------------------------------------------
# Webhook delivery
# ---------------------------------------------------------------------------

def forward_to_webhook(ping_msg: dict, parent_channel_id: str, batch: list[dict]):
    source_msg, debug_notes = resolve_source_message(ping_msg, parent_channel_id, batch)
    embeds = build_embeds(ping_msg, source_msg, debug_notes)

    payload = {"embeds": embeds}
    resp = requests.post(f"{WEBHOOK_URL}?wait=true", json=payload)

    if resp.status_code != 200:
        print(f"  [error] Webhook delivery failed: {resp.status_code} {resp.text}")
        time.sleep(WEBHOOK_DELAY_SECONDS)
        return

    posted_id = resp.json().get("id")
    print(f"  [ok] Forwarded message {ping_msg['id']}")

    # React to the forwarded message in the target channel with ✅
    if posted_id and TARGET_CHANNEL_ID:
        add_reaction(TARGET_CHANNEL_ID, posted_id, EMOJI_CHECK)

    time.sleep(WEBHOOK_DELAY_SECONDS)


# ---------------------------------------------------------------------------
# Backlog management
# ---------------------------------------------------------------------------

def process_backlog():
    """
    Read the last 100 messages in the target channel.

    For each message with >=2 ✅ reactions AND no 🏁 reaction yet:
      - React to the forwarded message with 🏁
      - React to each original message linked in the embed with ✅

    Then find the earliest message with <2 ✅ reactions — that's the backlog
    start. If it differs from the last known backlog ID (stored in Sheets B1),
    post a plaintext link to it via the webhook and update B1.
    """
    if not TARGET_CHANNEL_ID:
        print("  [warn] TARGET_CHANNEL_ID not set — skipping backlog processing.")
        return

    messages = fetch_latest_messages(TARGET_CHANNEL_ID, limit=100)
    if not messages:
        print("  [backlog] No messages in target channel.")
        return

    # Only consider submission messages posted by our webhook — exclude backlog
    # link/clear messages (which have content) and any non-webhook messages.
    messages = [
        m for m in messages
        if m.get("webhook_id") == WEBHOOK_ID and not m.get("content")
    ]
    if not messages:
        print("  [backlog] No webhook messages found in target channel.")
        return

    # --- Process fully reviewed messages (>=2 ✅, no 🏁 yet) ---
    for msg in messages:
        checks = reaction_count(msg, EMOJI_CHECK)
        if checks >= 2 and not has_reaction(msg, EMOJI_FLAG):
            msg_id = msg["id"]
            print(f"  [backlog] Message {msg_id} has {checks} ✅ — marking as done.")

            # 🏁 on the forwarded message
            add_reaction(TARGET_CHANNEL_ID, msg_id, EMOJI_FLAG)

            # ✅ on each original message linked in the embed
            for channel_id, original_msg_id in extract_message_links(msg):
                add_reaction(channel_id, original_msg_id, EMOJI_CHECK)

    # --- Find backlog start (earliest webhook message with <2 ✅) ---
    backlog_msg = None
    for msg in messages:
        if reaction_count(msg, EMOJI_CHECK) < 2:
            backlog_msg = msg
            break

    last_backlog_id = get_last_backlog_id()
    guild = GUILD_ID or "@me"

    # Helper: unpin any existing bot-owned pins in the channel
    def _replace_pin(new_message_id: str):
        pins = fetch_pins(TARGET_CHANNEL_ID)
        for pin in pins:
            if pin.get("webhook_id") == WEBHOOK_ID:
                unpin_message(TARGET_CHANNEL_ID, pin["id"])
        pin_message(TARGET_CHANNEL_ID, new_message_id)

    if backlog_msg is None:
        # All caught up — check if we already sent a clear message
        if last_backlog_id == "clear":
            print("  [backlog] Backlog already marked as clear.")
            return
        payload = {"content": "Backlog is clear! 🎉"}
        resp = requests.post(f"{WEBHOOK_URL}?wait=true", json=payload)
        if resp.status_code == 200:
            new_id = resp.json()["id"]
            _replace_pin(new_id)
            update_backlog_id("clear")
            print("  [backlog] Backlog is clear — posted and pinned clear message.")
        else:
            print(f"  [backlog] Failed to post clear message: {resp.status_code} {resp.text}")
        return

    backlog_id = backlog_msg["id"]
    if backlog_id == last_backlog_id:
        print(f"  [backlog] Backlog unchanged at message {backlog_id}.")
        return

    # Backlog has moved — post the link, pin it, update state
    backlog_url = f"https://discord.com/channels/{guild}/{TARGET_CHANNEL_ID}/{backlog_id}"
    payload = {"content": f"Jump to start of backlog: {backlog_url}"}
    resp = requests.post(f"{WEBHOOK_URL}?wait=true", json=payload)
    if resp.status_code == 200:
        new_id = resp.json()["id"]
        _replace_pin(new_id)
        update_backlog_id(backlog_id)
        print(f"  [backlog] Posted and pinned new backlog link → {backlog_url}")
    else:
        print(f"  [backlog] Failed to post backlog link: {resp.status_code} {resp.text}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _process_messages(msgs: list[dict], channel_id: str, highest_id: str, forwarded: int):
    """Process a batch of messages, forwarding any that contain a watched role ping."""
    for msg in msgs:
        mentioned = set(msg.get("mention_roles", []))
        if mentioned & ROLE_IDS:
            print(f"  Processing message {msg['id']} by {msg['author']['username']}...")
            forward_to_webhook(msg, channel_id, msgs)
            forwarded += 1
        if int(msg["id"]) > int(highest_id):
            highest_id = msg["id"]
    return highest_id, forwarded


def main():
    if not CHANNEL_IDS:
        print("[error] ACHIEVEMENTS_CHANNEL_IDS is not set or empty.")
        return
    if not ROLE_IDS:
        print("[error] HARD_CLEARS_ROLE_IDS is not set or empty.")
        return

    last_id = get_last_processed_id()
    highest_id = last_id
    forwarded = 0

    # --- Top-level messages for each configured channel ---
    for channel_id in CHANNEL_IDS:
        print(f"Scanning channel {channel_id}...")
        msgs = fetch_messages(channel_id, last_id)
        if not msgs:
            print(f"  No new messages in {channel_id}.")
        highest_id, forwarded = _process_messages(msgs, channel_id, highest_id, forwarded)

    # --- Active threads across all configured channels ---
    # One API call fetches threads for the whole guild; we filter to our channels.
    # Pings in archived threads are missed — switch to a persistent bot to catch those.
    if GUILD_ID:
        threads = fetch_active_threads(GUILD_ID, set(CHANNEL_IDS))
        print(f"Checking {len(threads)} active thread(s) across {len(CHANNEL_IDS)} channel(s)...")
        for thread in threads:
            thread_id = thread["id"]
            parent_channel_id = thread["parent_id"]
            thread_msgs = fetch_thread_messages_after(thread_id, last_id)
            time.sleep(0.05)  # 50 ms between thread fetches — safe at scale
            highest_id, forwarded = _process_messages(thread_msgs, parent_channel_id, highest_id, forwarded)
    else:
        print("  [warn] GUILD_ID not set — skipping active thread scan.")

    update_last_id(highest_id)
    print(f"Done forwarding. Forwarded {forwarded}. Last ID: {highest_id}")

    # --- Backlog management ---
    process_backlog()
