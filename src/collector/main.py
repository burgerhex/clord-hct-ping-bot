import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- Configuration (Use GitHub Secrets / .env) ---
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
# Comma-separated list of channel IDs to scan, e.g. "123456,789012"
CHANNEL_IDS = [c.strip() for c in os.getenv("ACHIEVEMENTS_CHANNEL_IDS", "").split(",") if c.strip()]
ROLE_ID = os.getenv("HARD_CLEARS_ROLE_ID")
WEBHOOK_URL = os.getenv("TARGET_WEBHOOK_URL")
SHEET_ID = os.getenv("STATE_SHEET_ID")
GUILD_ID = os.getenv("GUILD_ID")
STATE_WORKSHEET = "Sheet1"

DISCORD_API = "https://discord.com/api/v10"
HEADERS = {"Authorization": f"Bot {DISCORD_TOKEN}"}

ROLE_DISPLAY_NAME = "`@Hard Clears Team`"

WEBHOOK_DELAY_SECONDS = 1
MESSAGE_FETCH_LIMIT = 500


# --- Google Sheets Setup ---
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds_json = os.getenv("GOOGLE_CREDS_JSON")
creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).worksheet(STATE_WORKSHEET)


def _sheets_retry(fn, retries=3, delay=5):
    """
    Call fn(), retrying up to `retries` times on gspread APIError with a 5xx
    status code (transient Google Sheets backend errors).
    """
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


def get_last_processed_id():
    return _sheets_retry(lambda: sheet.acell("A1").value) or "0"


def update_last_id(new_id):
    _sheets_retry(lambda: sheet.update_acell("A1", str(new_id)))


# ---------------------------------------------------------------------------
# Discord API helpers
# ---------------------------------------------------------------------------

def fetch_messages(channel_id, after_id):
    """Fetch up to 100 messages from a channel after after_id."""
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    params = {"after": after_id, "limit": MESSAGE_FETCH_LIMIT}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    msgs = resp.json()
    return sorted(msgs, key=lambda m: int(m["id"]))


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

    Discord threads have a `parent_id` (the channel) and the thread's own ID
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
    """Fetch up to 100 messages in a thread newer than after_id."""
    url = f"{DISCORD_API}/channels/{thread_id}/messages"
    params = {"after": after_id, "limit": MESSAGE_FETCH_LIMIT}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 200:
        msgs = resp.json()
        return sorted(msgs, key=lambda m: int(m["id"]))
    print(f"  [warn] Could not fetch messages for thread {thread_id}: {resp.status_code}")
    return []


# ---------------------------------------------------------------------------
# Content extraction helpers
# ---------------------------------------------------------------------------

def extract_url(content: str):
    """Return the first URL found in a string, or None."""
    for word in content.split():
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


# ---------------------------------------------------------------------------
# Source-message resolution
# ---------------------------------------------------------------------------

def resolve_source_message(ping_msg: dict, parent_channel_id: str):
    """
    Determine the 'display' (source) message for the embed.

    Priority:
    1. If the ping came from inside a thread (channel_id != parent_channel_id),
       the source is always the thread's parent channel message.
    2. Ping message has a link or attachment — use it directly.
    3. Ping is a reply — if the referenced message has media, use that.
    4. Fallback — use the ping message as-is.

    Returns (source_msg, debug_notes: list[str])
    """
    debug = []

    # 1. Ping is inside a thread — source is always the thread-starter message
    if ping_msg.get("channel_id") != parent_channel_id:
        thread_id = ping_msg["channel_id"]
        debug.append(f"🧵 Ping is inside thread {thread_id} — fetching thread starter as source...")
        thread_starter = fetch_thread_starter(parent_channel_id, thread_id)
        if thread_starter:
            debug.append("✅ Using thread starter as source.")
            return thread_starter, debug
        else:
            debug.append("⚠️ Could not fetch thread starter — falling back to ping message.")
            return ping_msg, debug

    # 2. Ping message has media?
    if message_has_media(ping_msg):
        debug.append("✅ Link/attachment found on ping message itself.")
        return ping_msg, debug

    debug.append("ℹ️ Ping message has no link or attachments — looking for context.")

    # 3. Is the ping a reply?
    ref = ping_msg.get("referenced_message")
    if ref is None and ping_msg.get("message_reference"):
        ref_data = ping_msg["message_reference"]
        ref_channel = ref_data.get("channel_id", parent_channel_id)
        ref_msg_id = ref_data.get("message_id")
        if ref_msg_id:
            debug.append(f"↩️ Ping is a reply — fetching referenced message (id={ref_msg_id})...")
            ref = fetch_single_message(ref_channel, ref_msg_id)

    if ref and message_has_media(ref):
        debug.append("✅ Referenced (replied-to) message has link/attachment — using it as source.")
        return ref, debug

    if ref:
        debug.append("⚠️ Referenced message also has no link/attachment.")

    # 4. Fallback
    debug.append("⚠️ No media context found — falling back to original ping message.")
    return ping_msg, debug


# ---------------------------------------------------------------------------
# Embed construction
# ---------------------------------------------------------------------------

def build_embeds(ping_msg: dict, source_msg: dict, debug_notes: list):
    """
    Build a list of Discord embed dicts to send via webhook.

    - Primary embed: source message author, content, image, timestamp.
      Title is the raw jump URL to the source message.
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

    # Replace the role mention (renders as @unknown-role in foreign servers)
    content = source_msg.get("content", "")
    content = content.replace(f"<@&{ROLE_ID}>", ROLE_DISPLAY_NAME)

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
    DISCORD_EPOCH = 1420070400000
    snowflake_ts = (int(src_msg_id) >> 22) + DISCORD_EPOCH
    unix_ts = snowflake_ts // 1000

    # --- Primary embed ---
    # Title is a raw discord.com/channels/... URL — Discord renders it as a
    # clickable message-link chip inside embed titles.
    primary_embed = {
        "title": f"Submission: {jump_url}",
        "author": {
            "name": username,
            "icon_url": avatar_url,
        },
        "color": 0x5865F2,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(unix_ts)),
    }

    if content:
        primary_embed["description"] = content

    # First image attachment goes into the primary embed
    image_attachments = [a for a in attachments if a.get("content_type", "").startswith("image/")]
    other_attachments = [a for a in attachments if not a.get("content_type", "").startswith("image/")]

    if image_attachments:
        primary_embed["image"] = {"url": image_attachments[0]["url"]}

    # Non-image attachments: append as linked filenames in description
    if other_attachments:
        extras = "\n".join(f"📎 [{a.get('filename', 'attachment')}]({a['url']})" for a in other_attachments)
        existing = primary_embed.get("description", "")
        primary_embed["description"] = f"{existing}\n\n{extras}" if existing else extras

    # If the ping message differs from the source (reply/thread case), add a
    # field showing the ping message content and a raw link to it.
    fields = []
    if ping_url != jump_url:
        ping_content = ping_msg.get("content", "")
        ping_content = ping_content.replace(f"<@&{ROLE_ID}>", ROLE_DISPLAY_NAME)
        field_value = ping_content if ping_content else "<no content>"
        fields.append({
            "name": f"Ping message: {ping_url}",
            "value": field_value,
            "inline": False,
        })

    if fields:
        primary_embed["fields"] = fields

    # If primary embed has no description, make that explicit
    if "description" not in primary_embed:
        primary_embed["description"] = "<no content>"

    embeds = [primary_embed]

    # Extra image embeds (images 2–10)
    # Sharing the same `url` groups them into a gallery in Discord.
    for img in image_attachments[1:9]:
        embeds.append({
            "url": jump_url,
            "image": {"url": img["url"]},
            "color": 0x5865F2,
        })

    # --- Debug embed ---
    debug_text = "\n".join(debug_notes)
    embeds.append({
        "title": "🔍 Debug Info",
        "description": f"```\n{debug_text}\n```",
        "color": 0x2C2F33,
    })

    return embeds


# ---------------------------------------------------------------------------
# Webhook delivery
# ---------------------------------------------------------------------------

def forward_to_webhook(ping_msg: dict, parent_channel_id: str):
    source_msg, debug_notes = resolve_source_message(ping_msg, parent_channel_id)
    embeds = build_embeds(ping_msg, source_msg, debug_notes)

    # No top-level `content` — the webhook message is embeds only.
    payload = {"embeds": embeds}
    resp = requests.post(WEBHOOK_URL, json=payload)
    if resp.status_code not in (200, 204):
        print(f"  [error] Webhook delivery failed: {resp.status_code} {resp.text}")
    else:
        print(f"  [ok] Forwarded message {ping_msg['id']}")

    time.sleep(WEBHOOK_DELAY_SECONDS)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not CHANNEL_IDS:
        print("[error] ACHIEVEMENTS_CHANNEL_IDS is not set or empty.")
        return

    last_id = get_last_processed_id()
    highest_id = last_id
    forwarded = 0

    # --- Top-level messages for each configured channel ---
    for channel_id in CHANNEL_IDS:
        print(f"Scanning channel {channel_id}...")
        new_messages = fetch_messages(channel_id, last_id)
        if not new_messages:
            print(f"  No new messages in {channel_id}.")
        for msg in new_messages:
            if ROLE_ID in msg.get("mention_roles", []):
                print(f"  Processing message {msg['id']} by {msg['author']['username']}...")
                forward_to_webhook(msg, channel_id)
                forwarded += 1
            if int(msg["id"]) > int(highest_id):
                highest_id = msg["id"]

    # --- Active threads across all configured channels ---
    # One API call fetches threads for the whole guild; we filter to our channels.
    # Pings in archived threads are missed — switch to a persistent bot to catch those.
    # 50ms sleep between thread message fetches as a rate-limit safety rail.
    if GUILD_ID:
        threads = fetch_active_threads(GUILD_ID, set(CHANNEL_IDS))
        print(f"Checking {len(threads)} active thread(s) across {len(CHANNEL_IDS)} channel(s)...")
        for thread in threads:
            thread_id = thread["id"]
            parent_channel_id = thread["parent_id"]
            thread_msgs = fetch_thread_messages_after(thread_id, last_id)
            time.sleep(0.05)  # 50ms between thread fetches — safe at scale
            for msg in thread_msgs:
                if ROLE_ID in msg.get("mention_roles", []):
                    print(f"  [thread {thread_id}] Processing message {msg['id']} "
                          f"by {msg['author']['username']}...")
                    forward_to_webhook(msg, parent_channel_id)
                    forwarded += 1
                if int(msg["id"]) > int(highest_id):
                    highest_id = msg["id"]
    else:
        print("  [warn] GUILD_ID not set — skipping active thread scan.")

    update_last_id(highest_id)
    print(f"Done. Forwarded {forwarded}. Last ID: {highest_id}")