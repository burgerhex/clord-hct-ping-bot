import os
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- Configuration (Use GitHub Secrets) ---
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = os.getenv("ACHIEVEMENTS_CHANNEL_ID")
ROLE_ID = os.getenv("HARD_CLEARS_ROLE_ID")
WEBHOOK_URL = os.getenv("TARGET_WEBHOOK_URL")
SHEET_ID = os.getenv("STATE_SHEET_ID")
STATE_WORKSHEET = "Sheet1"  # Where the last_id is stored

# --- Google Sheets Setup ---
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(os.getenv("GOOGLE_CREDS_JSON"), scopes=scopes)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).worksheet(STATE_WORKSHEET)


def get_last_processed_id():
    return sheet.acell("A1").value or "0"


def update_last_id(new_id):
    sheet.update_acell("A1", str(new_id))


def fetch_messages(after_id):
    url = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages"
    headers = {"Authorization": f"Bot {DISCORD_TOKEN}"}
    params = {"after": after_id, "limit": 100}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    # Discord returns messages Oldest -> Newest when using 'after'
    return resp.json()


def forward_to_webhook(msg):
    author = msg['author']['username']
    content = msg['content']
    msg_link = f"https://discord.com/channels/@me/{CHANNEL_ID}/{msg['id']}"

    payload = {
        "content": f"**New Hard Clear Ping from {author}**\n{content}\n[Jump to Message]({msg_link})"
    }
    requests.post(WEBHOOK_URL, json=payload)


def main():
    last_id = get_last_processed_id()
    new_messages = fetch_messages(last_id)

    if not new_messages:
        print("No new messages.")
        return

    highest_id = last_id

    for msg in new_messages:
        # Check if the role was actually mentioned
        if ROLE_ID in msg.get("mention_roles", []):
            forward_to_webhook(msg)

        # Track the newest ID encountered
        if int(msg['id']) > int(highest_id):
            highest_id = msg['id']

    # Persist the state for the next run
    update_last_id(highest_id)
    print(f"Processed {len(new_messages)} messages. Last ID: {highest_id}")
