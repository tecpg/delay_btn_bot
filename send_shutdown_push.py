"""One-off push to phones still on the old Win 1x2 app (versions before 3.0,
the ones that use this backend): this version is shutting down, update.

Both apps share the FCM "all" topic, so this sends to tokens instead: every
token registered here (users.fcm_token), minus the tokens the new app has
registered with winscore_bot (--exclude-file, one token per line), so people
who already updated don't get it.

    python send_shutdown_push.py --exclude-file new_app_tokens.txt          # count only
    python send_shutdown_push.py --exclude-file new_app_tokens.txt --send   # send

Dead tokens FCM reports are cleared from users, as the normal sends do.
"""

import argparse
import asyncio

import fcm_service
from db_utils import get_db, release_db

TITLE = "⚠️ This version is shutting down"
BODY = "This version of Win 1x2 will stop working soon. Update in the App Store to keep your codes, tips and alerts."


def old_app_tokens() -> list:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DISTINCT fcm_token FROM users WHERE fcm_token IS NOT NULL AND fcm_token <> ''")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        release_db(conn)


def clear_tokens(tokens: list) -> None:
    if not tokens:
        return
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE users SET fcm_token = NULL WHERE fcm_token = ANY(%s)", (list(tokens),))
        conn.commit()
    finally:
        cursor.close()
        release_db(conn)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exclude-file", required=True, help="new-app tokens to skip, one per line")
    parser.add_argument("--send", action="store_true", help="actually send (default: count only)")
    args = parser.parse_args()

    with open(args.exclude_file) as f:
        updated = {line.strip() for line in f if line.strip()}
    registered = old_app_tokens()
    targets = [t for t in registered if t not in updated]

    print(f"Registered with the old backend: {len(registered)}")
    print(f"Already on the new app (skipped): {len(registered) - len(targets)}")
    print(f"Will get the push:               {len(targets)}")
    print(f"\n  {TITLE}\n  {BODY}\n")

    if not args.send:
        print("Count only. Add --send to send it.")
        return
    if not fcm_service.is_enabled():
        print("FCM isn't set up in this environment -- nothing sent.")
        return

    success, dead = asyncio.run(fcm_service.send_tokens(targets, TITLE, BODY, {"type": "app_update"}))
    clear_tokens(dead)
    print(f"Sent to {success} of {len(targets)} phones; cleared {len(dead)} dead tokens.")


if __name__ == "__main__":
    main()
