# notification_service.py
import httpx
from datetime import datetime
from typing import List, Dict
from db_utils import get_db, release_db
import kbt_load_env
import requests  # used by send_betcode_notification


class MatchNotificationService:
    def __init__(self):
        self.onesignal_app_id = kbt_load_env.onesignal_app_id
        self.onesignal_api_key = kbt_load_env.onesignal_api_key
        self.api_url = "https://api.onesignal.com/notifications"

        print(f"📱 App ID: {self.onesignal_app_id}")
        print(f"🔑 API Key exists: {bool(self.onesignal_api_key)}")

    # ========================= BETCODE BROADCAST =========================
    def send_betcode_notification(self):
        url = "https://onesignal.com/api/v1/notifications"

        payload = {
            "app_id": self.onesignal_app_id,
            "included_segments": ["All"],
            "headings": {"en": "🔥 New Betcodes Available"},
            "contents": {"en": "Fresh booking codes just dropped. Tap to view now!"},
            "data": {
                "type": "betcodes"
            }
        }

        headers = {
            "Authorization": f"Basic {self.onesignal_api_key}",
            "Content-Type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)

        print("📢 Notification sent:", response.status_code, response.text)

    # ========================= SEND REMINDER =========================
    async def send_match_reminder(self, fixture: Dict):
        try:
            fixture_id = fixture['fixture_id']

            users = await self.get_users_for_fixture(fixture_id)

            if not users:
                print(f"❌ No users for fixture {fixture_id}")
                return

            # 🔒 Idempotency guard: atomically claim this reminder BEFORE sending.
            # If it was already sent (e.g. the scheduler ran again a minute later,
            # or two workers fired at once) the claim fails and we skip — this is
            # what stops the same notification going out 20 times.
            if not await self._claim_reminder(fixture_id):
                print(f"⏭️  Reminder already sent for fixture {fixture_id}, skipping")
                return

            match_time = fixture['match_datetime']
            if isinstance(match_time, str):
                match_time = datetime.fromisoformat(match_time)

            # Use a clock that matches the fixture's tz-awareness so we don't
            # crash subtracting an aware datetime from a naive one.
            now = datetime.now(match_time.tzinfo) if match_time.tzinfo else datetime.now()
            minutes_until = int((match_time - now).total_seconds() / 60)

            payload = {
                "app_id": self.onesignal_app_id,
                "include_external_user_ids": users,
                "target_channel": "push",
                "headings": {"en": "⚽ Match Starting Soon!"},
                "contents": {
                    "en": f"{fixture['home_team']} vs {fixture['away_team']} starts in {minutes_until} mins"
                },
                "data": {
                    "type": "match_reminder",
                    "fixture_id": str(fixture_id)  # 🔥 ONLY fixture_id
                }
            }

            sent = await self._send(payload)

            # If the send failed, release the claim so a later run can retry
            # instead of the notification being silently lost forever.
            if not sent:
                await self._release_reminder(fixture_id)

        except Exception as e:
            print(f"❌ Reminder error: {e}")

    # ========================= SEND RESULT =========================
    async def send_prediction_result(self, fixture: Dict):
        try:
            fixture_id = fixture['fixture_id']

            users = await self.get_users_for_fixture(fixture_id)

            if not users:
                return

            # 🔒 Same idempotency guard for result notifications.
            if not await self._claim_result(fixture_id):
                print(f"⏭️  Result already sent for fixture {fixture_id}, skipping")
                return

            home = fixture['home_team']
            away = fixture['away_team']
            hs = fixture.get('home_score', 0)
            aw = fixture.get('away_score', 0)
            pred = fixture.get('prediction', '')

            correct = self.is_prediction_correct(pred, hs, aw)

            title = "🎯 Prediction Correct!" if correct else "📊 Match Result"
            message = f"{home} {hs}-{aw} {away}\nPrediction: {pred}"

            payload = {
                "app_id": self.onesignal_app_id,
                "include_external_user_ids": users,
                "target_channel": "push",
                "headings": {"en": title},
                "contents": {"en": message},
                "data": {
                    "fixture_id": fixture_id
                }
            }

            sent = await self._send(payload)

            if not sent:
                await self._release_result(fixture_id)

        except Exception as e:
            print(f"❌ Result error: {e}")

    # ========================= SEND CORE =========================
    async def _send(self, payload: Dict) -> bool:
        """POST to OneSignal. Returns True only on a 2xx response."""
        try:
            async with httpx.AsyncClient() as client:
                res = await client.post(
                    self.api_url,
                    headers={
                        "Authorization": f"Key {self.onesignal_api_key}",
                        "Content-Type": "application/json"
                    },
                    json=payload
                )

            print("📬", res.status_code, res.text)

            if res.status_code not in (200, 201):
                print("❌ Notification failed")
                return False
            return True
        except Exception as e:
            print(f"❌ Notification request error: {e}")
            return False

    # ========================= USERS =========================
    async def get_users_for_fixture(self, fixture_id: int) -> List[str]:
        conn = get_db()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT DISTINCT user_id
                FROM device_fixture_notifications
                WHERE fixture_id = %s
                AND enabled = TRUE
            """, (fixture_id,))

            # DISTINCT in SQL + dedupe here guards against a single user with
            # multiple device rows being pushed the same notification N times.
            users = list(dict.fromkeys(row[0] for row in cursor.fetchall()))
            print(f"👤 Found {len(users)} users for fixture {fixture_id}")
            return users

        finally:
            cursor.close()
            release_db(conn)

    # ========================= REGISTER =========================
    async def register_user(self, user_id: str, device_info: Dict):
        conn = get_db()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO users (user_id, device_model, app_version, last_active)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET last_active = NOW()
            """, (
                user_id,
                device_info.get('device_model'),
                device_info.get('app_version')
            ))

            conn.commit()
            print(f"✅ User registered: {user_id}")

        finally:
            cursor.close()
            release_db(conn)

    # ========================= IDEMPOTENT CLAIM / RELEASE =========================
    async def _claim_reminder(self, fixture_id: int) -> bool:
        """
        Atomically mark a reminder as sent. Returns True only if THIS call won
        the claim (i.e. it had not already been sent).

        The `ON CONFLICT ... DO UPDATE ... WHERE` only updates when the flag is
        not already TRUE, and RETURNING yields a row only when an insert or
        update actually happened. Postgres locks the conflicting row for the
        duration, so two concurrent callers can't both win.
        """
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, reminder_sent)
                VALUES (%s, TRUE)
                ON CONFLICT (fixture_id)
                DO UPDATE SET reminder_sent = TRUE
                WHERE notification_log.reminder_sent IS DISTINCT FROM TRUE
                RETURNING fixture_id
            """, (fixture_id,))
            claimed = cursor.fetchone() is not None
            conn.commit()
            return claimed
        finally:
            cursor.close()
            release_db(conn)

    async def _release_reminder(self, fixture_id: int):
        """Undo a reminder claim after a failed send so it can be retried."""
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE notification_log
                SET reminder_sent = FALSE
                WHERE fixture_id = %s
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)

    async def _claim_result(self, fixture_id: int) -> bool:
        """Atomically mark a result notification as sent. See _claim_reminder."""
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, result_notification_sent)
                VALUES (%s, TRUE)
                ON CONFLICT (fixture_id)
                DO UPDATE SET result_notification_sent = TRUE
                WHERE notification_log.result_notification_sent IS DISTINCT FROM TRUE
                RETURNING fixture_id
            """, (fixture_id,))
            claimed = cursor.fetchone() is not None
            conn.commit()
            return claimed
        finally:
            cursor.close()
            release_db(conn)

    async def _release_result(self, fixture_id: int):
        """Undo a result claim after a failed send so it can be retried."""
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE notification_log
                SET result_notification_sent = FALSE
                WHERE fixture_id = %s
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)

    # ========================= LOGGING (kept for backward compatibility) =========================
    async def log_reminder_sent(self, fixture_id: int):
        conn = get_db()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, reminder_sent)
                VALUES (%s, TRUE)
                ON CONFLICT (fixture_id)
                DO UPDATE SET reminder_sent = TRUE
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)

    async def log_result_sent(self, fixture_id: int):
        conn = get_db()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, result_notification_sent)
                VALUES (%s, TRUE)
                ON CONFLICT (fixture_id)
                DO UPDATE SET result_notification_sent = TRUE
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)

    # ========================= LOGIC =========================
    def is_prediction_correct(self, prediction, hs, aw):
        prediction = (prediction or "").lower()
        if hs > aw:
            return "home" in prediction
        elif hs < aw:
            return "away" in prediction
        return "draw" in prediction