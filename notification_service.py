# notification_service.py
import httpx
from datetime import datetime
from typing import List, Dict
from db_utils import get_db, release_db
import kbt_load_env
import requests  # 🔥 FIX: add import


class MatchNotificationService:
    def __init__(self):
        self.onesignal_app_id = kbt_load_env.onesignal_app_id
        self.onesignal_api_key = kbt_load_env.onesignal_api_key
        self.api_url = "https://api.onesignal.com/notifications"

        print(f"📱 App ID: {self.onesignal_app_id}")
        print(f"🔑 API Key exists: {bool(self.onesignal_api_key)}")

        # ========================= SEND REMINDER =========================


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
            "Authorization": f"Basic {self.onesignal_api_key}",  # 🔥 FIXED
            "Content-Type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)

        print("📢 Notification sent:", response.status_code, response.text)


    async def send_match_reminder(self, fixture: Dict):
        try:
            users = await self.get_users_for_fixture(fixture['fixture_id'])

            if not users:
                print(f"❌ No users for fixture {fixture['fixture_id']}")
                return

            match_time = fixture['match_datetime']
            if isinstance(match_time, str):
                match_time = datetime.fromisoformat(match_time)

            minutes_until = int((match_time - datetime.now()).total_seconds() / 60)

            payload = {
                "app_id": self.onesignal_app_id,
                "include_external_user_ids": users,  # ✅ FIXED
                "target_channel": "push",
                "headings": {"en": "⚽ Match Starting Soon!"},
                "contents": {
                    "en": f"{fixture['home_team']} vs {fixture['away_team']} starts in {minutes_until} mins"
                },
                "data": {
                    "type": "match_reminder",
                    "fixture_id": fixture['fixture_id']
                }
            }

            await self._send(payload)

            await self.log_reminder_sent(fixture['fixture_id'])

        except Exception as e:
            print(f"❌ Reminder error: {e}")

    # ========================= SEND RESULT =========================
    async def send_prediction_result(self, fixture: Dict):
        users = await self.get_users_for_fixture(fixture['fixture_id'])

        if not users:
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
            "include_external_user_ids": users,  # ✅ FIXED
            "target_channel": "push",
            "headings": {"en": title},
            "contents": {"en": message},
            "data": {
                "fixture_id": fixture['fixture_id']
            }
        }

        await self._send(payload)
        await self.log_result_sent(fixture['fixture_id'])

    # ========================= SEND CORE =========================
    async def _send(self, payload: Dict):
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

            if res.status_code not in [200, 201]:
                print("❌ Notification failed")

    # ========================= USERS =========================
    async def get_users_for_fixture(self, fixture_id: int) -> List[str]:
        conn = get_db()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT user_id
                FROM device_fixture_notifications
                WHERE fixture_id = %s
                AND enabled = TRUE
            """, (fixture_id,))

            users = [row[0] for row in cursor.fetchall()]
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

    # ========================= LOGGING =========================
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
        if hs > aw:
            return "home" in prediction.lower()
        elif hs < aw:
            return "away" in prediction.lower()
        return "draw" in prediction.lower()