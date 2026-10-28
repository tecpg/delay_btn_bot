# notification_service.py
from datetime import datetime
from typing import List, Dict
from db_utils import get_db, release_db
import fcm_service


class MatchNotificationService:

    TOP_LEAGUES = [
        "World Cup",
        "English Premier League",
        "Premier League",
        "La Liga",
        "Bundesliga",
        "Serie A",
        "Ligue 1",
        "UEFA Champions League",
        "UEFA Europa League",
        "FIFA World Cup",
        "UEFA European Championship",
        "Copa del Rey",
        "FA Cup",
        "Carabao Cup",
        "DFB Pokal",
        "Coppa Italia",
        "Coupe de France",
    ]

    def __init__(self):
        # Pushes go through Firebase only. OneSignal was dropped once it
        # paused this app's notifications; app versions without an FCM
        # token (before 2.0.5) no longer get pushes.
        pass

    # ========================= BETCODE BROADCAST =========================
    def send_betcode_notification(self):
        payload = {
            "headings": {"en": "🔥 New Betcodes Available"},
            "contents": {"en": "Fresh booking codes just dropped. Tap to view now!"},
            "data": {"type": "betcodes"}
        }

        self._broadcast_sync(payload)

    # ========================= LOGIC =========================
    def is_prediction_correct(self, prediction, hs, aw):
        prediction = (prediction or "").lower()
        if hs > aw:
            return "home" in prediction
        elif hs < aw:
            return "away" in prediction
        return "draw" in prediction

    def is_top_league(self, league_name: str) -> bool:
        if not league_name:
            return False
        league_lower = league_name.lower()
        return any(t.lower() in league_lower for t in self.TOP_LEAGUES)

    # ========================= SEND CORE =========================
    @staticmethod
    def _fcm_args(payload: Dict) -> Dict:
        """Map a OneSignal payload onto fcm_service arguments."""
        return {
            "title": payload["headings"]["en"],
            "body": payload["contents"]["en"],
            "data": payload.get("data"),
            "subtitle": (payload.get("subtitle") or {}).get("en"),
            "ttl": payload.get("ttl"),
        }

    def _broadcast_sync(self, payload: Dict):
        """Blocking broadcast to the FCM "all" topic."""
        if fcm_service.is_enabled():
            fcm_service.send_topic_sync(**self._fcm_args(payload))

    async def _send(self, payload: Dict) -> bool:
        """
        Deliver through FCM: the "all" topic for a broadcast, the users'
        registered tokens otherwise. False only when FCM is set up and
        nothing was delivered, so the caller can release its claim.
        """
        if not fcm_service.is_enabled():
            return True
        users = payload.get("include_external_user_ids")

        if users is None:
            # Broadcast
            return await fcm_service.send_topic(**self._fcm_args(payload))

        # Per-user
        tokens = self._get_fcm_tokens(users)
        if not tokens:
            return True
        success, dead = await fcm_service.send_tokens(
            list(tokens.values()), **self._fcm_args(payload)
        )
        self._clear_fcm_tokens(dead)
        return success > 0

    # ========================= USERS =========================
    async def get_users_for_fixture(self, fixture_id: int) -> List[str]:
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT user_id
                FROM device_fixture_notifications
                WHERE fixture_id = %s AND enabled = TRUE
            """, (fixture_id,))
            users = list(dict.fromkeys(row[0] for row in cursor.fetchall()))
            print(f"👤 Found {len(users)} users for fixture {fixture_id}")
            return users
        finally:
            cursor.close()
            release_db(conn)

    # ========================= FCM TOKENS =========================
    def _get_fcm_tokens(self, user_ids: List[str]) -> Dict[str, str]:
        """user_id -> fcm_token for the users that have one."""
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT user_id, fcm_token
                FROM users
                WHERE user_id = ANY(%s) AND fcm_token IS NOT NULL
            """, (list(user_ids),))
            return {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            cursor.close()
            release_db(conn)

    def _clear_fcm_tokens(self, tokens: List[str]):
        if not tokens:
            return
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE users SET fcm_token = NULL WHERE fcm_token = ANY(%s)",
                (list(tokens),)
            )
            conn.commit()
            print(f"🧹 Cleared {len(tokens)} dead FCM tokens")
        finally:
            cursor.close()
            release_db(conn)

    # ========================= REGISTER =========================
    async def register_user(self, user_id: str, device_info: Dict):
        fcm_token = device_info.get('fcm_token')

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

            # Only apps on the FCM build send fcm_token ("" = notifications off)
            if fcm_token is not None:
                cursor.execute("""
                    UPDATE users SET fcm_token = NULL
                    WHERE fcm_token = %s AND user_id <> %s
                """, (fcm_token, user_id))
                cursor.execute("""
                    UPDATE users
                    SET fcm_token = NULLIF(%s, ''),
                        platform = %s,
                        fcm_updated_at = NOW()
                    WHERE user_id = %s
                """, (fcm_token, device_info.get('platform'), user_id))

            conn.commit()
            print(f"✅ User registered: {user_id}")
        finally:
            cursor.close()
            release_db(conn)

    # ========================= MATCH REMINDER (per-user) =========================
    async def send_match_reminder(self, fixture: Dict):
        try:
            fixture_id = fixture['fixture_id']

            users = await self.get_users_for_fixture(fixture_id)
            if not users:
                print(f"❌ No users for fixture {fixture_id}")
                return

            if not await self._claim_reminder(fixture_id):
                print(f"⏭️ Reminder already sent for fixture {fixture_id}, skipping")
                return

            match_time = fixture['match_datetime']
            if isinstance(match_time, str):
                match_time = datetime.fromisoformat(match_time)

            now = datetime.now(match_time.tzinfo) if match_time.tzinfo else datetime.now()
            minutes_until = int((match_time - now).total_seconds() / 60)

            payload = {
                "include_external_user_ids": users,
                "target_channel": "push",
                "headings": {"en": "⚽ Match Starting Soon!"},
                "contents": {
                    "en": f"{fixture['home_team']} vs {fixture['away_team']} starts in {minutes_until} mins"
                },
                "data": {
                    "type": "match_reminder",
                    "fixture_id": str(fixture_id)
                }
            }

            sent = await self._send(payload)
            if not sent:
                await self._release_reminder(fixture_id)

        except Exception as e:
            print(f"❌ Reminder error: {e}")

    # ========================= MATCH RESULT (per-user) =========================
    async def send_prediction_result(self, fixture: Dict):
        try:
            fixture_id = fixture['fixture_id']

            users = await self.get_users_for_fixture(fixture_id)
            if not users:
                return

            if not await self._claim_result(fixture_id):
                print(f"⏭️ Result already sent for fixture {fixture_id}, skipping")
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
                "include_external_user_ids": users,
                "target_channel": "push",
                "headings": {"en": title},
                "contents": {"en": message},
                "data": {
                    "type": "prediction_result",
                    "fixture_id": str(fixture_id)
                }
            }

            sent = await self._send(payload)
            if not sent:
                await self._release_result(fixture_id)

        except Exception as e:
            print(f"❌ Result error: {e}")

    # ========================= TOP LEAGUE REMINDER (broadcast) =========================
    async def send_top_league_reminder(self, fixture: Dict):
        try:
            fixture_id = fixture['fixture_id']

            if not await self._claim_top_league_reminder(fixture_id):
                print(f"⏭️ Top-league reminder already sent for {fixture_id}")
                return

            match_time = fixture['match_datetime']
            if isinstance(match_time, str):
                match_time = datetime.fromisoformat(match_time)

            now = datetime.now(match_time.tzinfo) if match_time.tzinfo else datetime.now()
            minutes_until = int((match_time - now).total_seconds() / 60)

            payload = {
                "headings": {"en": "⚽ Top Match Starting Soon!"},
                "contents": {
                    "en": (
                        f"{fixture['home_team']} vs {fixture['away_team']} "
                        f"kicks off in {minutes_until} mins\n"
                        f"🏆 {fixture.get('league', '')}"
                    )
                },
                "data": {
                    "type": "match_reminder",
                    "fixture_id": str(fixture_id)
                }
            }

            sent = await self._send(payload)
            if not sent:
                await self._release_top_league_reminder(fixture_id)

        except Exception as e:
            print(f"❌ Top-league reminder error: {e}")

    # ========================= TOP LEAGUE RESULT (broadcast) =========================
    async def send_top_league_result(self, fixture: Dict):
        try:
            fixture_id = fixture['fixture_id']

            if not await self._claim_top_league_result(fixture_id):
                print(f"⏭️ Top-league result already sent for {fixture_id}")
                return

            home = fixture['home_team']
            away = fixture['away_team']
            hs = fixture.get('home_score', 0)
            aw = fixture.get('away_score', 0)
            pred = fixture.get('prediction', '')
            league = fixture.get('league', '')

            correct = self.is_prediction_correct(pred, hs, aw)

            if correct:
                title = "🎯 Prediction Correct!"
                body = f"{home} {hs}-{aw} {away} ✅\n🏆 {league}"
            else:
                title = f"📊 {league} Result"
                body = f"{home} {hs}-{aw} {away}"

            payload = {
                "headings": {"en": title},
                "contents": {"en": body},
                "data": {
                    "type": "match_reminder",
                    "fixture_id": str(fixture_id),
                    "correct": correct
                }
            }

            sent = await self._send(payload)
            if not sent:
                await self._release_top_league_result(fixture_id)

        except Exception as e:
            print(f"❌ Top-league result error: {e}")

    # ========================= IDEMPOTENCY: PER-USER =========================
    async def _claim_reminder(self, fixture_id: int) -> bool:
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
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE notification_log SET reminder_sent = FALSE WHERE fixture_id = %s
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)

    async def _claim_result(self, fixture_id: int) -> bool:
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
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE notification_log SET result_notification_sent = FALSE WHERE fixture_id = %s
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)

    # ========================= IDEMPOTENCY: TOP LEAGUE =========================
    async def _claim_top_league_reminder(self, fixture_id: int) -> bool:
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, top_league_reminder_sent)
                VALUES (%s, TRUE)
                ON CONFLICT (fixture_id)
                DO UPDATE SET top_league_reminder_sent = TRUE
                WHERE notification_log.top_league_reminder_sent IS DISTINCT FROM TRUE
                RETURNING fixture_id
            """, (fixture_id,))
            claimed = cursor.fetchone() is not None
            conn.commit()
            return claimed
        finally:
            cursor.close()
            release_db(conn)

    async def _release_top_league_reminder(self, fixture_id: int):
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE notification_log SET top_league_reminder_sent = FALSE WHERE fixture_id = %s
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)

    async def _claim_top_league_result(self, fixture_id: int) -> bool:
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, top_league_result_sent)
                VALUES (%s, TRUE)
                ON CONFLICT (fixture_id)
                DO UPDATE SET top_league_result_sent = TRUE
                WHERE notification_log.top_league_result_sent IS DISTINCT FROM TRUE
                RETURNING fixture_id
            """, (fixture_id,))
            claimed = cursor.fetchone() is not None
            conn.commit()
            return claimed
        finally:
            cursor.close()
            release_db(conn)

    async def _release_top_league_result(self, fixture_id: int):
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE notification_log SET top_league_result_sent = FALSE WHERE fixture_id = %s
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)

    # ========================= LEGACY LOGGING =========================
    async def log_reminder_sent(self, fixture_id: int):
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, reminder_sent)
                VALUES (%s, TRUE)
                ON CONFLICT (fixture_id) DO UPDATE SET reminder_sent = TRUE
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
                ON CONFLICT (fixture_id) DO UPDATE SET result_notification_sent = TRUE
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
    

        # ========================= VIP RESULT (broadcast) =========================
    async def send_vip_result(self, fixture: Dict):
        try:
            fixture_id = fixture['fixture_id']

            home = fixture['home_team']
            away = fixture['away_team']
            hs = fixture.get('home_score', 0)
            aw = fixture.get('away_score', 0)
            pred = fixture.get('prediction', '')

            correct = self.is_prediction_correct(pred, hs, aw)

            if not correct:
                print(f"⏭️ VIP prediction incorrect for {fixture_id}, skipping notification")
                return

            if not await self._claim_vip_result(fixture_id):
                print(f"⏭️ VIP result already sent for {fixture_id}")
                return

            payload = {
                "headings": {"en": "💎 VIP Prediction Correct!"},
                "contents": {"en": f"{home} {hs}-{aw} {away} ✅\nPrediction: {pred}"},
                "data": {
                    "type": "vip_result",
                    "fixture_id": str(fixture_id),
                    "correct": True
                }
            }

            sent = await self._send(payload)
            if not sent:
                await self._release_vip_result(fixture_id)

        except Exception as e:
            print(f"❌ VIP result error: {e}")
    async def _claim_vip_result(self, fixture_id: int) -> bool:
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, vip_result_sent)
                VALUES (%s, TRUE)
                ON CONFLICT (fixture_id)
                DO UPDATE SET vip_result_sent = TRUE
                WHERE notification_log.vip_result_sent IS DISTINCT FROM TRUE
                RETURNING fixture_id
            """, (fixture_id,))
            claimed = cursor.fetchone() is not None
            conn.commit()
            return claimed
        finally:
            cursor.close()
            release_db(conn)

    async def _release_vip_result(self, fixture_id: int):
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE notification_log SET vip_result_sent = FALSE WHERE fixture_id = %s
            """, (fixture_id,))
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
    

    # ========================= DAILY PREDICTIONS READY =========================
    def send_predictions_ready(self):
        """Broadcast notification that daily predictions are posted."""
        payload = {
            "headings": {"en": "Let's Win again today! 💰🎉💰🎉🎉"},
            "subtitle": {"en": "🎉🎉💲💲Win 1x2 Predictions for Today"},
            "contents": {"en": "Today's winning prediction are already posted check them out!"},
            "data": {"type": "predictions_ready"},
            "priority": 10,
            "ttl": 259200,    # 3 days in seconds
            "mutable_content": True,
            "ios_relevance_score": 1.0,
            "ios_interruption_level": "active",
        }

        self._broadcast_sync(payload)