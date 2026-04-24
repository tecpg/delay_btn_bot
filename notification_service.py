# notification_service.py
import httpx
from datetime import datetime, timedelta
from typing import List, Dict
from db_utils import get_db, release_db
import kbt_load_env

class MatchNotificationService:
# notification_service.py

    def __init__(self):
        self.onesignal_app_id = kbt_load_env.onesignal_app_id
        
        # ✅ TEMPORARY: Hardcode for testing
        self.onesignal_api_key =  kbt_load_env.onesignal_app_key
        
        self.api_url = "https://onesignal.com/api/v1/notifications"
        
        print(f"🔑 API Key loaded (first 20 chars): {self.onesignal_api_key[:20]}...")
        print(f"📱 App ID: {self.onesignal_app_id}")
    # notification_service.py
    async def send_match_reminder(self, fixture: Dict):
        """Send reminder that match is starting soon"""
        devices = await self.get_devices_for_fixture(fixture['fixture_id'])
        
        if not devices:
            print("No devices registered for notifications")
            return
        
        # ... rest of your code ...
        
        notification_data = {
            "app_id": self.onesignal_app_id,
            "include_player_ids": devices,
            "headings": {"en": f"⚽ Match Starting Soon!"},
            "contents": {
                "en": f"🔮 {home_team} vs {away_team} starts in {minutes_until} minutes!\n\nPrediction: {prediction}"
            },
            "data": {
                "type": "match_reminder",
                "fixture_id": fixture['fixture_id'],
                "home_team": home_team,
                "away_team": away_team,
                "prediction": prediction,
                "match_time": match_time.isoformat()
            },
            "url": f"yourapp://fixture/{fixture['fixture_id']}",
            # ✅ REMOVE or COMMENT OUT android_channel_id for iOS
            # "android_channel_id": "match_reminders",  # ← Remove this line
            "priority": 10
        }
    async def send_prediction_result(self, fixture: Dict):
        """Send notification ONLY to devices that enabled notifications for this fixture"""
        
        # ✅ Get devices that have specifically enabled this fixture
        devices = await self.get_devices_for_fixture(fixture['fixture_id'])
        
        if not devices:
            print(f"❌ No devices with notifications enabled for fixture {fixture['fixture_id']}")
            return
        
        home_team = fixture['home_team']
        away_team = fixture['away_team']
        home_score = fixture.get('home_score', 0)
        away_score = fixture.get('away_score', 0)
        prediction = fixture.get('prediction', '')
        
        # Determine if prediction was correct
        is_correct = self.is_prediction_correct(prediction, home_score, away_score)
        
        if is_correct:
            title = "🎯 Prediction Correct!"
            message = f"✅ {home_team} {home_score}-{away_score} {away_team}\n\nOur prediction was spot on! {prediction}"
        else:
            title = "📊 Match Result"
            message = f"📝 {home_team} {home_score}-{away_score} {away_team}\n\nPrediction: {prediction}"
        
        notification_data = {
            "app_id": self.onesignal_app_id,
            "include_player_ids": devices,
            "headings": {"en": title},
            "contents": {"en": message},
            "data": {
                "type": "prediction_result",
                "fixture_id": fixture['fixture_id'],
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "prediction": prediction,
                "was_correct": is_correct
            },
            "url": f"yourapp://fixture/{fixture['fixture_id']}",
            "android_channel_id": "match_results"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                headers={
                    "Authorization": f"Key {self.onesignal_api_key}",
                    "Content-Type": "application/json"
                },
                json=notification_data
            )
            
            print(f"📬 OneSignal Response: {response.status_code} - {response.text}")
            
            await self.log_result_sent(fixture['fixture_id'])
            print(f"Result notification sent for fixture {fixture['fixture_id']}")
            return response.json()
    
    # ✅ NEW: Get devices that have enabled notifications for a specific fixture
    async def get_devices_for_fixture(self, fixture_id: int) -> List[str]:
        """Get devices that have notifications enabled for this specific fixture"""
        conn = get_db()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT d.onesignal_player_id 
                FROM devices d
                INNER JOIN device_fixture_notifications dfn 
                    ON d.device_id = dfn.device_id 
                WHERE dfn.fixture_id = %s 
                AND dfn.enabled = TRUE 
                AND d.is_active = TRUE
            """, (fixture_id,))
            
            devices = [row[0] for row in cursor.fetchall()]
            print(f"🔍 Found {len(devices)} devices with notifications enabled for fixture {fixture_id}")
            return devices
        finally:
            cursor.close()
            release_db(conn)
    
    # ✅ Keep this for other notifications that need to go to all devices
    async def get_all_devices(self) -> List[str]:
        """Get all registered device OneSignal IDs (for non-fixture specific notifications)"""
        conn = get_db()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT onesignal_player_id 
                FROM devices 
                WHERE is_active = TRUE
            """)
            
            devices = [row[0] for row in cursor.fetchall()]
            return devices
        finally:
            cursor.close()
            release_db(conn)
    
    async def register_device(self, onesignal_player_id: str, device_info: Dict):
        """Register a device"""
        conn = get_db()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO devices (device_id, onesignal_player_id, device_model, app_version, last_active)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (device_id) 
                DO UPDATE SET 
                    last_active = NOW(),
                    device_model = EXCLUDED.device_model,
                    app_version = EXCLUDED.app_version,
                    is_active = TRUE
            """, (
                onesignal_player_id,
                onesignal_player_id,
                device_info.get('device_model', 'Unknown'),
                device_info.get('app_version', '1.0.0')
            ))
            
            conn.commit()
            print(f"Device registered: {onesignal_player_id}")
        finally:
            cursor.close()
            release_db(conn)
    
    async def log_reminder_sent(self, fixture_id: int):
        """Log that reminder was sent for this fixture"""
        conn = get_db()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, reminder_sent, reminder_sent_at)
                VALUES (%s, TRUE, NOW())
                ON CONFLICT (fixture_id)
                DO UPDATE SET reminder_sent = TRUE, reminder_sent_at = NOW()
            """, (fixture_id,))
            
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
    
    async def log_result_sent(self, fixture_id: int):
        """Log that result notification was sent for this fixture"""
        conn = get_db()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO notification_log (fixture_id, result_notification_sent, result_notification_sent_at)
                VALUES (%s, TRUE, NOW())
                ON CONFLICT (fixture_id)
                DO UPDATE SET result_notification_sent = TRUE, result_notification_sent_at = NOW()
            """, (fixture_id,))
            
            conn.commit()
        finally:
            cursor.close()
            release_db(conn)
    
    def is_prediction_correct(self, prediction: str, home_score: int, away_score: int) -> bool:
        """Check if prediction matches actual result"""
        prediction_lower = prediction.lower()
        
        if home_score > away_score:
            actual = "home"
        elif home_score == away_score:
            actual = "draw"
        else:
            actual = "away"
        
        if actual == "home" and ("home" in prediction_lower or "win" in prediction_lower):
            return True
        elif actual == "away" and "away" in prediction_lower:
            return True
        elif actual == "draw" and "draw" in prediction_lower:
            return True
        
        return False
    




