# notification_service.py
import httpx
from datetime import datetime, timedelta
from typing import List, Dict
from db_utils import get_db, release_db
import kbt_load_env  # Import from db_utils instead of api_server

class MatchNotificationService:
    def __init__(self):
        self.onesignal_app_id = kbt_load_env.onesignal_app_id
        self.onesignal_api_key = kbt_load_env.onesignal_app_key
        self.api_url = "https://api.onesignal.com/notifications"
    
    async def send_match_reminder(self, fixture: Dict):
        """Send reminder that match is starting soon"""
        devices = await self.get_all_devices()
        
        if not devices:
            print("No devices registered for notifications")
            return
        
        match_time = fixture['match_datetime']
        if isinstance(match_time, str):
            match_time = datetime.fromisoformat(match_time)
        
        minutes_until = int((match_time - datetime.now()).total_seconds() / 60)
        home_team = fixture['home_team']
        away_team = fixture['away_team']
        prediction = fixture.get('prediction', '')
        
        notification_data = {
            "app_id": self.onesignal_app_id,
            "include_player_ids": devices,
            "headings": {"en": f"⚽ Match Starting Soon!"},
            "contents": {
                "en": f"🔮 {home_team} vs {away_team} starts in {minutes_until} minutes!\n\nPrediction: {prediction}\n\nDon't miss the action! 📺"
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
            "android_channel_id": "match_reminders",
            "priority": 10
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
            
            await self.log_reminder_sent(fixture['fixture_id'])
            print(f"Reminder sent for fixture {fixture['fixture_id']}")
            return response.json()
    
    async def send_prediction_result(self, fixture: Dict):
        """Send notification about prediction result after match ends"""
        devices = await self.get_all_devices()
        
        if not devices:
            print("No devices registered for notifications")
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
            message = f"✅ {home_team} {home_score}-{away_score} {away_team}\n\nOur prediction was spot on! {prediction}\n\nShare your excitement! 🏆"
        else:
            title = "📊 Match Result"
            message = f"📝 {home_team} {home_score}-{away_score} {away_team}\n\nPrediction: {prediction}\n\nBetter luck next time! 🔮"
        
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
            
            await self.log_result_sent(fixture['fixture_id'])
            print(f"Result notification sent for fixture {fixture['fixture_id']}")
            return response.json()
    
    async def get_all_devices(self) -> List[str]:
        """Get all registered device OneSignal IDs"""
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