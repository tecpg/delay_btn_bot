# fcm_service.py
# Firebase Cloud Messaging (HTTP v1) sender.
# Enabled only when FIREBASE_SERVICE_ACCOUNT_JSON is set.
import asyncio
import json
import threading
import time
from typing import Dict, List, Optional, Tuple

import httpx
import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account

import kbt_load_env

SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]

# Topic every app install subscribes to (replaces OneSignal's "All" segment)
ALL_TOPIC = "all"

MAX_CONCURRENT_SENDS = 50

_credentials = None
_project_id = None
_lock = threading.Lock()

if kbt_load_env.firebase_service_account:
    try:
        _info = json.loads(kbt_load_env.firebase_service_account)
        _credentials = service_account.Credentials.from_service_account_info(
            _info, scopes=SCOPES
        )
        _project_id = _info["project_id"]
        print(f"🔥 FCM enabled for project: {_project_id}")
    except Exception as e:
        print(f"❌ Invalid FIREBASE_SERVICE_ACCOUNT_JSON, FCM disabled: {e}")
        _credentials = None


def is_enabled() -> bool:
    return _credentials is not None


def _url() -> str:
    return f"https://fcm.googleapis.com/v1/projects/{_project_id}/messages:send"


def _headers() -> Dict:
    with _lock:
        if not _credentials.valid:
            _credentials.refresh(GoogleAuthRequest())
        token = _credentials.token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _message(
    target: Dict,
    title: str,
    body: str,
    data: Optional[Dict] = None,
    subtitle: Optional[str] = None,
    ttl: Optional[int] = None,
) -> Dict:
    alert = {"title": title, "body": body}
    if subtitle:
        alert["subtitle"] = subtitle

    apns = {"payload": {"aps": {"alert": alert, "sound": "default"}}}
    android = {"priority": "HIGH"}
    if ttl:
        apns["headers"] = {"apns-expiration": str(int(time.time()) + ttl)}
        android["ttl"] = f"{ttl}s"

    return {
        "message": {
            **target,
            "notification": {"title": title, "body": body},
            # FCM data values must be strings
            "data": {k: str(v) for k, v in (data or {}).items()},
            "apns": apns,
            "android": android,
        }
    }


def _is_unregistered(res) -> bool:
    """True when FCM says the token is dead and should be removed."""
    if res.status_code == 404:
        return True
    try:
        details = res.json().get("error", {}).get("details", [])
        return any(d.get("errorCode") == "UNREGISTERED" for d in details)
    except Exception:
        return False


# ========================= TOPIC (broadcast) =========================
def send_topic_sync(title: str, body: str, data: Optional[Dict] = None,
                    topic: str = ALL_TOPIC, **kwargs) -> bool:
    try:
        res = requests.post(
            _url(),
            headers=_headers(),
            json=_message({"topic": topic}, title, body, data, **kwargs),
            timeout=15,
        )
        print("🔥 FCM topic:", res.status_code, res.text)
        return res.status_code == 200
    except Exception as e:
        print(f"❌ FCM topic error: {e}")
        return False


async def send_topic(title: str, body: str, data: Optional[Dict] = None,
                     topic: str = ALL_TOPIC, **kwargs) -> bool:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.post(
                _url(),
                headers=_headers(),
                json=_message({"topic": topic}, title, body, data, **kwargs),
            )
        print("🔥 FCM topic:", res.status_code, res.text)
        return res.status_code == 200
    except Exception as e:
        print(f"❌ FCM topic error: {e}")
        return False


# ========================= TOKENS (per-user) =========================
async def send_tokens(tokens: List[str], title: str, body: str,
                      data: Optional[Dict] = None, **kwargs) -> Tuple[int, List[str]]:
    """Send to each token. Returns (success_count, dead_tokens)."""
    headers = _headers()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)
    invalid: List[str] = []
    success = 0

    async def send_one(client, token):
        nonlocal success
        async with semaphore:
            try:
                res = await client.post(
                    _url(),
                    headers=headers,
                    json=_message({"token": token}, title, body, data, **kwargs),
                )
            except Exception as e:
                print(f"❌ FCM token send error: {e}")
                return

        if res.status_code == 200:
            success += 1
        elif _is_unregistered(res):
            invalid.append(token)
        else:
            print("❌ FCM token send failed:", res.status_code, res.text)

    async with httpx.AsyncClient(timeout=15) as client:
        await asyncio.gather(*(send_one(client, t) for t in tokens))

    print(f"🔥 FCM tokens: {success}/{len(tokens)} sent, {len(invalid)} dead")
    return success, invalid
