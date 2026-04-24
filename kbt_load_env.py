import os
import dotenv as _dotenv

_dotenv.load_dotenv()

db_host = os.environ['db_host']
db_dbname = os.environ['db_name']
db_user = os.environ['db_user']
db_pwd = os.environ['db_password']

api_key =os.environ['api_key']
api_key_secret =os.environ['api_key_secret']
api_football_key = os.environ['api_football_api_key']

access_token =os.environ['access_token']
access_token_secret = os.environ['access_token_secret']


# Read environment variables
onesignal_app_id = os.getenv("ONESIGNAL_APP_ID")
onesignal_app_key = os.getenv("ONESIGNAL_API_KEY")

# Debug: Check if loaded (don't print full key in production)
print(f"🔑 oneSignal_app_id loaded: {bool(onesignal_app_id)}")
print(f"🔑 oneSignal_app_key loaded: {bool(onesignal_app_key)}")
if onesignal_app_key:
    print(f"🔑 Key starts with: {onesignal_app_key[:15]}...")


client_id = os.environ['client_id']
client_id_secret = os.environ['client_id_secret']

live_pwd =  os.environ['live_pwd']
local_pwd =  os.environ['local_pwd']

redis_url = os.environ['REDISCLOUD_URL']
supabase_url = os.environ['SUPABASE_URL']
