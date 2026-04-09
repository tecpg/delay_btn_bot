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

onesignal_app_id =  os.environ['onesignal_app_id']
onesignal_app_key =  os.environ['os_v2_app_bag6ymcddvegfhpotuu5nquhledle4bgvaruqkerbj4in37ci7w62ukamcn6b4h63yio5mao4tdke6zw7ouzr33isc7llzq3hcsqk3q']


client_id = os.environ['client_id']
client_id_secret = os.environ['client_id_secret']

live_pwd =  os.environ['live_pwd']
local_pwd =  os.environ['local_pwd']

redis_url = os.environ['REDISCLOUD_URL']
supabase_url = os.environ['SUPABASE_URL']
