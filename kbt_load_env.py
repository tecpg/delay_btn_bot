import os
import dotenv as _dotenv

_dotenv.load_dotenv()

db_host = os.environ['db_host']
db_dbname = os.environ['db_name']
db_user = os.environ['db_user']
db_pwd = os.environ['db_password']

api_key =os.environ['api_key']
api_key_secret =os.environ['api_key_secret']

access_token =os.environ['access_token']
access_token_secret = os.environ['access_token_secret']

client_id = os.environ['client_id']
client_id_secret = os.environ['client_id_secret']

live_pwd =  os.environ['live_pwd']
local_pwd =  os.environ['local_pwd']

redis_url = os.environ['REDISCLOUD_URL']
