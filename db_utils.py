# db_utils.py - New file for database utilities
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
import kbt_load_env

# Database pool
db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=kbt_load_env.supabase_url
)

def get_db():
    return db_pool.getconn()

def release_db(conn):
    db_pool.putconn(conn)

def get_db_cursor(conn=None, cursor_factory=None):
    """Get cursor from existing connection or create new one"""
    if conn is None:
        conn = get_db()
    return conn.cursor(cursor_factory=cursor_factory if cursor_factory else RealDictCursor)